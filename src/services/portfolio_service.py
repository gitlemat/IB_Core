from typing import Any, Dict, List
from .base_service import IBBaseService
from models import AccountSummary
from utils import clean_float
from config import Config

class PortfolioService(IBBaseService):
    def __init__(self, connector: 'IBConnector'):
        super().__init__(connector)
        self.last_account_summary = {}
        from portfolio_manager import PortfolioManager
        self.portfolio_manager = PortfolioManager(self.connector.db_client, self.connector.watchlist_manager)

    @property
    def reconciled_positions(self):
        return self.portfolio_manager.reconciled_positions

    @property
    def reconciled_avg_costs(self):
        return self.portfolio_manager.reconciled_avg_costs

    def handle_account_summary_end(self, req_id: int):
        """
        Called when account summary request is complete.
        Constructs AccountSummary objects and writes to DB.
        """
        if not self.connector.wrapper.account_summary:
            return

        self.logger.info(f"Processing Account Summary for ReqId: {req_id}")
        
        for account, tags in self.connector.wrapper.account_summary.items():
            try:
                # Map basic tags to our model
                summary = AccountSummary(
                    account_code=account,
                    net_liquidation=clean_float(tags.get("NetLiquidation", "0")),
                    available_funds=clean_float(tags.get("AvailableFunds", "0")),
                    buying_power=clean_float(tags.get("BuyingPower", "0"))
                )
                
                # Write to DB
                self.connector.db_client.write_account_summary(summary, is_paper=Config.is_paper_trading())
                
            except Exception as e:
                self.logger.error(f"Error processing account summary for {account}: {e}")

        # Broadcast to WebSockets (Delta Logic)
        if self.connector.ws_manager and self.connector.main_loop:
            import asyncio
            import copy

            current_summary = self.connector.wrapper.account_summary
            delta = {}
            has_changed = False
            
            # Calculate Delta
            for account, data in current_summary.items():
                if account not in self.last_account_summary:
                    delta[account] = data
                    has_changed = True
                else:
                    acc_delta = {}
                    last_data = self.last_account_summary[account]
                    for k, v in data.items():
                        if last_data.get(k) != v:
                            acc_delta[k] = v
                            has_changed = True
                    if acc_delta:
                        delta[account] = acc_delta
            
            if has_changed:
                self.last_account_summary = copy.deepcopy(current_summary)
                try:
                    self.logger.info("Sending WS update to clients for topic 'account'")
                    asyncio.run_coroutine_threadsafe(
                        self.connector.ws_manager.broadcast("account", delta, msg_type="delta"), 
                        self.connector.main_loop
                    )
                except Exception as e:
                    self.logger.error(f"WS Broadcast error (account): {e}")

    def handle_position(self, account, contract, position, avgCost):
        self.logger.info(f"Position Update | Account: {account} | Symbol: {contract.symbol} | Position: {position} | AvgCost: {avgCost}")
        # 1. Update Portfolio Manager
        self.portfolio_manager.on_position_update(account, contract, position, avgCost)
        
        # 2. Dynamic Subscription: Ensure we are watching this contract
        if position != 0:
            self.connector.subscribe_contract(contract)

        if self.connector.is_ready:
            self.portfolio_manager.reconcile(self.connector.active_subscriptions)

    def handle_position_end(self):
        self.portfolio_manager.reconcile(self.connector.active_subscriptions)
        
        self.connector.positions_synced = True
        self.connector._check_system_ready()

        # Broadcast to WebSockets
        if self.connector.ws_manager and self.connector.main_loop:
            import asyncio
            try:
                data = self.get_active_portfolio()
                self.logger.info("Sending WS update to clients for topic 'portfolio'")
                asyncio.run_coroutine_threadsafe(
                    self.connector.ws_manager.broadcast("portfolio", data),
                    self.connector.main_loop
                )
            except Exception as e:
                self.logger.error(f"WS Broadcast error (portfolio): {e}")

    def get_active_portfolio(self):
        """
        Generates the full portfolio summary (same as /Contract/ListAllUnique).
        """
        results = []
        for g_con_id, data in self.connector.active_subscriptions.items():
            contract = data['contract']
            
            # Get reconciled positions
            positions_list = []
            # A. Accounts with Reconciled Positions
            for acc, positions in self.reconciled_positions.items():
                if g_con_id in positions:
                    positions_list.append({
                        "account": acc,
                        "qty": positions[g_con_id],
                        "avgPrice": self.reconciled_avg_costs.get(acc, {}).get(g_con_id, 0.0)
                    })
            
            # Get Last Price
            tick = self.connector.db_client.get_latest_tick(g_con_id)
            last_price = tick.get('price', 0.0) if tick else 0.0
            
            payload = {
                "gConId": g_con_id,
                "symbol": self.connector.req_id_to_symbol.get(data['req_id'], contract.symbol),
                "secType": contract.secType,
                "currency": contract.currency,
                "exchange": contract.exchange,
                "conId": contract.conId,
                "last": last_price,
                "positions": positions_list
            }

            # If BAG, include leg mapping for frontend synthetic pricing
            if contract.secType == "BAG" and contract.comboLegs:
                legs_info = []
                for leg in contract.comboLegs:
                    leg_g_con_id = self.connector.market_data_service._find_g_con_id_by_con_id(leg.conId)
                    if leg_g_con_id:
                        leg_tick = self.connector.db_client.get_latest_tick(leg_g_con_id)
                        leg_price = leg_tick.get('price', 0.0) if leg_tick else 0.0
                        
                        legs_info.append({
                            "gConId": leg_g_con_id,
                            "conId": leg.conId,
                            "ratio": leg.ratio,
                            "action": leg.action, 
                            "exchange": leg.exchange,
                            "lastPrice": leg_price
                        })
                payload['legs'] = legs_info

            results.append(payload)
        return results
