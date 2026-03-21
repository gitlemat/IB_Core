import time
import threading
import sys
import os
from typing import Any, Dict, List, Optional
from connection_manager import ConnectionManager
from datetime import datetime # Added for Optional[datetime]

# sys.path.append... REMOVED (ibapi is now in src)

try:
    from ibapi.contract import Contract, ComboLeg
    from ibapi.order import Order
except ImportError:
    class Contract: pass
    class ComboLeg: pass
    class Order: pass

from logger import LoggerSetup
from config import Config
from ib_wrapper import IBWrapper
from ib_client import IBClient
from watchlist import WatchlistManager
from db_client import DatabaseClient
from utils import (
    generate_g_con_id, map_tick_type, create_contract, create_order, 
    generate_spread_symbol_str, parse_contract_symbol, get_short_symbol,
    get_exchange_for_product, log_ib_object, clean_float
)
from models import IBContract, AccountSummary
from services.contract_service import ContractResolutionService
from services.order_service import OrderAndReconciliationService
from services.broadcaster_service import TickDataBroadcaster
from services.portfolio_service import PortfolioService
from services.connection_service import IBConnectionService
from services.market_data_service import MarketDataService


class IBConnector:
    """
    Main controller for the Interactive Brokers connection.
    Manages the Wrapper, Client, and background thread logic.
    """
    
    def __init__(self):
        self.logger = LoggerSetup.get_logger("IBConnector")
        
        # Initialize components
        self.wrapper = IBWrapper()
        self.client = IBClient(self.wrapper)
        
        # Link wrapper back to this app context
        self.wrapper.app_context = self
        
        # Managers
        self.watchlist_manager = WatchlistManager()
        self.db_client = DatabaseClient()
        self.lock = threading.RLock()
        
        # Cache for ExecId -> Context (timestamp + tags) for merging execution + commission
        self.execution_context: Dict[str, Dict] = {}
        
        # WebSocket Manager
        self.ws_manager: Optional[ConnectionManager] = None
        self.main_loop: Optional[asyncio.AbstractEventLoop] = None
        
        # Open Orders Sync Tracking (Moved to services or simplified)
        self.is_syncing_orders = False
        self.active_sync_order_ids: set = set()
        # Core Services Execution
        self.contract_service = ContractResolutionService(self)
        self.order_service = OrderAndReconciliationService(self)
        self.broadcaster_service = TickDataBroadcaster(self)
        self.portfolio_service = PortfolioService(self)
        self.connection_service = IBConnectionService(self)
        self.market_data_service = MarketDataService(self)
        
        # Reconciliation State
        self.last_db_execution_time: Optional[datetime] = None
        
        # State
        self.connected = False
        self.started = False
        
        # Mappings

        # reqId -> gConId used to map incoming Data
        self.req_id_to_g_con_id: Dict[int, str] = {}
        # reqId -> Symbol Name (calculated)
        self.req_id_to_symbol: Dict[int, str] = {}
        # conId -> Symbol Name (Cache for naming BAGs)
        self.symbol_cache = {}
        # gConId -> {'req_id': int, 'contract': Contract} (Active Subscriptions registry)
        self.active_subscriptions: Dict[str, Dict[str, Any]] = {}
        self.order_id_to_g_con_id: Dict[int, str] = {} # orderId -> gConId map
        
        # reqIds that should NOT be saved to DB (e.g. BAGs)
        self.non_persisted_req_ids: set = set()
        
        # Readiness Handling
        self.open_orders_synced = False
        self.positions_synced = False
        self.pending_resolutions: Dict[int, str] = {} # Map of reqId -> signature
        self.in_flight_signatures = set() # Set of active request signatures
        self.pending_open_orders = [] # Orders waiting for contract details
        
        # Order Watchdog
        self.pending_order_confirmations: Dict[int, float] = {} # orderId -> timestamp
        
        self.resolution_events: Dict[int, threading.Event] = {} # reqId -> Event for synchronous waits
        self.next_req_id = 1000


    def set_ws_manager(self, manager: ConnectionManager):
        import asyncio
        self.ws_manager = manager
        try:
            self.main_loop = asyncio.get_running_loop()
            self.logger.info("IBConnector captured the main event loop for WebSocket broadcasts.")
        except RuntimeError:
            self.logger.warning("IBConnector: set_ws_manager called without a running event loop.")

    def get_req_id(self) -> int:
        """
        Thread-safe method to get the next request ID.
        """
        with self.lock:
            req_id = self.next_req_id
            self.next_req_id += 1
        return req_id




    def start(self):
        """
        Starts the connection and the background thread loops.
        """
        self.logger.info("Starting IBConnector...")
        self.started = True
        
        # Start DB Flush Monitor
        self.db_client.start_monitor()

        # Start Tick Flush Loop (Throttling) 
        self.flush_thread = threading.Thread(target=self.broadcaster_service.flush_ticks_loop, daemon=True)
        self.flush_thread.start()
        
        # Start Pending Orders Watchdog Loop
        self.watchdog_thread = threading.Thread(target=self.order_service.monitor_pending_orders_loop, daemon=True)
        self.watchdog_thread.start()
        
        # Start Connection Service
        self.connection_service.start()
        
        self.logger.info("IBConnector started successfully.")
        
    @property
    def is_ready(self) -> bool:
        """Indicates if the IB connection is up, nextValidId received, and all startup streams synced."""
        has_id = getattr(self.wrapper, 'next_order_id', None) is not None
        is_synced = getattr(self, '_is_fully_synced', False)
        return self.connected and has_id and is_synced

    def stop(self):
        """
        Stops the connection.
        """
        self.started = False
        self.connection_service.stop()
        self.db_client.close()



    def _request_orders_update(self):
        """
        Forces a refresh of all open orders to ensure we capture any state changes 
        that missed the initial callback or are out of sync.
        """
        if self.client and self.client.isConnected():
            self.logger.debug("Requesting forced Open Orders update...")
            self.client.reqAllOpenOrders()

    def unsubscribe_contract(self, g_con_id: str):
        return self.market_data_service.unsubscribe_contract(g_con_id)

    def _find_g_con_id_by_con_id(self, con_id: int) -> Optional[str]:
        return self.market_data_service._find_g_con_id_by_con_id(con_id)

    def _is_leg_used_by_active_bags(self, leg_con_id: int) -> bool:
        return self.market_data_service._is_leg_used_by_active_bags(leg_con_id)

    def _is_contract_in_watchlist(self, contract: Contract) -> bool:
        return self.market_data_service._is_contract_in_watchlist(contract)

    def get_sync_contract_data(self, symbol_str: str, timeout: float = 5.0) -> Dict[str, Any]:
        return self.market_data_service.get_sync_contract_data(symbol_str, timeout)


    def on_connected(self):
        """
        Callback from Wrapper when connection is established.
        """
        self.connected = True
        self.logger.info("IB Connection Established. Starting Resync Sequence...")
        
        # 0. Trigger Startup Reconciliation (Context Cache + History Fetch)
        # We run this in a separate thread to avoid blocking the IBAPI Reader Thread
        # while waiting for synchronous InfluxDB queries (prevents heartbeat loss).
        threading.Thread(target=self.reconcile_executions, daemon=True).start()
        
        # 1. Request Account Summary
        # Valid tags for reqAccountSummary: NetLiquidation, TotalCashValue, SettledCash, AvailableFunds, BuyingPower, GrossPositionValue, MaintMarginReq, etc.
        # PnL tags are NOT supported here (they require reqPnL).
        tags = "NetLiquidation,TotalCashValue,AvailableFunds,BuyingPower,GrossPositionValue,MaintMarginReq"
        self.account_summary_req_id = self.get_req_id()
        self.client.reqAccountSummary(self.account_summary_req_id, "All", tags)
        
        # 2. Request Positions (triggers PortfolioManager)
        self.client.reqPositions()
        
        # 3. Request Open Orders (Client + TWS)
        self.client.reqAllOpenOrders()
        
        # 4. Set Market Data Type to 3 (Delayed) - requested by user
        self.logger.info("Setting Market Data Type to 3 (Delayed)...")
        self.client.reqMarketDataType(3)
        
        # 5. Subscribe Watchlist (Market Data)
        self.market_data_service.subscribe_watchlist()

        # 6. Resolve Symbols (for BAG naming)
        self.resolve_watchlist_symbols()
        
    def on_disconnected(self):
        """
        Callback from Wrapper when connection lost.
        """
        self.connected = False
        self.logger.warning("Disconnected from IB.")

    def handle_account_summary_end(self, req_id: int):
        self.portfolio_service.handle_account_summary_end(req_id)

    def resolve_watchlist_symbols(self):
        self.contract_service.resolve_watchlist_symbols(self.watchlist_manager.get_contracts())
        
    def resolve_contract_by_conid(self, con_id: int):
        self.contract_service.resolve_contract_by_conid(con_id)

    def on_contract_details_end(self, req_id: int):
        self.contract_service.on_contract_details_end(req_id, self.started)

    def on_error(self, req_id: int, error_code: int, error_string: str):
        self.contract_service.on_error(req_id, error_code, error_string, self.started)

    def on_open_orders_end(self):
        self.order_service.on_open_orders_end()

    def _check_system_ready(self):
        """
        Central gate that announces readiness when all initial streams
        and contract resolutions have fulfilled.
        """
        # Drain pending orders if resolutions are complete
        if not self.pending_resolutions and getattr(self, 'pending_open_orders', []):
            to_retry = self.pending_open_orders
            self.pending_open_orders = []
            self.logger.info(f"Resolutions complete. Retrying {len(to_retry)} deferred open orders.")
            for args in to_retry:
                self.handle_open_order(*args)

        if getattr(self, '_is_fully_synced', False):
            return
            
        if self.open_orders_synced and self.positions_synced and not self.pending_resolutions and not self.in_flight_signatures:
            self.logger.info("=========================================================")
            self.logger.info("IB_Core Startup Sequence Complete. System is now READY.")
            self.logger.info("=========================================================")
            
            # Additional late-stage bindings if any
            self._check_pending_bags()
            self._is_fully_synced = True
        else:
            waiting_for = []
            if not self.open_orders_synced: waiting_for.append("OpenOrders")
            if not self.positions_synced: waiting_for.append("Positions")
            if self.pending_resolutions or self.in_flight_signatures: waiting_for.append(f"Contracts({len(self.pending_resolutions)} pending)")
            self.logger.info(f"Startup Sequence pending: {', '.join(waiting_for)}")

    def get_g_con_id(self, contract: Contract) -> str:
        return self.contract_service.get_g_con_id(contract)
    def subscribe_contract(self, contract_data: Any):
        return self.market_data_service.subscribe_contract(contract_data)

    def subscribe_contract_by_conid(self, con_id: int):
        return self.market_data_service.subscribe_contract_by_conid(con_id)

    def _check_pending_bags(self):
        return self.market_data_service.check_pending_bags()

    def get_readable_contract_name(self, contract: Contract) -> str:
        return self.contract_service.get_readable_contract_name(contract)

    def get_next_order_id(self, count: int = 1) -> int:
        """
        Returns the next valid order ID from the wrapper and increments it.
        """
        if self.wrapper.next_order_id is None:
             self.logger.warning("next_order_id is None in wrapper. Using fallback 1.")
             self.wrapper.next_order_id = 1
             
        start_id = self.wrapper.next_order_id
        self.wrapper.next_order_id += count
        return start_id

    def resolve_bag_contract(self, contract: Contract):
        self.contract_service.resolve_bag_contract(contract)

    # --- Order Management ---

    def place_simple_order(self, contract: Contract, action: str, quantity: float, order_type: str, price: float = 0.0, lmtPrice: float = 0.0, auxPrice: float = 0.0) -> int:
        return self.order_service.place_simple_order(contract, action, quantity, order_type, price, lmtPrice, auxPrice)

    def request_open_orders_sync(self):
        """
        Manually triggers an open orders sync and tracks which orders are returned.
        Any previously known open orders not returned will be marked Unknown and removed.
        """
        with self.lock:
            self.is_syncing_orders = True
            self.active_sync_order_ids = set()
            self.logger.info("Manual Open Orders Sync requested. Tracking active orders...")
        self.client.reqOpenOrders()

    def modify_order(self, order_id: int, qty: float, lmt_price: float) -> bool:
        return self.order_service.modify_order(order_id, qty, lmt_price)

    def place_oca_order(self, order_id: Optional[int], contract_data: dict, oca_data: dict) -> dict:
        return self.order_service.place_oca_order(order_id, contract_data, oca_data)

    def place_bracket_order(self, contract: Contract, action: str, quantity: float, entry_price: float, tp_price: float, sl_price: float) -> dict:
        return self.order_service.place_bracket_order(contract, action, quantity, entry_price, tp_price, sl_price)
        
    def attach_bracket_to_execution(self, exec_id: str, action: str, quantity: float, tp_price: float, sl_price: float) -> dict:
        return self.order_service.attach_bracket_to_execution(exec_id, action, quantity, tp_price, sl_price)

    def cancel_order(self, order_id: int):
        self.order_service.cancel_order(order_id)


    # --- Callbacks fromWrapper ---

    def handle_tick_price(self, reqId: int, tickType: int, price: float):
        self.broadcaster_service.handle_tick_price(reqId, tickType, price)

    def handle_tick_size(self, reqId: int, tickType: int, size: int):
        self.broadcaster_service.handle_tick_size(reqId, tickType, size)


    def reconcile_executions(self):
        self.order_service.reconcile_executions()

    def handle_execution(self, req_id, contract, execution):
        self.order_service.handle_execution(req_id, contract, execution)

    def handle_commission_report(self, report):
        self.order_service.handle_commission_report(report)

    def get_all_orders(self):
        return self.order_service.get_all_orders()

    def handle_order_status(self, orderId, status, filled, remaining, avgFillPrice, permId, parentId, lastFillPrice, clientId):
        self.order_service.handle_order_status(orderId, status, filled, remaining, avgFillPrice, permId, parentId, lastFillPrice, clientId)

    def _check_and_write_order_status(self, order_id: int, force: bool = False) -> tuple[bool, dict]:
        return self.order_service.check_and_write_order_status(order_id, force)

    def handle_position(self, account, contract, position, avgCost):
        self.portfolio_service.handle_position(account, contract, position, avgCost)

    def handle_open_order(self, orderId, contract, order, orderState):
        self.order_service.handle_open_order(orderId, contract, order, orderState)

    def get_active_portfolio(self):
        return self.portfolio_service.get_active_portfolio()

    def handle_position_end(self):
        self.portfolio_service.handle_position_end()

    def reconcile_portfolio(self, subscriptions: Optional[Dict] = None):
        if subscriptions is None:
            subscriptions = self.active_subscriptions
        self.portfolio_service.portfolio_manager.reconcile(subscriptions)

    def expand_bag_legs(self, contract: Contract, metadata_legs: Optional[List[Dict]] = None) -> List[Dict]:
        return self.market_data_service.expand_bag_legs(contract, metadata_legs)

    def _update_dependent_bags(self, trigger_g_con_id: str):
        return self.market_data_service.update_dependent_bags(trigger_g_con_id)

    def _update_dependent_bags_by_conid(self, con_id: int):
        return self.market_data_service.update_dependent_bags_by_conid(con_id)

    def _recalculate_bag_price(self, g_con_id: str, data: Dict):
        return self.market_data_service.recalculate_bag_price(g_con_id, data)
