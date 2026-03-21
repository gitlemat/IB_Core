import time
import threading
from typing import Dict, Any, List, Optional
from datetime import datetime, timezone

from ibapi.contract import Contract
from ibapi.order import Order
from ibapi.execution import ExecutionFilter

from .base_service import IBBaseService
from utils import create_order, get_bracket_reference, clean_float
from config import Config

class OrderAndReconciliationService(IBBaseService):
    def __init__(self, connector: 'IBConnector'):
        super().__init__(connector)
        # Cache for OrderId -> Last Written State for change detection
        self.order_status_cache: Dict[int, Dict] = {}
        self._last_forced_req_time = 0.0
        
    def place_simple_order(self, contract: Contract, action: str, quantity: float, order_type: str, price: float = 0.0, lmtPrice: float = 0.0, auxPrice: float = 0.0) -> int:
        """
        Places a simple order.
        """
        order_id = self.connector.get_next_order_id()
        
        o = create_order(
            action=action,
            quantity=quantity,
            order_type=order_type,
            lmt_price=lmtPrice if lmtPrice else price,
            aux_price=auxPrice
        )
        # Avoid GTC for BAGs, day orders usually safer for combos unless setup exactly right
        if contract.secType == "BAG":
            o.tif = "DAY"
            
        self.logger.info(f"Placing Order {order_id} | {action} {quantity} {contract.symbol} @ {o.lmtPrice}")
        
        with self.connector.lock:
            self.connector.pending_order_confirmations[order_id] = time.time()
            
        self.connector.client.placeOrder(order_id, contract, o)
        return order_id

    def place_bracket_order(self, contract: Contract, action: str, quantity: float, 
                           entry_price: float, tp_price: float, sl_price: float) -> dict:
        """
        Places a full bracket order (Entry + TP + SL).
        """
        parent_id = self.connector.get_next_order_id(3)
        tp_id = parent_id + 1
        sl_id = parent_id + 2
        
        # Parent limits
        parent = create_order(action, quantity, "LMT", lmt_price=entry_price)
        parent.orderId = parent_id
        parent.transmit = False
        
        # Determine child actions
        child_action = "SELL" if action.upper() == "BUY" else "BUY"
        
        # Take Profit
        tp = create_order(child_action, quantity, "LMT", lmt_price=tp_price)
        tp.orderId = tp_id
        tp.parentId = parent_id
        tp.transmit = False
        
        # Stop Loss
        sl = create_order(child_action, quantity, "STP", aux_price=sl_price)
        sl.orderId = sl_id
        sl.parentId = parent_id
        sl.transmit = True # Transmit the whole bracket
        
        self.logger.info(f"Placing Bracket [{parent_id}] | Entry: {entry_price}, TP: {tp_price}, SL: {sl_price}")
        
        with self.connector.lock:
            self.connector.pending_order_confirmations[parent_id] = time.time()
            self.connector.pending_order_confirmations[tp_id] = time.time()
            self.connector.pending_order_confirmations[sl_id] = time.time()
            
        self.connector.client.placeOrder(parent_id, contract, parent)
        self.connector.client.placeOrder(tp_id, contract, tp)
        self.connector.client.placeOrder(sl_id, contract, sl)
        
        return {
            "entry_id": parent_id,
            "tp_id": tp_id,
            "sl_id": sl_id
        }
        
    def attach_bracket_to_execution(self, exec_id: str, action: str, quantity: float, tp_price: float, sl_price: float) -> dict:
        """
        Attaches a TP and SL to an existing execution.
        """
        with self.connector.lock:
            context = self.connector.execution_context.get(exec_id)
            if not context:
                # Try DB
                context = self.connector.db_client.get_execution_context(exec_id)
        
        if not context:
            raise ValueError(f"Execution {exec_id} not found in cache. Cannot attach bracket.")
            
        tags = context.get('tags', {})
        sym_str = tags.get('Symbol')
        order_ref = tags.get('Strategy', "")
        
        if not sym_str:
            raise ValueError("Execution context missing symbol. Cannot attach bracket.")
            
        # Try to resolve contract locally since we only have string
        contract = None
        for g_con_id, data in self.connector.active_subscriptions.items():
            if self.connector.get_readable_contract_name(data['contract']) == sym_str:
                contract = data['contract']
                break
                
        if not contract:
            raise ValueError(f"Contract {sym_str} not found in active subscriptions.")
            
        tp_id = self.connector.get_next_order_id(2)
        sl_id = tp_id + 1
        
        child_action = "SELL" if action.upper() == "BUY" else "BUY"
        bracket_ref = get_bracket_reference()
        
        tp = create_order(child_action, quantity, "LMT", lmt_price=tp_price)
        tp.orderId = tp_id
        tp.orderRef = bracket_ref
        tp.transmit = False
        
        sl = create_order(child_action, quantity, "STP", aux_price=sl_price)
        sl.orderId = sl_id
        sl.orderRef = bracket_ref
        sl.transmit = True 
        
        self.logger.info(f"Attaching Bracket to {sym_str} [{exec_id}] | Ref: {bracket_ref} | TP: {tp_price}, SL: {sl_price}")
        
        with self.connector.lock:
            self.connector.pending_order_confirmations[tp_id] = time.time()
            self.connector.pending_order_confirmations[sl_id] = time.time()
            
        self.connector.client.placeOrder(tp_id, contract, tp)
        self.connector.client.placeOrder(sl_id, contract, sl)
        
        return {
            "tp_id": tp_id,
            "sl_id": sl_id,
            "ref": bracket_ref
        }

    def cancel_order(self, order_id: int):
        """
        Cancels an order by ID.
        """
        self.logger.info(f"Cancelling Order {order_id}")
        self.connector.client.cancelOrder(order_id, "")
        # Force refresh via the dedicated method
        self.connector._request_orders_update()

    def modify_order(self, order_id: int, qty: float, lmt_price: float) -> bool:
        """
        Modifies an existing open order by sending a new order with the same orderId.
        """
        order_entry = self.connector.wrapper.open_orders.get(order_id)
        if not order_entry:
            self.logger.warning(f"Order {order_id} not found in open_orders cache. Cannot modify.")
            return False
        
        contract = order_entry['contract']
        original_order = order_entry['order']
        
        # Create a new order object with updated fields
        new_order = Order()
        new_order.action = original_order.action
        new_order.orderType = original_order.orderType
        new_order.totalQuantity = qty
        new_order.lmtPrice = lmt_price
        new_order.tif = original_order.tif
        new_order.ocaGroup = original_order.ocaGroup
        new_order.parentId = original_order.parentId
        new_order.transmit = True

        self.logger.info(f"Modifying Order {order_id}. New Qty: {qty}, New Lmt: {lmt_price}")
        self.connector.client.placeOrder(order_id, contract, new_order)
        
        # Force refresh
        self.connector._request_orders_update()
        return True

    def place_oca_order(self, order_id: Optional[int], contract_data: dict, oca_data: dict) -> dict:
        """
        Places an OCA (One-Cancels-All) Group.
        """
        if order_id is None:
            order_id = self.connector.get_next_order_id(2)
        
        from utils import create_contract
        contract = create_contract(contract_data)
        self.connector.contract_service.resolve_bag_contract(contract)
        
        oca_group = f"OCA_{order_id}"
        order_ref = oca_data.get('orderRef', '')
        
        # Stop Loss Order
        sl_id = order_id
        sl_order = create_order(
            action=oca_data.get('actionSL'),
            quantity=float(oca_data.get('qty', 0)),
            order_type="STP",
            aux_price=float(oca_data.get('LmtPriceSL', 0)),
            tif=oca_data.get('tif'),
            order_ref=order_ref
        )
        sl_order.ocaGroup = oca_group
        sl_order.ocaType = 1
        
        # Take Profit Order
        tp_id = order_id + 1
        tp_order = create_order(
            action=oca_data.get('actionTP'),
            quantity=float(oca_data.get('qty', 0)),
            order_type="LMT",
            lmt_price=float(oca_data.get('LmtPrice', 0)),
            tif=oca_data.get('tif'),
            order_ref=order_ref
        )
        tp_order.ocaGroup = oca_group
        tp_order.ocaType = 1
        
        # Account Handling
        account_id = oca_data.get('accountId')
        if not account_id:
            available_accounts = sorted(self.connector.wrapper.account_summary.keys())
            if available_accounts:
                account_id = available_accounts[0]
        
        if account_id:
            sl_order.account = account_id
            tp_order.account = account_id
        
        self.logger.info(f"Placing OCA Group {oca_group} for {contract.symbol}. IDs: {sl_id} (SL), {tp_id} (TP)")
        
        self.connector.client.placeOrder(sl_id, contract, sl_order)
        self.connector.client.placeOrder(tp_id, contract, tp_order)
        
        # Track pending confirmations
        now = time.time()
        with self.connector.lock:
            self.connector.pending_order_confirmations[sl_id] = now
            self.connector.pending_order_confirmations[tp_id] = now
        
        self.connector._request_orders_update()
        return {"SL": sl_id, "TP": tp_id}

    def reconcile_executions(self):
        """
        Performs startup reconciliation: Loads caches with ALL recent executions and requests updates.
        """
        self.logger.info("Starting Execution Reconciliation...")
        
        # 1. Load recent executions into cache
        incomplete = self.connector.db_client.get_recent_executions_context()
        count = 0
        with self.connector.lock:
            for ctx in incomplete:
                self.connector.execution_context[ctx['execId']] = {
                    'timestamp': ctx['timestamp'],
                    'tags': ctx['tags']
                }
                count += 1
        self.logger.info(f"Reconciliation: Loaded {count} recent executions into cache.")
        
        # 2. Get last execution time
        self.connector.last_db_execution_time = self.connector.db_client.get_last_execution_time()
        if self.connector.last_db_execution_time:
            self.logger.info(f"Reconciliation: Last known execution time in DB: {self.connector.last_db_execution_time}")
        else:
            self.logger.info("Reconciliation: No previous executions found in DB.")
            
        # 3. Request today's executions from IB
        self.logger.info("Reconciliation: Requesting daily executions from IB...")
        self.connector.client.reqExecutions(self.connector.get_req_id(), ExecutionFilter())

    def handle_execution(self, req_id, contract, execution):
        """
        Handles execution reports.
        """
        try:
            clean_time_str = execution.time.replace("  ", " ")
            exec_timestamp = datetime.strptime(clean_time_str, "%Y%m%d %H:%M:%S").replace(tzinfo=timezone.utc)
        except ValueError:
            self.logger.warning(f"Could not parse execution time '{execution.time}', using now().")
            exec_timestamp = datetime.now(timezone.utc)
            
        symbol_name = self.connector.get_readable_contract_name(contract)

        context = {
            'timestamp': exec_timestamp,
            'tags': {
                'Symbol': symbol_name,
                'AccountId': execution.accountId,
                'PermId': execution.permId,
                'ExecId': execution.execId,
                'Strategy': execution.orderRef
            }
        }

        with self.connector.lock:
            self.connector.execution_context[execution.execId] = context

        if self.connector.last_db_execution_time and exec_timestamp <= self.connector.last_db_execution_time:
            self.logger.debug(f"Skipping execution {execution.execId} ({exec_timestamp}) as it is older than last DB entry")
            return

        exec_data = {
            "execId": execution.execId,
            "orderId": execution.orderId,
            "symbol": symbol_name,
            "side": execution.side,
            "quantity": float(execution.shares),
            "fillPrice": float(execution.price),
            "permId": execution.permId,
            "secType": contract.secType,
            "avgPrice": execution.avgPrice,
            "accountId": execution.accountId,
            "strategy": execution.orderRef 
        }
        self.connector.db_client.write_execution(exec_data, Config.is_paper_trading(), timestamp=exec_timestamp)

        if self.connector.ws_manager and self.connector.main_loop:
            import asyncio
            ws_payload = exec_data.copy()
            ws_payload['timestamp'] = exec_timestamp.isoformat()
            
            try:
                self.logger.info(f"Sending WS update to clients for topic 'executions' (ExecId: {execution.execId})")
                asyncio.run_coroutine_threadsafe(
                    self.connector.ws_manager.broadcast("executions", ws_payload),
                    self.connector.main_loop
                )
            except Exception as e:
                self.logger.error(f"Failed to broadcast execution: {e}")

    def handle_open_order(self, orderId, contract, order, orderState):
        """
        Handles open order reports.
        """
        # Ensure we have a gConId and the contract is registered
        g_con_id = self.connector.get_g_con_id(contract)
        
        if g_con_id == "UNRESOLVED_BAG":
            self.logger.debug(f"Order #{orderId} refers to an Unresolved BAG. Queueing for retry.")
            self.connector.pending_open_orders.append((orderId, contract, order, orderState))
            return
            
        self.connector.order_id_to_g_con_id[orderId] = g_con_id
        
        # Acknowledge order creation if it was pending
        self.connector.pending_order_confirmations.pop(orderId, None)
        
        # Dynamic Subscription: Ensure we are watching this contract
        self.connector.subscribe_contract(contract)
        
        # Trigger change detection and write to InfluxDB
        has_changed, delta = self.check_and_write_order_status(orderId)

        if self.connector.is_syncing_orders:
            with self.connector.lock:
                self.connector.active_sync_order_ids.add(orderId)

        if has_changed and self.connector.ws_manager and self.connector.main_loop:
            import asyncio
            try:
                self.logger.info(f"Sending WS update to clients for topic 'orders' (OpenOrder: {orderId})")
                asyncio.run_coroutine_threadsafe(
                    self.connector.ws_manager.broadcast("orders", delta, msg_type="delta"),
                    self.connector.main_loop
                )
            except Exception as e:
                self.logger.error(f"WS Broadcast error (orders): {e}")

    def handle_order_status(self, orderId, status, filled, remaining, avgFillPrice, permId, parentId, lastFillPrice, clientId):
        # Acknowledge order creation if it was pending
        with self.connector.lock:
            self.connector.pending_order_confirmations.pop(orderId, None)
        
        # Trigger change detection and write to InfluxDB
        has_changed, delta = self.check_and_write_order_status(orderId)
        if has_changed:
            self.logger.info(f"Order {orderId} changed. Scheduling WS broadcast.")
        
        # Broadcast to WebSockets ONLY if changed
        if has_changed and self.connector.ws_manager and self.connector.main_loop:
            import asyncio
            try:
                self.logger.info(f"Sending WS update to clients for topic 'orders' (OrderId: {orderId})")
                asyncio.run_coroutine_threadsafe(
                    self.connector.ws_manager.broadcast("orders", delta, msg_type="delta"),
                    self.connector.main_loop
                )
            except Exception as e:
                self.logger.error(f"WS Broadcast error (orders): {e}")

    def check_and_write_order_status(self, order_id: int, force: bool = False) -> tuple[bool, dict]:
        """
        Combines order info and status to check for changes and write only changed fields to InfluxDB.
        Returns: (has_changed, delta_dict)
        """
        if not self.connector.open_orders_synced and not force:
            return False, {}
            
        open_data = self.connector.wrapper.open_orders.get(order_id)
        status_data = self.connector.wrapper.order_statuses.get(order_id)
        
        if not open_data:
            self.logger.warning(f"Order {order_id} not found in open_orders. Discarding status update.")
            return False, {}
            
        order = open_data['order']
        contract = open_data['contract']
        order_state = open_data.get('state')
    
        with self.connector.lock:
            last_state = self.order_status_cache.get(order_id)
            
            status_fb = "Submitted"
            if order_state and hasattr(order_state, 'status') and order_state.status:
                status_fb = order_state.status
            if last_state and last_state.get("status"):
                status_fb = last_state.get("status")
                
            filled_fb = float(last_state.get("filled", 0.0)) if last_state else 0.0
            remaining_fb = float(last_state.get("remaining", order.totalQuantity)) if last_state else float(order.totalQuantity)
            last_fill_fb = float(last_state.get("lastFillPrice", 0.0)) if last_state else 0.0

            current_state = {
                "accountId": order.account,
                "orderId": order_id,
                "symbol": self.connector.contract_service.get_readable_contract_name(contract),
                "action": order.action,
                "totalQuantity": float(order.totalQuantity),
                "orderType": order.orderType,
                "tif": order.tif,
                "lmtPrice": clean_float(order.lmtPrice) or 0.0,
                "auxPrice": clean_float(order.auxPrice) or 0.0,
                "permId": str(order.permId),
                "status": status_data.get("status") if status_data else status_fb,
                "filled": float(status_data.get("filled", filled_fb)) if status_data else filled_fb,
                "remaining": float(status_data.get("remaining", remaining_fb)) if status_data else remaining_fb,
                "lastFillPrice": float(status_data.get("lastFillPrice", last_fill_fb)) if status_data else last_fill_fb
            }
            
            changed_data = {
                "accountId": current_state["accountId"],
                "symbol": current_state["symbol"],
                "orderId": current_state["orderId"],
                "permId": current_state["permId"]
            }
            
            is_initial_write = (last_state is None)
            any_change = False
            
            for k, v in current_state.items():
                if k in ["accountId", "symbol", "orderId", "permId"]:
                    continue 
                
                is_changed = False
                last_v = None
                if not last_state:
                    is_changed = True
                else:
                    last_v = last_state.get(k)
                    if k in ['lmtPrice', 'auxPrice', 'totalQuantity', 'filled', 'remaining', 'lastFillPrice']:
                        if abs(float(v) - float(last_v or 0.0)) > 0.000001:
                            is_changed = True
                    else:
                        if str(v) != str(last_v):
                            is_changed = True
                
                if is_changed:
                    changed_data[k] = v
                    any_change = True
            
            if not any_change:
                return False, {}
            
            data_to_write = current_state if is_initial_write else changed_data
            self.connector.db_client.write_order_status(data_to_write)
            self.order_status_cache[order_id] = current_state

            return True, changed_data

    def get_all_orders(self) -> Dict[int, Dict[str, Any]]:
        """
        Returns all active orders in the format expected by the API.
        """
        orders_data = {}
        for oid, data in self.connector.wrapper.open_orders.items():
            order = data['order']
            contract = data['contract']
            status_info = self.connector.wrapper.order_statuses.get(oid, {})
            if isinstance(status_info, str):
                status_info = {"status": status_info}

            orders_data[oid] = {
                 "accountId": order.account,
                 "symbol": self.connector.contract_service.get_readable_contract_name(contract),
                 "action": order.action,
                 "totalQuantity": order.totalQuantity,
                 "orderType": order.orderType,
                 "tif": order.tif,
                 "lmtPrice": order.lmtPrice,
                 "auxPrice": order.auxPrice,
                 "permId": order.permId,
                 "status": status_info.get("status", "Unknown"),
                 "filled": float(status_info.get("filled", 0.0)),
                 "remaining": float(status_info.get("remaining", order.totalQuantity)),
                 "avgFillPrice": float(status_info.get("avgFillPrice", 0.0))
            }
        return orders_data

    def handle_commission_report(self, report):
        """
        Handles commission reports.
        """
        exec_id = report.execId
        timestamp = None
        extra_tags = None
        
        with self.connector.lock:
            context = self.connector.execution_context.get(exec_id)
            if context:
                timestamp = context.get('timestamp')
                extra_tags = context.get('tags')
            
        if not timestamp or not extra_tags:
            self.logger.warning(f"ExecId {exec_id} context missing in memory. Attempting DB recovery...")
            context = self.connector.db_client.get_execution_context(exec_id)
            if context:
                timestamp = context.get('timestamp')
                extra_tags = context.get('tags')
            
        if not timestamp:
            self.logger.error(f"ExecId {exec_id} not found in cache OR DB. Discarding commission report.")
            return

        comm_data = {
            "execId": exec_id,
            "commission": float(report.commission if report.commission else 0.0),
            "currency": report.currency,
            "realizedPNL": float(report.realizedPNL if report.realizedPNL else 0.0),
            "yield": float(report.yield_ if report.yield_ else 0.0),
            "yieldRedemptionDate": str(report.yieldRedemptionDate)
        }
        self.connector.db_client.write_commission(comm_data, Config.is_paper_trading(), timestamp=timestamp, extra_tags=extra_tags)

    def monitor_pending_orders_loop(self):
        """
        Periodically checks for placed orders that haven't received an openOrder or orderStatus.
        """
        self.logger.info("Order Watchdog Loop started.")
        
        while self.connector.started:
            try:
                time.sleep(2)
                
                if not self.connector.client or not self.connector.client.isConnected():
                    continue
                    
                now = time.time()
                needs_refresh = False
                timeout_ids = []
                
                with self.connector.lock:
                    pending_items = list(self.connector.pending_order_confirmations.items())
                    
                for order_id, created_at in pending_items:
                    elapsed = now - created_at
                    
                    if elapsed > 60:
                        self.logger.warning(f"[WATCHDOG] OrderId {order_id} pending over 60s. Dropping.")
                        timeout_ids.append(order_id)
                        continue
                        
                    if elapsed >= 2.0:
                        needs_refresh = True
                        self.logger.info(f"[WATCHDOG] Delayed confirmation OrderId {order_id}. {elapsed:.2f}s")
                
                with self.connector.lock:
                    for order_id in timeout_ids:
                        self.connector.pending_order_confirmations.pop(order_id, None)
                    
                if needs_refresh:
                    time_since_last_req = now - self._last_forced_req_time
                    if time_since_last_req > 5.0:
                        self.logger.warning("[WATCHDOG] Triggering reqOpenOrders() to recover missing confirmations.")
                        self._last_forced_req_time = now
                        self.connector.client.reqOpenOrders()
                        
            except Exception as e:
                self.logger.error(f"[WATCHDOG] Loop Error: {e}", exc_info=True)

    def on_open_orders_end(self):
        """
        Called when initial Open Orders sync is done.
        We check if we have pending resolutions. If not, we are ready.
        """
        # --- NEW: Order Status Reconciliation (Cache Warming) ---
        active_ids = list(self.connector.wrapper.open_orders.keys())
        if active_ids:
            self.logger.info(f"Warming Order Status Cache for {len(active_ids)} active orders...")
            # We run this in a thread because it's a synchronous DB query
            threading.Thread(target=self._reconcile_orders, args=(active_ids,), daemon=True).start()

        # --- NEW: Order Status Reconciliation (Manual Sync Cleanup) ---
        if self.connector.is_syncing_orders:
            with self.connector.lock:
                stale_orders = set(self.connector.wrapper.open_orders.keys()) - self.connector.active_sync_order_ids
                if stale_orders:
                    self.logger.warning(f"Sync complete. Found {len(stale_orders)} stale orders not present in IB stream. Marking as Unknown.")
                    for oid in stale_orders:
                        if oid in self.connector.wrapper.open_orders:
                            # Safely handle the status dictionary
                            if oid not in self.connector.wrapper.order_statuses or isinstance(self.connector.wrapper.order_statuses[oid], str):
                                self.connector.wrapper.order_statuses[oid] = {}
                            self.connector.wrapper.order_statuses[oid]["status"] = "Unknown"
                            
                            # Write Unknown status to DB and broadcast to GUI
                            has_changed, delta = self.check_and_write_order_status(oid)
                            if has_changed and self.connector.ws_manager and self.connector.main_loop:
                                import asyncio
                                try:
                                    self.logger.info(f"Sending WS update to clients for topic 'orderStatus' (stale {oid})")
                                    asyncio.run_coroutine_threadsafe(
                                        self.connector.ws_manager.broadcast_json({"type": "orderStatus", "data": delta}),
                                        self.connector.main_loop
                                    )
                                except Exception as e:
                                    self.logger.error(f"Failed to broadcast stale update for {oid}: {e}")
                            
                            # Clean up tracking dictionaries
                            del self.connector.wrapper.open_orders[oid]
                            del self.connector.wrapper.order_statuses[oid]
                self.connector.is_syncing_orders = False
                self.connector.active_sync_order_ids.clear()
        
        self.connector.open_orders_synced = True
        self.connector._check_system_ready()

    def _reconcile_orders(self, order_ids: List[int]):
        """
        Background task to warm the order status cache.
        """
        states = self.connector.db_client.get_last_order_states(order_ids)
        with self.connector.lock:
            self.order_status_cache.update(states)
            
        self.logger.info(f"Order Cache warmed with {len(states)} records from InfluxDB.")
        
        # After warming, trigger a check for all current orders to capture any changes 
        # that happened while the app was offline OR during the sync itself.
        for oid in order_ids:
            has_changed, delta = self.check_and_write_order_status(oid, force=True)
            if has_changed and self.connector.ws_manager and self.connector.main_loop:
                import asyncio
                try:
                    self.logger.info(f"Sending WS update to clients for topic 'orders' (startup {oid})")
                    asyncio.run_coroutine_threadsafe(
                        self.connector.ws_manager.broadcast("orders", delta, msg_type="delta"),
                        self.connector.main_loop
                    )
                except Exception as e:
                    self.logger.error(f"Failed to broadcast startup update for {oid}: {e}")
