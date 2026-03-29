import time
import threading
from typing import Dict, Any, List, Optional
from ibapi.contract import Contract, ComboLeg
from .base_service import IBBaseService
from utils import (
    create_contract, get_exchange_for_product, clean_float, 
    parse_single_leg_details, parse_contract_symbol
)

class MarketDataService(IBBaseService):
    def __init__(self, connector: 'IBConnector'):
        super().__init__(connector)
        # Tick Buffer for Throttling (if we wanted to move it here, 
        # but broadcaster uses it too. For now let's focus on logic)
        self.pending_bags: List[Dict] = []

    def subscribe_contract_by_conid(self, con_id: int):
        """
        Subscribes to market data for a contract using its conId.
        """
        details = self.connector.symbol_cache.get(con_id)
        if isinstance(details, dict):
            c = Contract()
            c.conId = con_id
            c.symbol = details.get('symbol', '')
            c.localSymbol = details.get('localSymbol', '')
            c.secType = details.get('secType', 'FUT')
            c.currency = details.get('currency', 'USD')
            c.exchange = details.get('exchange', get_exchange_for_product(c.symbol))
            c.lastTradeDateOrContractMonth = details.get('expiry', '')
            c.multiplier = details.get('multiplier', '')
            c.primaryExchange = details.get('primaryExchange', '')
            
            self.subscribe_contract(c)
        else:
            self.logger.debug(f"ConId {con_id} not in cache. Resolving before subscription...")
            self.connector.contract_service.resolve_contract_by_conid(con_id)

    def subscribe_contract(self, contract_data: Any):
        """
        Requests streaming data for a contract.
        If BAG, decomposes into legs and subscribes to them individually.
        """
        if isinstance(contract_data, dict):
            contract = create_contract(contract_data) 
            legs = contract_data.get('legs')
            product = contract_data.get('product', '')
            month = contract_data.get('month', '')
            year = contract_data.get('year', '')
            
            if legs and contract.secType == 'BAG':
                contract._metadata_legs = legs
            
            if not product and contract.secType != "BAG":
                details = parse_single_leg_details(contract.symbol)
                product = details.get('product', '')
                month = details.get('month', '')
                year = details.get('year', '')
            
            g_con_id = self.connector.contract_service.get_g_con_id(contract)
            contract._g_con_id = g_con_id

            if product and contract.symbol != product:
                if not contract.localSymbol:
                     contract.localSymbol = contract.symbol
                contract.symbol = product
        else:
            contract = contract_data
            legs = None
            g_con_id = self.connector.contract_service.get_g_con_id(contract)
            contract._g_con_id = g_con_id

            if contract.secType != "BAG":
                details = parse_single_leg_details(contract.localSymbol or contract.symbol)
                product = details.get('product', '')
                month = details.get('month', '')
                year = details.get('year', '')
                
                if product and contract.symbol != product:
                    if not contract.localSymbol:
                         contract.localSymbol = contract.symbol
                    contract.symbol = product
            else:
                 product = month = year = ""

        # Check Registry
        with self.connector.lock:
            if g_con_id in self.connector.active_subscriptions:
                existing_sub = self.connector.active_subscriptions[g_con_id]
                if not existing_sub.get('contract'):
                    existing_sub['contract'] = contract
                else:
                    self.logger.debug(f"Contract {contract.symbol} (ID: {g_con_id}) already in registry.")
                    if contract.secType != "BAG":
                        self.update_dependent_bags(g_con_id)
                    
                    if contract.secType != "BAG" and g_con_id not in self.connector.db_client.latest_prices:
                        symbol_name = self.connector.contract_service.get_readable_contract_name(contract)
                        last_record = self.connector.db_client.get_last_price_record(symbol_name)
                        if any(v > 0 for v in last_record.values()):
                            self.connector.db_client.latest_prices[g_con_id] = last_record
                            self.logger.info(f"Syncing pricing for EXISTING contract {symbol_name} from DB: {last_record}")
                            self.update_dependent_bags(g_con_id)
                    return

        # If it's a BAG, we register it but DO NOT subscribe to it directly
        if contract.secType == 'BAG':
            with self.connector.lock:
                if g_con_id not in self.connector.active_subscriptions:
                    self.logger.info(f"Registering BAG {contract.symbol} (gConId: {g_con_id}). Decomposing into legs...")
                    self.connector.active_subscriptions[g_con_id] = {
                        "req_id": None,
                        "contract": contract,
                        "legs": legs,
                        "product": product,
                        "month": month,
                        "year": year
                    }
                    self.update_dependent_bags(g_con_id)
            
            if legs and not hasattr(contract, '_metadata_legs'):
                contract._metadata_legs = legs
            
            reg_contract = self.connector.active_subscriptions[g_con_id]['contract']
            reg_legs = self.connector.active_subscriptions[g_con_id].get('legs')

            if reg_legs:
                for l in reg_legs:
                    leg_contract_info = {
                        "symbol": l.get('symbol'),
                        "secType": l.get('secType', 'FUT'),
                        "lastTradeDateOrContractMonth": l.get('expiry') or l.get('lastTradeDateOrContractMonth'),
                        "exchange": l.get('exchange', get_exchange_for_product(l.get('symbol'))),
                        "currency": l.get('currency', 'USD'),
                        "strike": l.get('strike', 0.0),
                        "right": l.get('right', '')
                    }
                    self.subscribe_contract(leg_contract_info)
            
            if reg_contract.comboLegs:
                for leg in reg_contract.comboLegs:
                    self.subscribe_contract_by_conid(leg.conId)
            
            if reg_legs and not reg_contract.comboLegs:
                all_resolved = True
                combo_legs = []
                for l in reg_legs:
                    l_conid = None
                    l_name = l.get('symbol')
                    l_expiry = l.get('expiry')
                    
                    for cid, details in self.connector.symbol_cache.items():
                        if isinstance(details, dict) and details.get('symbol') == l_name:
                             cache_expiry = details.get('expiry', "")
                             if cache_expiry.startswith(l_expiry):
                                l_conid = cid
                                break
                    
                    if l_conid:
                        clem = ComboLeg()
                        clem.conId = l_conid
                        clem.ratio = l.get('ratio', 1)
                        clem.action = l.get('action', 'BUY')
                        clem.exchange = l.get('exchange', get_exchange_for_product(l_name))
                        combo_legs.append(clem)
                    else:
                        all_resolved = False
                        break
                
                if all_resolved:
                    reg_contract.comboLegs = combo_legs
                    self.logger.info(f"BAG {reg_contract.symbol} legs fully resolved and comboLegs populated. Triggering initial pricing and direct subscription.")
                    
                    # Trigger initial pricing calculation
                    self._recalculate_bag_price(g_con_id, self.connector.active_subscriptions[g_con_id])
                    
                    # Request direct market data for the BAG Spread
                    req_id = self.connector.active_subscriptions[g_con_id].get('req_id')
                    if not req_id:
                        req_id = self.connector.get_req_id()
                        self.connector.active_subscriptions[g_con_id]['req_id'] = req_id
                        self.connector.req_id_to_g_con_id[req_id] = g_con_id
                    
                    self.connector.client.reqMktData(req_id, reg_contract, "", False, False, [])
                else:
                    self.logger.info(f"BAG {reg_contract.symbol} legs still not fully resolved.")
                    if contract_data not in self.pending_bags:
                        self.pending_bags.append(contract_data)

            return 
            
        # Single-Leg Subscription
        req_id = self.connector.get_req_id()
        
        with self.connector.lock:
            self.connector.req_id_to_g_con_id[req_id] = g_con_id
            self.connector.active_subscriptions[g_con_id] = {
                "req_id": req_id,
                "contract": contract,
                "product": product,
                "month": month,
                "year": year
            }
        
        is_new_pricing = False
        if g_con_id not in self.connector.db_client.latest_prices:
             symbol_name = self.connector.contract_service.get_readable_contract_name(contract)
             last_record = self.connector.db_client.get_last_price_record(symbol_name)
             if any(v > 0 for v in last_record.values()):
                 self.connector.db_client.latest_prices[g_con_id] = last_record
                 self.logger.info(f"Initialized pricing for {symbol_name} from DB: {last_record}")
                 is_new_pricing = True
        
        if is_new_pricing:
            self.update_dependent_bags(g_con_id)
        
        with self.connector.lock:
            self.connector.req_id_to_symbol[req_id] = contract.localSymbol if contract.localSymbol else contract.symbol
             
        self.logger.info(f"Subscribing to {self.connector.req_id_to_symbol.get(req_id, 'Unknown')} (ReqId: {req_id}, gConId: {g_con_id})...")
        self.connector.client.reqMktData(req_id, contract, "", False, False, [])

    def check_pending_bags(self):
        """
        Iterates through pending bags and tries to subscribe if all legs are now in cache.
        """
        if not self.pending_bags:
            return
            
        still_pending = []
        for bag_data in self.pending_bags:
            legs = bag_data.get('legs', [])
            all_resolved = True
            for l in legs:
                 l_name = l.get('symbol')
                 l_expiry = l.get('expiry')
                 found = False
                 for cid, details in self.connector.symbol_cache.items():
                     if isinstance(details, dict) and details.get('symbol') == l_name:
                         cache_expiry = details.get('expiry', "")
                         if cache_expiry.startswith(l_expiry):
                             found = True
                             break
                 if not found:
                     all_resolved = False
                     break
            
            if all_resolved:
                self.logger.info(f"All legs resolved for {bag_data.get('symbol')}. Re-triggering subscription.")
                self.subscribe_contract(bag_data)
            else:
                still_pending.append(bag_data)
        
        self.pending_bags = still_pending

    def expand_bag_legs(self, contract: Contract, metadata_legs: Optional[List[Dict]] = None) -> List[Dict]:
        """
        Unified helper to expand a BAG into detailed leg dicts.
        """
        resolved_legs = []
        source_legs = []
        if hasattr(contract, 'comboLegs') and contract.comboLegs:
            for leg in contract.comboLegs:
                source_legs.append({
                    "conId": leg.conId,
                    "ratio": leg.ratio,
                    "action": leg.action,
                    "exchange": leg.exchange
                })
        elif metadata_legs:
            source_legs = metadata_legs

        if not source_legs:
            return []

        for l in source_legs:
            l_conid = l.get("conId")
            l_display_sym = l.get("symbol")
            l_exp = l.get("expiry") or l.get("lastTradeDateOrContractMonth")
            
            details = None
            if l_conid:
                details = self.connector.symbol_cache.get(l_conid)
            
            if not isinstance(details, dict) and l_display_sym and l_exp:
                for cid, cache_details in self.connector.symbol_cache.items():
                    if not isinstance(cache_details, dict): continue
                    if cache_details.get('symbol') == l_display_sym or \
                       (cache_details.get('localSymbol') and cache_details.get('localSymbol').replace(" ", "") == l_display_sym):
                        if cache_details.get('expiry', "").startswith(l_exp):
                            details = cache_details
                            l_conid = cid
                            break

            res_leg = {
                "ratio": clean_float(l.get("ratio", 1.0)),
                "action": l.get("action"),
                "exchange": l.get("exchange", "CME"),
                "conId": l_conid,
                "symbol": l_display_sym,
                "lastTradeDateOrContractMonth": l_exp,
                "gConId": l.get("gConId") or l.get("g_con_id"),
                "multiplier": 1.0
            }

            if isinstance(details, dict):
                temp_c = create_contract(details)
                res_leg["symbol"] = self.connector.contract_service.get_readable_contract_name(temp_c)
                res_leg["lastTradeDateOrContractMonth"] = temp_c.lastTradeDateOrContractMonth
                res_leg["conId"] = l_conid
                if not res_leg["gConId"]:
                    res_leg["gConId"] = self.connector.contract_service.get_g_con_id(temp_c)
            elif not res_leg["symbol"] and l_conid:
                 res_leg["symbol"] = f"Unknown:{l_conid}"
                 
            if isinstance(details, dict):
                 res_leg["multiplier"] = clean_float(details.get("multiplier", 1.0))
            
            leg_prices = self.connector.db_client.latest_prices.get(res_leg["gConId"], {})
            res_leg["bid"] = clean_float(leg_prices.get("BID"))
            res_leg["ask"] = clean_float(leg_prices.get("ASK"))
            res_leg["last"] = clean_float(leg_prices.get("LAST"))
            
            resolved_legs.append(res_leg)

        return resolved_legs

    def update_dependent_bags(self, trigger_g_con_id: str):
        """
        Updates synthetic prices for any BAG that contains the trigger_g_con_id as a leg.
        """
        for g_con_id, data in self.connector.active_subscriptions.items():
            contract = data.get('contract')
            if contract and contract.secType == "BAG":
                if g_con_id == trigger_g_con_id:
                    self.recalculate_bag_price(g_con_id, data)
                    continue

                legs = data.get('legs') 
                if isinstance(legs, list) and any(l and l.get('g_con_id') == trigger_g_con_id for l in legs):
                    self.recalculate_bag_price(g_con_id, data)
                    continue
                
                if not isinstance(legs, list) and hasattr(contract, 'comboLegs') and contract.comboLegs:
                    for leg in contract.comboLegs:
                        details = self.connector.symbol_cache.get(leg.conId)
                        if isinstance(details, dict):
                            leg_c = create_contract(details)
                            leg_g_con_id = self.connector.contract_service.get_g_con_id(leg_c)
                            if leg_g_con_id == trigger_g_con_id:
                                self.recalculate_bag_price(g_con_id, data)
                                break

    def update_dependent_bags_by_conid(self, con_id: int):
        """
        Checks which BAGs depend on this conId and attempts recalculation.
        """
        for g_con_id, data in self.connector.active_subscriptions.items():
            contract = data.get('contract')
            if contract and contract.secType == "BAG" and hasattr(contract, 'comboLegs') and contract.comboLegs:
                if any(leg.conId == con_id for leg in contract.comboLegs):
                    self.logger.info(f"Leg details resolved for conId {con_id}. Triggering BAG {g_con_id} recalculation.")
                    self.recalculate_bag_price(g_con_id, data)

    def recalculate_bag_price(self, g_con_id: str, data: Dict):
        """
        Internal helper to perform the synthetic calculation and update cache.
        """
        legs = data.get('legs')
        contract = data.get('contract')
        
        if not isinstance(legs, list) and contract and contract.comboLegs:
            legs_list = []
            for leg in contract.comboLegs:
                details = self.connector.symbol_cache.get(leg.conId)
                if isinstance(details, dict):
                    leg_c = create_contract(details)
                    legs_list.append({
                        "g_con_id": self.connector.contract_service.get_g_con_id(leg_c),
                        "conId": leg.conId,
                        "ratio": leg.ratio,
                        "action": leg.action,
                        "symbol": self.connector.contract_service.get_readable_contract_name(leg_c),
                        "expiry": details.get('expiry')
                    })
                else:
                    self.connector.contract_service.resolve_contract_by_conid(leg.conId)
                    return 
            data['legs'] = legs_list
            legs = legs_list
        
        if not isinstance(legs, list) or not legs: return
        
        pricing = self.connector.db_client.get_realtime_synthetic_price(legs)
        clean_pricing = {
            "BID": pricing.get('bid'),
            "ASK": pricing.get('ask'),
            "LAST": pricing.get('last')
        }
        
        if any(v is not None and v != 0 for v in clean_pricing.values()):
            self.connector.db_client.latest_prices[g_con_id] = clean_pricing
            
            # Broadcast Synthetic Update to WebSockets
            if self.connector.ws_manager and self.connector.main_loop:
                import asyncio
                for tick_type, val in clean_pricing.items():
                    if val is not None:
                        msg = {
                            "gConId": g_con_id,
                            "symbol": self.connector.active_subscriptions.get(g_con_id, {}).get('symbol', 'Unknown'),
                            "tickType": tick_type,
                            "price": val,
                            "timestamp": int(time.time() * 1000)
                        }
                        asyncio.run_coroutine_threadsafe(
                            self.connector.ws_manager.broadcast(f"market:{g_con_id}", msg),
                            self.connector.main_loop
                        )

    def unsubscribe_contract(self, g_con_id: str):
        """
        Cancels market data subscription for a contract and removes from registry.
        """
        if g_con_id not in self.connector.active_subscriptions:
            return

        sub = self.connector.active_subscriptions[g_con_id]
        req_id = sub.get('req_id')
        contract = sub.get('contract')
        
        self.logger.info(f"Unsubscribing from {g_con_id} (ReqId: {req_id})")
        
        if req_id is not None:
            self.connector.client.cancelMktData(req_id)
            with self.connector.lock:
                if req_id in self.connector.req_id_to_g_con_id:
                    del self.connector.req_id_to_g_con_id[req_id]
                if req_id in self.connector.req_id_to_symbol:
                    del self.connector.req_id_to_symbol[req_id]

        with self.connector.lock:
            del self.connector.active_subscriptions[g_con_id]
        
        # Cleanup legs for BAGs
        if contract and contract.secType == "BAG" and contract.comboLegs:
            for leg in contract.comboLegs:
                leg_g_con_id = self._find_g_con_id_by_con_id(leg.conId)
                if leg_g_con_id and leg_g_con_id in self.connector.active_subscriptions:
                    used_by_bag = self._is_leg_used_by_active_bags(leg.conId)
                    leg_sub_contract = self.connector.active_subscriptions[leg_g_con_id].get('contract')
                    in_watchlist = self._is_contract_in_watchlist(leg_sub_contract)
                    
                    if not used_by_bag and not in_watchlist:
                        self.logger.info(f"Leg {leg.conId} ({leg_g_con_id}) is no longer needed. Unsubscribing.")
                        self.unsubscribe_contract(leg_g_con_id)

    def get_sync_contract_data(self, symbol_str: str, timeout: float = 5.0) -> Dict[str, Any]:
        """
        Synchronously fetches contract data (conId and ticks).
        """
        self.logger.info(f"Syncing data for {symbol_str} (timeout={timeout}s)...")
        from utils import parse_contract_symbol
        pre_existing_cache_keys = set(self.connector.symbol_cache.keys())
        
        legs = parse_contract_symbol(symbol_str)
        contract_data = {"symbol": symbol_str, "legs": legs}
        if len(legs) > 1:
            contract_data["secType"] = "BAG"
            
        temp_contract = create_contract(contract_data) 
        if len(legs) > 1:
            temp_contract._metadata_legs = legs
            
        g_con_id = self.connector.contract_service.get_g_con_id(temp_contract)
        
        is_bag = (len(legs) > 1)
        cached_price = None
        
        if is_bag:
            if g_con_id in self.connector.active_subscriptions:
                reg_legs = self.connector.active_subscriptions[g_con_id].get('legs')
                if reg_legs:
                    cached_price = self.connector.db_client.get_realtime_synthetic_price(reg_legs)
        else:
            cached_price = self.connector.db_client.latest_prices.get(g_con_id)
            
        has_data = False
        if cached_price:
            if is_bag:
                if any(v is not None for v in cached_price.values()): has_data = True
            else:
                if any(v > 0 for v in cached_price.values() if isinstance(v, (int, float))): has_data = True
                
        if has_data:
            self.logger.info(f"Sync Request: Cache Hit for {symbol_str}. Returning existing data.")
            return {
                "conId": temp_contract.conId, 
                "gConId": g_con_id,
                "ticks": cached_price
            }
            
        self.logger.info(f"Sync Request: Cache Miss for {symbol_str}. Requesting subscription...")
        was_already_subscribed = (g_con_id in self.connector.active_subscriptions)
        self.subscribe_contract(contract_data)
        
        start_time = time.time()
        final_price = None
        
        while (time.time() - start_time) < timeout:
            time.sleep(0.2)
            if is_bag:
                if g_con_id in self.connector.active_subscriptions:
                     reg_legs = self.connector.active_subscriptions[g_con_id].get('legs')
                     if reg_legs:
                          self.logger.info(f"Sync Request: Calculating BAG price for {symbol_str}...")
                          p = self.connector.db_client.get_realtime_synthetic_price(reg_legs)
                          if any(v is not None for v in p.values()):
                              final_price = p
                              break
            else:
                p = self.connector.db_client.latest_prices.get(g_con_id)
                if p and any(v > 0 for v in p.values() if isinstance(v, (int, float))):
                    self.logger.info(f"Sync Request: Ticks received for {symbol_str}: {p}")
                    final_price = p
                    break
        
        if final_price:
            self.logger.info(f"Sync Request: Data received for {symbol_str} after {(time.time() - start_time):.2f}s")
            final_price = p # Added to be explicit though p is already set if break happened correctly
        
        if not was_already_subscribed:
            self.logger.info(f"Sync Request: Cleaning up stateless subscription for {symbol_str}")
            created_con_ids = []
            if g_con_id in self.connector.active_subscriptions:
                c_obj = self.connector.active_subscriptions[g_con_id].get('contract')
                if c_obj:
                    if hasattr(c_obj, 'conId') and c_obj.conId:
                        created_con_ids.append(c_obj.conId)
                    if c_obj.comboLegs:
                        for cl in c_obj.comboLegs:
                            created_con_ids.append(cl.conId)

            self.unsubscribe_contract(g_con_id)
            
            if g_con_id in self.connector.db_client.latest_prices:
                del self.connector.db_client.latest_prices[g_con_id]
            if g_con_id in self.connector.db_client.latest_sizes:
                del self.connector.db_client.latest_sizes[g_con_id]
                
            if created_con_ids:
                for cid in created_con_ids:
                    if cid not in pre_existing_cache_keys and cid in self.connector.symbol_cache:
                        self.logger.info(f"Cleaning up resolved cache for conId: {cid}")
                        del self.connector.symbol_cache[cid]
            
        if final_price:
            return {"conId": temp_contract.conId, "gConId": g_con_id, "ticks": final_price}
        else:
            self.logger.warning(f"Sync Request: TIMEOUT for {symbol_str} after {timeout}s.")
            return {"conId": temp_contract.conId, "gConId": g_con_id, "ticks": None, "error": "Timeout waiting for market data"}

    def _find_g_con_id_by_con_id(self, con_id: int) -> Optional[str]:
        for gid, sub in self.connector.active_subscriptions.items():
            c = sub.get('contract')
            if c and c.conId == con_id: return gid
        return None

    def _is_leg_used_by_active_bags(self, leg_con_id: int) -> bool:
        for gid, sub in self.connector.active_subscriptions.items():
            c = sub.get('contract')
            if c and c.secType == "BAG" and c.comboLegs:
                if any(cl.conId == leg_con_id for cl in c.comboLegs): return True
        return False

    def _is_contract_in_watchlist(self, contract: Contract) -> bool:
        if not contract: return False
        watchlist = self.connector.watchlist_manager.get_contracts()
        if contract.symbol in watchlist: return True
        if contract.localSymbol in watchlist: return True
        return False
    def subscribe_watchlist(self):
        """
        Iterates through the Watchlist, parses symbols, and requests market data.
        Note: BAGs will wait for their legs to be resolved in symbol_cache.
        """
        symbols = self.connector.watchlist_manager.get_contracts()
        for sym_str in symbols:
            legs = parse_contract_symbol(sym_str)
            if len(legs) == 1:
                # Single leg, subscribe directly
                self.subscribe_contract(legs[0])
            else:
                # Spread, wait for resolve logic to trigger subscription
                # or try to subscribe if cache is already hit (rare at startup)
                self.subscribe_contract({"symbol": sym_str, "secType": "BAG", "legs": legs})

    def initialize_pricing_from_db(self):
        """
        Loads the last known BID/ASK/LAST from InfluxDB for all currently registered single legs.
        """
        self.logger.info("Initializing pricing cache from InfluxDB...")
        for g_con_id, data in self.connector.active_subscriptions.items():
            contract = data.get('contract')
            if contract and contract.secType != "BAG":
                symbol_name = self.connector.contract_service.get_readable_contract_name(contract)
                last_record = self.connector.db_client.get_last_price_record(symbol_name)
                
                if any(v > 0 for v in last_record.values()):
                    self.connector.db_client.latest_prices[g_con_id] = last_record
                    self.logger.debug(f"Loaded pricing for {symbol_name} (ID: {g_con_id}): {last_record}")
        
        for g_con_id, data in self.connector.active_subscriptions.items():
            contract = data.get('contract')
            if contract and contract.secType == "BAG":
                self.update_dependent_bags(g_con_id)
