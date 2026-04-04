from typing import Dict, Any, List, Optional
import threading
import time
from ibapi.contract import Contract, ComboLeg
from .base_service import IBBaseService
from utils import parse_contract_symbol, get_short_symbol, generate_g_con_id

class ContractResolutionService(IBBaseService):
    def __init__(self, connector: 'IBConnector'):
        super().__init__(connector)
        # Cache for Product Symbol -> Valid Exchange (e.g., 'HE' -> 'CME')
        # This allows instant resolution for all contracts of the same product.
        self.product_exchange_cache: Dict[str, str] = {}
        
    def resolve_product_exchange(self, contract: Contract) -> Optional[str]:
        """
        Attempts to resolve the exchange for a contract using local caches.
        Returns the exchange string if found, otherwise None.
        """
        from utils import parse_single_leg_details
        
        # 1. Check if already present in contract
        if contract.exchange:
            return contract.exchange
            
        # 2. Check per-instrument cache (if conId exists)
        if contract.conId and contract.conId in self.connector.symbol_cache:
            details = self.connector.symbol_cache[contract.conId]
            if isinstance(details, dict) and details.get('exchange'):
                return details['exchange']
                
        # 3. Check product-level cache
        sym_to_parse = contract.localSymbol if contract.localSymbol else contract.symbol
        details = parse_single_leg_details(sym_to_parse)
        product = details.get('product')
        
        if product and product in self.product_exchange_cache:
            return self.product_exchange_cache[product]
            
        return None

    def get_g_con_id(self, contract: Contract) -> str:
        """
        Helper to generate a consistent gConId for any contract.
        If the contract object already has a stable ID attached, we return it.
        """
        if hasattr(contract, '_g_con_id'):
            return contract._g_con_id
            
        if contract.secType == "BAG":
            # 1. Generate a stable conId-based signature for absolute identity
            con_sig = None
            if hasattr(contract, 'comboLegs') and contract.comboLegs:
                leg_ids = sorted([f"{l.conId}:{l.ratio}:{l.action}" for l in contract.comboLegs])
                con_sig = "|".join(leg_ids)
                
            # 2. Check Registry for an existing equivalent contract
            # Identity is defined by the comboLegs signature
            if con_sig:
                for gid, sub in self.connector.active_subscriptions.items():
                    sub_c = sub.get('contract')
                    if sub_c and sub_c.secType == "BAG" and sub_c.comboLegs:
                        sub_sig = "|".join(sorted([f"{cl.conId}:{cl.ratio}:{cl.action}" for cl in sub_c.comboLegs]))
                        if sub_sig == con_sig:
                            # Found an existing entry!
                            # Let's see if we can improve its name now
                            nice_name = self.get_readable_contract_name(contract)
                            if nice_name and nice_name != "BAG:Unknown" and gid.startswith("BAG:"):
                                # If existing entry is a placeholder and we have a real name, RENAME it
                                self.connector._rename_subscription(gid, nice_name)
                                return nice_name
                            return gid

            # 3. If NOT in registry, try to get a nice name for the NEW entry
            nice_name = self.get_readable_contract_name(contract)
            if nice_name and nice_name != "BAG:Unknown":
                return nice_name
                
            # 4. Fallback to symbol string ONLY if it's clearly a spread string (contains - or +)
            if contract.symbol and contract.symbol != "BAG" and any(char in contract.symbol for char in "-+"):
                return contract.symbol
                
            # 5. Final fallback: Use the signature as a placeholder ID
            if con_sig:
                return f"BAG:{con_sig}"
                
            return generate_g_con_id("", "", "", "BAG", contract.symbol or "UnknownBAG")
        
        # For single legs, we need product, month, year
        from utils import parse_single_leg_details
        sym_to_parse = contract.localSymbol if contract.localSymbol else contract.symbol
        details = parse_single_leg_details(sym_to_parse)
        
        product = details.get('product', '')
        month = details.get('month', '')
        year = details.get('year', '')
        
        if not month or not year:
            expiry = contract.lastTradeDateOrContractMonth
            if expiry and len(expiry) >= 6:
                year = expiry[:4]
                month_num = expiry[4:6]
                if not month:
                    month = month_num

        return generate_g_con_id(
            product, 
            month, 
            year, 
            contract.secType
        )

    def get_readable_contract_name(self, contract: Contract) -> str:
        """
        Returns a friendly name for the contract. 
        Resolves BAG legs using cache if needed.
        """
        if contract.secType != "BAG":
             if contract.localSymbol:
                 return contract.localSymbol
             return get_short_symbol(contract.symbol, contract.lastTradeDateOrContractMonth)

        if not contract.comboLegs:
            if contract.symbol and contract.symbol != "BAG":
                if contract.localSymbol and contract.localSymbol != contract.symbol:
                     return contract.localSymbol
                return contract.symbol
            return "BAG:Unknown"

        display_parts = []
        for leg in contract.comboLegs:
             cache_entry = self.connector.symbol_cache.get(leg.conId)
             if not cache_entry:
                  # If we are missing any leg resolution, stop and return empty.
                  # This prevents falling back to a generic symbol like "HE".
                  return ""
             
             leg_name = f"wid:{leg.conId}"
             if isinstance(cache_entry, dict):
                 leg_name = cache_entry.get('localSymbol') or cache_entry.get('symbol', leg_name)
             elif isinstance(cache_entry, str):
                 leg_name = cache_entry
             
             part = ""
             if leg.action == "BUY":
                  part = f"+{leg.ratio}{leg_name}" if leg.ratio > 1 else f"+{leg_name}"
             else:
                  part = f"-{leg.ratio}{leg_name}" if leg.ratio > 1 else f"-{leg_name}"
             display_parts.append(part)

        full_str = "".join(display_parts)
        if full_str.startswith("+"):
            full_str = full_str[1:]
            
        return full_str

    def resolve_contract_details_sync(self, contract: Contract, timeout: float = 5.0) -> bool:
        """
        Synchronously requests contract details and waits for the result.
        Populates the input contract object with resolved metadata if successful.
        """
        if contract.conId and contract.exchange and contract.multiplier:
            return True # Already has basic metadata
            
        req_id = self.connector.get_req_id()
        event = threading.Event()
        self.connector.resolution_events[req_id] = event
        
        # Log exactly what we are sending for debugging
        self.logger.info(f"Sync Resolution {req_id}: Sending request for Symbol='{contract.symbol}', Expiry='{contract.lastTradeDateOrContractMonth}', SecType='{contract.secType}', Exchange='{contract.exchange}'")
        self.connector.client.reqContractDetails(req_id, contract)
        
        start_wait = time.time()
        was_notified = event.wait(timeout=timeout)
        wait_duration = time.time() - start_wait
        
        if was_notified:
            # Check wrapper for the resolved details
            details = self.connector.wrapper.contract_details.get(req_id)
            if details:
                # Update the original contract object with resolved details
                contract.conId = details.contract.conId
                contract.exchange = details.contract.exchange
                contract.multiplier = details.contract.multiplier
                contract.lastTradeDateOrContractMonth = details.contract.lastTradeDateOrContractMonth
                contract.symbol = details.contract.symbol
                contract.localSymbol = details.contract.localSymbol
                self.logger.info(f"Sync Resolution {req_id}: Success after {wait_duration:.2f}s for '{contract.symbol}' -> ConId:{contract.conId}")
                return True
            else:
                # If event was set but no details found, it's likely an error that was handled in on_error
                error_msg = self.connector.wrapper.last_errors.get(req_id, "Unknown Connection Error")
                self.logger.warning(f"Sync Resolution {req_id}: FAILED after {wait_duration:.2f}s for '{contract.symbol}': {error_msg}")
                return False
        
        self.logger.warning(f"Sync Resolution {req_id}: TIMEOUT after {wait_duration:.2f}s (no response from IB) for '{contract.symbol}'")
        return False

    def resolve_all_contracts_sync(self, product: str, sec_type: str = "FUT", timeout: float = 10.0) -> List[Dict]:
        """
        Broadly requests all contracts for a product code and waits for all results.
        Returns a list of simplified contract dictionaries.
        """
        contract = Contract()
        contract.symbol = product
        contract.secType = sec_type
        contract.currency = "USD" # Default, usually fine for futures
        
        # Try to resolve exchange to narrow search if possible, else empty for broad search
        contract.exchange = self.resolve_product_exchange(contract) or ""
        
        req_id = self.connector.get_req_id()
        event = threading.Event()
        self.connector.resolution_events[req_id] = event
        
        # Ensure we have a fresh list in bulk_contract_details
        self.connector.wrapper.bulk_contract_details[req_id] = []
        
        self.logger.info(f"Sync Discovery {req_id}: Requesting all '{sec_type}' for product '{product}' (Exchange: '{contract.exchange}')")
        self.connector.client.reqContractDetails(req_id, contract)
        
        start_wait = time.time()
        was_notified = event.wait(timeout=timeout)
        wait_duration = time.time() - start_wait
        
        # Retrieve the accumulated results
        results = self.connector.wrapper.bulk_contract_details.pop(req_id, [])
        
        final_list = []
        for details in results:
            c = details.contract
            final_list.append({
                "conId": c.conId,
                "symbol": c.symbol,
                "localSymbol": c.localSymbol,
                "expiry": c.lastTradeDateOrContractMonth,
                "multiplier": c.multiplier,
                "exchange": c.exchange,
                "currency": c.currency,
                "longName": details.longName,
                "marketName": details.marketName
            })
            
        if was_notified:
            self.logger.info(f"Sync Discovery {req_id}: Found {len(final_list)} contracts in {wait_duration:.2f}s")
        else:
            self.logger.warning(f"Sync Discovery {req_id}: TIMEOUT after {wait_duration:.2f}s. Returning partial list of {len(final_list)} contracts.")
            
        return final_list

    def resolve_contract_by_conid(self, con_id: int):
        """
        Requests contract details for a specific conId to populate cache.
        """
        if con_id in self.connector.symbol_cache:
            return 
            
        c = Contract()
        c.conId = con_id
        
        sig = f"conId:{con_id}"
        if sig in self.connector.in_flight_signatures:
            return 
            
        self.connector.in_flight_signatures.add(sig)
        
        req_id = self.connector.get_req_id()
        
        self.connector.pending_resolutions[req_id] = sig
        
        self.logger.info(f"Triggering resolution for conId: {con_id} (ReqId: {req_id})")
        self.connector.client.reqContractDetails(req_id, c)

    def resolve_watchlist_symbols(self, symbols: List[str]):
        """
        Parses watchlist symbols, extracts all unique single legs,
        and requests contract details to populate symbol_cache.
        """
        unique_legs = set()
        
        for sym_str in symbols:
            legs_parsed = parse_contract_symbol(sym_str)
            for leg in legs_parsed:
                # Try to resolve exchange from cache
                exc = leg.get('exchange')
                if not exc:
                    # Create a temporary contract to check the cache
                    temp_c = Contract()
                    temp_c.symbol = leg['symbol']
                    temp_c.secType = leg.get('secType', 'FUT')
                    exc = self.resolve_product_exchange(temp_c) or ""
                
                unique_legs.add((
                    leg.get('product', ''),
                    leg.get('expiry', ''),
                    leg['symbol'],
                    leg.get('secType', 'FUT'),
                    exc,
                    leg.get('currency', 'USD')
                ))
                    
        self.logger.info(f"Resolving {len(unique_legs)} unique legs for Cache...")
        
        for prod, exp, full_sym, sec, exc, cur in unique_legs:
            c = Contract()
            
            if prod and exp:
                c.symbol = prod
                c.lastTradeDateOrContractMonth = exp
                if full_sym and full_sym[-1].isdigit():
                    c.localSymbol = full_sym
            elif full_sym and full_sym[-1].isdigit():
                c.localSymbol = full_sym
                c.symbol = "" 
            else:
                c.symbol = full_sym
            
            c.secType = sec
            c.exchange = exc
            c.currency = cur
            
            sig = f"{c.symbol}|{c.localSymbol}|{c.lastTradeDateOrContractMonth}|{c.secType}|{c.exchange}|{c.currency}"
            if sig in self.connector.in_flight_signatures:
                continue
            self.connector.in_flight_signatures.add(sig)
            
            req_id = self.connector.get_req_id()
            
            self.connector.pending_resolutions[req_id] = sig
            
            self.logger.info(f"DEBUG: [reqContractDetails] ReqId:{req_id} | Symbol:'{c.symbol}' | Local:'{c.localSymbol}' | Expiry:'{c.lastTradeDateOrContractMonth}' | Sec:'{c.secType}' | Exc:'{c.exchange}' | Cur:'{c.currency}'")
            self.connector.client.reqContractDetails(req_id, c)

    def resolve_bag_contract(self, contract: Contract):
        """
        If the contract symbol suggests a multileg (BAG), we resolve its legs
        and populate the comboLegs list using the internal symbol_cache.
        If legs are missing from cache, we request them on-the-fly and wait.
        """
        legs_parsed = parse_contract_symbol(contract.symbol)
        
        if len(legs_parsed) <= 1:
            return 
            
        self.logger.info(f"Resolving BAG legs for symbol: {contract.symbol}")
        
        combo_legs = []
        all_resolved = True
        
        for l in legs_parsed:
            l_conid = None
            l_name = l.get('symbol')
            l_expiry = l.get('expiry')
            
            for cid, details in self.connector.symbol_cache.items():
                if isinstance(details, dict) and (details.get('symbol') == l_name or details.get('localSymbol') == l_name):
                    cache_expiry = details.get('expiry', "")
                    if cache_expiry.startswith(l_expiry):
                        l_conid = cid
                        break
            
            if not l_conid:
                self.logger.info(f"Leg {l_name} {l_expiry} not in cache. Requesting on-the-fly...")
                
                lc = Contract()
                prod = l.get('product')
                if prod and l_expiry:
                    lc.symbol = prod
                    lc.lastTradeDateOrContractMonth = l_expiry
                else:
                    lc.symbol = l_name 
                
                lc.secType = l.get('secType', 'FUT')
                lc.exchange = l.get('exchange', self.resolve_product_exchange(lc) or "")
                lc.currency = l.get('currency', 'USD')
                
                req_id = self.connector.get_req_id()
                
                event = threading.Event()
                self.connector.resolution_events[req_id] = event
                
                self.connector.client.reqContractDetails(req_id, lc)
                
                if event.wait(timeout=10.0):
                    self.logger.info(f"Leg {l_name} resolved successfully.")
                    for cid, details in self.connector.symbol_cache.items():
                        if isinstance(details, dict) and (details.get('symbol') == l_name or details.get('localSymbol') == l_name):
                            cache_expiry = details.get('expiry', "")
                            if cache_expiry.startswith(l_expiry):
                                l_conid = cid
                                break
                else:
                    self.logger.warning(f"Timeout waiting for leg resolution: {l_name}")

            if l_conid:
                clem = ComboLeg()
                clem.conId = l_conid
                clem.ratio = l.get('ratio', 1)
                clem.action = l.get('action', 'BUY')
                
                # Check cache for leg exchange
                temp_lc = Contract()
                temp_lc.conId = l_conid
                temp_lc.symbol = l_name
                clem.exchange = l.get('exchange', self.resolve_product_exchange(temp_lc) or "")
                combo_legs.append(clem)
            else:
                self.logger.warning(f"Could not resolve conId for leg: {l_name} {l_expiry}")
                all_resolved = False
                break
                
        if all_resolved:
            contract.secType = "BAG"
            contract.comboLegs = combo_legs
            
            if legs_parsed:
                contract.symbol = legs_parsed[0].get("product")
            
            # Resolve BAG exchange
            contract.exchange = self.resolve_product_exchange(contract) or "SMART"
            self.logger.info(f"Successfully resolved BAG: {contract.symbol} with {len(combo_legs)} legs.")
        else:
            self.logger.error(f"Failed to fully resolve BAG legs for {contract.symbol}. Order might fail if IB cannot resolve it.")

    def on_contract_details_end(self, req_id: int, is_started: bool):
        """
        Called from IBConnector when details request finishes.
        """
        is_sync_resolution = False
        if req_id in self.connector.resolution_events:
            is_sync_resolution = True
            event = self.connector.resolution_events.pop(req_id)
            event.set()

        details = self.connector.wrapper.contract_details.get(req_id)
        if details:
            leg_contract = details.contract
            self.connector.subscribe_contract(leg_contract)

        if req_id in self.connector.pending_resolutions:
            sig = self.connector.pending_resolutions.pop(req_id)
            if sig in self.connector.in_flight_signatures:
                self.connector.in_flight_signatures.remove(sig)
            
        self.connector._check_system_ready()
        
        if not self.connector.pending_resolutions and is_started:
            self.connector._check_pending_bags()
            if not self.connector.is_ready:
                self.logger.info("Watchlist symbols resolved. System READY.")
                self.connector.is_ready = True
                
    def on_error(self, req_id: int, error_code: int, error_string: str, is_started: bool):
        """
        Handles resolution errors.
        """
        if req_id in self.connector.resolution_events:
            event = self.connector.resolution_events.pop(req_id)
            event.set()

        if req_id in self.connector.pending_resolutions:
            self.logger.warning(f"Resolution failed for ReqId {req_id} (Code: {error_code}, Msg: {error_string}). Removing from pending queue.")
            sig = self.connector.pending_resolutions.pop(req_id)
            if sig in self.connector.in_flight_signatures:
                self.connector.in_flight_signatures.remove(sig)
            
            self.connector._check_system_ready()
            if not self.connector.pending_resolutions and is_started:
                self.connector._check_pending_bags()
                if not self.connector.is_ready:
                    self.logger.info("Watchlist symbols resolved (with errors). System READY.")
                    self.connector.is_ready = True
