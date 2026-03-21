import time
from typing import Dict, Any, List, Optional
from .base_service import IBBaseService
from utils import map_tick_type

class TickDataBroadcaster(IBBaseService):
    def __init__(self, connector: 'IBConnector'):
        super().__init__(connector)
        # Tick Buffer for Throttling
        self.tick_buffer: Dict[str, Dict] = {}
        
    def handle_tick_price(self, reqId: int, tickType: int, price: float):
        """
        Handles price ticks received from the Wrapper.
        """
        if price <= 0: return
        
        # Skip if this ID is flagged as NoPersist (e.g. BAG)
        if reqId in self.connector.non_persisted_req_ids:
            return

        tick_name = map_tick_type(tickType)
        if tick_name == "UNKNOWN":
            return
            
        g_con_id = self.connector.req_id_to_g_con_id.get(reqId)
        if g_con_id:
            symbol = self.connector.req_id_to_symbol.get(reqId, "Unknown")
            
            # Update internal price cache/database
            self.connector.db_client.update_tick_data(g_con_id, tick_name, price, symbol)

            # Buffer for Throttled WebSocket Broadcast
            with self.connector.lock:
                if g_con_id not in self.tick_buffer:
                    self.tick_buffer[g_con_id] = {}
                
                # Store latest price for this tick type
                self.tick_buffer[g_con_id][tick_name] = price
                # Also store symbol for context
                self.tick_buffer[g_con_id]['symbol'] = symbol
                # And timestamp
                self.tick_buffer[g_con_id]['timestamp'] = int(time.time() * 1000)
            
            # Update any dependent BAGs reactively (Synthetic Pricing)
            self.connector.market_data_service.update_dependent_bags(g_con_id)

    def handle_tick_size(self, reqId: int, tickType: int, size: int):
        """
        Handles size ticks (0=BidSize, 3=AskSize, 5=LastSize, 8=Volume).
        """
        if size < 0: return

        # Skip if this ID is flagged as NoPersist (e.g. BAG)
        if reqId in self.connector.non_persisted_req_ids:
            return

        tick_name = map_tick_type(tickType)
        if tick_name == "UNKNOWN":
            return
            
        g_con_id = self.connector.req_id_to_g_con_id.get(reqId)
        if g_con_id:
            symbol = self.connector.req_id_to_symbol.get(reqId, "Unknown")
            self.connector.db_client.update_tick_data(g_con_id, tick_name, float(size), symbol)

    def flush_ticks_loop(self):
        """
        Background loop to flush buffered ticks to WebSocket every 30 seconds.
        """
        while self.connector.started:
            time.sleep(30)
            
            ticks_to_send = {}
            with self.connector.lock:
                if not self.tick_buffer:
                    continue
                ticks_to_send = self.tick_buffer
                self.tick_buffer = {}
            
            # Broadcast buffered ticks
            if self.connector.ws_manager and self.connector.main_loop:
                import asyncio
                try:
                    count = 0
                    for g_con_id, data in ticks_to_send.items():
                        symbol = data.pop('symbol', 'Unknown')
                        timestamp = data.pop('timestamp', int(time.time() * 1000))
                        
                        for tick_type, price in data.items():
                            msg = {
                                "gConId": g_con_id,
                                "symbol": symbol,
                                "tickType": tick_type,
                                "price": price,
                                "timestamp": timestamp
                            }
                            asyncio.run_coroutine_threadsafe(
                                 self.connector.ws_manager.broadcast(f"market:{g_con_id}", msg),
                                 self.connector.main_loop
                            )
                            count += 1
                    
                    if count > 0:
                        self.logger.info(f"Flushed {count} tick updates to WebSocket.")
                        
                except Exception as e:
                    self.logger.error(f"Error flushing ticks: {e}")
