from datetime import datetime, timezone, time, timedelta
import threading
import time as time_module
try:
    from zoneinfo import ZoneInfo
except ImportError:
    # Fallback for older python if needed, though 3.9+ has it.
    from dateutil.tz import gettz as ZoneInfo 

from typing import List, Dict, Optional, Set, Tuple
import influxdb_client
from influxdb_client.client.write_api import SYNCHRONOUS, Point
import pandas as pd
from config import Config
from logger import LoggerSetup

from models import AccumulatedTickRecord, AccountSummary

class DatabaseClient:
    """
    Handles interaction with InfluxDB v2.
    Implements buffering to reduce HTTP requests.
    """
    
    def __init__(self):
        self.logger = LoggerSetup.get_logger("DatabaseClient")
        
        try:
            self.client = influxdb_client.InfluxDBClient(
                url=Config.INFLUXDB_URL,
                token=Config.INFLUXDB_TOKEN,
                org=Config.INFLUXDB_ORG
            )
            self.write_api = self.client.write_api(write_options=SYNCHRONOUS)
            self.query_api = self.client.query_api()
            self.org = Config.INFLUXDB_ORG
            self.logger.info(f"Initialized InfluxDB Client for Org: {self.org}")
        except Exception as e:
            self.logger.error(f"Failed to initialize InfluxDB Client: {e}")
            self.client = None
            self.write_api = None

        # Buffer for accumulating ticks before write
        # Key: g_con_id, Value: AccumulatedTickRecord
        self.pending_ticks: Dict[str, AccumulatedTickRecord] = {}
        
        # Buffer for ready records to be written in batch
        # List of Points
        self.write_buffer: List[Point] = []
        self.BUFFER_SIZE = 5
        self.chicago_tz = ZoneInfo("America/Chicago")

        # Threading & Flush Monitor
        self.lock = threading.Lock()
        self.monitor_active = False
        self.monitor_thread = None
        self.last_flush_time = datetime.now(timezone.utc)
        self.FLUSH_INTERVAL = 5 # seconds

        # In-memory cache for latest prices (real-time synthetic engine)
        # Key: g_con_id, Value: {'BID': float, 'ASK': float, 'LAST': float}
        self.latest_prices: Dict[str, Dict[str, float]] = {}
        
        # In-memory cache for latest sizes (to support stateful snapshots)
        # Key: g_con_id, Value: {'BID_SIZE': float, 'ASK_SIZE': float, 'LAST_SIZE': float, 'VOLUME': float}
        self.latest_sizes: Dict[str, Dict[str, float]] = {}
        
        # Buffer for pending volume updates (throttled)
        # Key: g_con_id, Value: {'value': float, 'symbol': str}
        self.pending_volumes: Dict[str, Dict[str, Any]] = {}
        
        # Cache for last queued prices to dedup writes
        # Key: g_con_id, Value: (bid, ask, last)
        self.last_queued_prices: Dict[str, Tuple[float, float, float]] = {}

        # Cache for last written order status to dedup writes
        # Key: orderId, Value: Dict of written fields
        self.last_written_orders: Dict[int, Dict[str, Any]] = {}

    def start_monitor(self):
        """Starts the background flush monitor."""
        if self.monitor_active: return
        
        self.monitor_active = True
        self.monitor_thread = threading.Thread(target=self._monitor_flush_loop, daemon=True)
        self.monitor_thread.start()
        self.logger.info("Database Flush Monitor Started.")

    def stop_monitor(self):
        """Stops the background flush monitor."""
        self.monitor_active = False
        if self.monitor_thread and self.monitor_thread.is_alive():
            self.monitor_thread.join(timeout=2.0)
        self.logger.info("Database Flush Monitor Stopped.")

    def get_latest_tick(self, g_con_id: str) -> Dict[str, float]:
        """
        Returns the latest known price for a contract from memory cache.
        """
        with self.lock:
            # Check memory cache first (Real-time)
            prices = self.latest_prices.get(g_con_id)
            if prices:
                # Prefer LAST, then (BID+ASK)/2, then BID, then ASK
                price = prices.get('LAST', 0.0)
                if price <= 0:
                    bid = prices.get('BID', 0.0)
                    ask = prices.get('ASK', 0.0)
                    if bid > 0 and ask > 0:
                         price = (bid + ask) / 2
                    elif bid > 0:
                         price = bid
                    elif ask > 0:
                         price = ask
                
                return {"price": price}
            
            # If not in memory, we could query Influx, but for "latest tick" speed is key.
            # We return 0 so UI just waits for next tick.
            return {"price": 0.0}

    def _monitor_flush_loop(self):
        """
        Background loop that checks for stale data in buffer.
        """
        while self.monitor_active:
            time_module.sleep(1) # Check every second
                        
            try:
                should_flush = False
                with self.lock:
                    if self.write_buffer or self.pending_volumes:
                        elapsed = datetime.now(timezone.utc) - self.last_flush_time
                        if elapsed.total_seconds() > self.FLUSH_INTERVAL:
                            should_flush = True
                
                if should_flush:
                    self.logger.debug("Time-based flush triggered.")
                    # Flush utilizes the lock internally
                    with self.lock:
                        self._flush_buffer_internal(Config.INFLUXDB_BUCKET_PRICES)
            except Exception as e:
                self.logger.error(f"Error in flush monitor: {e}")

    def _get_chicago_eod_time(self, current_utc_time: datetime) -> datetime:
        """
        Converts a UTC time to Chicago Time, sets it to end of day (23:59:59),
        and converts back to UTC.
        """
        # Convert UTC to Chicago
        chicago_time = current_utc_time.astimezone(self.chicago_tz)
        
        # Set to end of day: 23:59:59
        # Note: 23:59:59.999999 ensures it's the very last moment.
        # But user implementation requested 23:59:59.
        eod_chicago = chicago_time.replace(hour=23, minute=59, second=59, microsecond=0)
        
        # Convert back to UTC for InfluxDB
        eod_utc = eod_chicago.astimezone(timezone.utc)
        return eod_utc


    def update_tick_data(self, g_con_id: str, tick_type: str, value: float, symbol_name: str = ""):
        """
        Updates the pending tick record for a contract. 
        Uses State-Fill logic: If we have cached values for missing fields, we use them.
        This ensures that we generate complete snapshots even if IB sends partial updates.
        """
        # 1. Update State Caches immediately
        if tick_type in ["BID", "ASK", "LAST"]:
            if g_con_id not in self.latest_prices:
                self.latest_prices[g_con_id] = {}
            self.latest_prices[g_con_id][tick_type.upper()] = value
        elif tick_type == "VOLUME":
             # Special handling for VOLUME: Cache and throttle
             if g_con_id not in self.latest_sizes:
                 self.latest_sizes[g_con_id] = {}
             self.latest_sizes[g_con_id]["VOLUME"] = value
             
             # Store in pending_volumes to be flushed periodically
             # We only keep the LATEST value seen
             self.pending_volumes[g_con_id] = {
                 "value": value,
                 "symbol": symbol_name
             }
             return # Skip AccumulatedTickRecord logic for Volume

        elif tick_type in ["BID_SIZE", "ASK_SIZE", "LAST_SIZE"]:
            if g_con_id not in self.latest_sizes:
                self.latest_sizes[g_con_id] = {}
            self.latest_sizes[g_con_id][tick_type.upper()] = value

        # 2. Get or Create Record
        if g_con_id not in self.pending_ticks:
            self.pending_ticks[g_con_id] = AccumulatedTickRecord(
                time=datetime.now(timezone.utc),
                contract_g_con_id=g_con_id
            )
            
            # STRICT ACCUMULATION: Do not pre-fill. Start incomplete and wait for all fields.
        
        record = self.pending_ticks[g_con_id]

        #self.logger.info(f"Updating tick data for {g_con_id}: {tick_type} = {value} - {record}")
        
        # 3. Apply Current Update (Overrides cache with fresh data)
        if tick_type == "BID": record.bid_price = value
        elif tick_type == "ASK": record.ask_price = value
        elif tick_type == "LAST": record.last_price = value
        elif tick_type == "BID_SIZE": record.bid_size = value
        elif tick_type == "ASK_SIZE": record.ask_size = value
        elif tick_type == "LAST_SIZE": record.last_size = value
        elif tick_type == "VOLUME": record.volume = value
            
        # 4. Check completeness (Strict Mode: All 6 fields must be present)
        if record.is_complete():
            self.logger.debug(f"Record is complete: {record}")
            self._queue_price_record(record, symbol_name)
            # Reset record for next accumulation to capture next "frame"
            del self.pending_ticks[g_con_id]

    def _queue_price_record(self, record: AccumulatedTickRecord, symbol_name: str):
        """
        Creates a Point for the complete record and adds to buffer.
        """
        
        # Point for Prices (Measurement: precios)
        point_price = Point("precios") \
            .tag("symbol", symbol_name if symbol_name else record.contract_g_con_id) \
            .field("BID", record.bid_price) \
            .field("ASK", record.ask_price) \
            .field("LAST", record.last_price) \
            .field("BID_SIZE", record.bid_size) \
            .field("ASK_SIZE", record.ask_size) \
            .field("LAST_SIZE", record.last_size) \
            .time(record.time)
            
        # Deduplication: Check if prices changed since last write
        current_prices = (record.bid_price, record.ask_price, record.last_price)
        last_prices = self.last_queued_prices.get(record.contract_g_con_id)
        
        if last_prices == current_prices:
            # self.logger.debug(f"Skipping redundant price record for {symbol_name}: {current_prices}")
            return

        # Update cache
        self.last_queued_prices[record.contract_g_con_id] = current_prices
        
        with self.lock:
            self.write_buffer.append(point_price)
            self._check_buffer_full(Config.INFLUXDB_BUCKET_PRICES)


    def _flush_buffer_internal(self, bucket: str):
        """
        Internal method to write buffer to DB. 
        Must be called with self.lock HELD.
        """
        if not self.write_buffer and not self.pending_volumes:
            return

        try:
            # Process pending volumes into write buffer
            if self.pending_volumes:
                for gid, data in self.pending_volumes.items():
                     val = data["value"]
                     sym = data["symbol"]
                     # Use EOD time so it overwrites any previous entry for today
                     eod_time = self._get_chicago_eod_time(datetime.now(timezone.utc))
                     
                     point_vol = Point("volumen") \
                        .tag("symbol", sym if sym else gid) \
                        .field("volume", val) \
                        .time(eod_time)
                     self.write_buffer.append(point_vol)
                self.pending_volumes = {}

            if self.write_api and self.write_buffer:
                self.logger.debug(f"Flushing {len(self.write_buffer)} records to InfluxDB...")
                self.write_api.write(bucket=bucket, record=self.write_buffer)
                self.write_buffer = [] # Clear buffer
                self.last_flush_time = datetime.now(timezone.utc)
        except Exception as e:
            self.logger.error(f"Failed to flush buffer: {e}")
            self.write_buffer = []  

    def _check_buffer_full(self, bucket: str):
        """
        Checks if buffer is full and flushes if needed.
        Must be called with self.lock HELD.
        """
        self.logger.debug(f"Checking buffer size: {len(self.write_buffer)} (Vol: {len(self.pending_volumes)}) for {bucket}...")
        if len(self.write_buffer) >= self.BUFFER_SIZE:
            self.logger.debug(f"Buffer full, flushing {len(self.write_buffer)} records to InfluxDB...")
            self._flush_buffer_internal(bucket)

    def write_positions(self, positions: List[Dict]):
        """
        Writes Reconciled Positions to InfluxDB.
        """
        # Feature Disabled: User Requested 2026-02-11
        # Reason: Complexity with BAGs/Legs causing data issues.
        # self.logger.info("Skipping portfolio write (Disabled)")
        return

        bucket = Config.DATA_BUCKET # Use the same active data bucket
        # Ideally, we check 'account' to decide bucket, but simplified here.
        measurement = "positions_reconciled"

        
        points = []
        now = datetime.now(timezone.utc)
        
        for pos in positions:
            point = Point(measurement) \
                .tag("symbol", pos.get("symbol")) \
                .tag("type", pos.get("type")) \
                .tag("account", pos.get("account")) \
                .field("quantity", float(pos.get("qty"))) \
                .time(now)
            points.append(point)
            
        try:
            if self.write_api and points:
                self.write_api.write(bucket=bucket, record=points)
                self.logger.info(f"Written {len(points)} reconciled positions.")
        except Exception as e:
            self.logger.error(f"Failed to write positions: {e}")

    def write_order_status(self, order_data: Dict):
        """
        Writes Order Status update to InfluxDB.
        Tags: accountId, symbol, orderId, permId
        Writes only fields present in order_data (excluding tags).
        """
        bucket = Config.DATA_BUCKET
        measurement = "orders"
        now = datetime.now(timezone.utc)
        
        tags = ["accountId", "symbol", "orderId", "permId"]
        
        try:
            point = Point(measurement)
            
            # 1. Add Tags (Required for identity)
            for t in tags:
                val = order_data.get(t)
                if val is not None:
                    point.tag(t, str(val))
            
            # 2. Add Fields (Only those provided in order_data)
            field_types = {
                "orderId": int,
                "totalQuantity": float,
                "lmtPrice": float,
                "auxPrice": float,
                "filled": float,
                "remaining": float,
                "lastFillPrice": float
            }
            
            # Tags are already added above. Only write fields that are not tags
            # to prevent InfluxDB pivot conflicts where a column exists as both tag and field.
            has_fields = False
            for k, v in order_data.items():
                if v is None or k in tags: 
                    continue
                
                # Type casting
                if k in field_types:
                    try:
                        f_val = field_types[k](v)
                        point.field(k, f_val)
                        has_fields = True
                    except (ValueError, TypeError):
                        pass
                else:
                    point.field(k, str(v))
                    has_fields = True
            
            # Deduplication Check
            oid = int(order_data.get("orderId", 0))
            if oid > 0:
                # 1. Normalize current snapshot to match InfluxDB types
                field_types = {
                    "orderId": int,
                    "totalQuantity": float,
                    "lmtPrice": float,
                    "auxPrice": float,
                    "filled": float,
                    "remaining": float,
                    "lastFillPrice": float
                }
                
                normalized_snapshot = {}
                for k, v in order_data.items():
                    if k in ["time", "timestamp"] or v is None:
                        continue
                        
                    if k in field_types:
                        try:
                            normalized_snapshot[k] = field_types[k](v)
                        except (ValueError, TypeError):
                             normalized_snapshot[k] = v
                    else:
                        normalized_snapshot[k] = str(v)
                
                current_snapshot = normalized_snapshot
                
                # 2. Lazy Load: If not in memory, try to fetch from DB
                if oid not in self.last_written_orders:
                    # Attempt to restore state from InfluxDB
                    restored_states = self.get_last_order_states([oid])
                    if oid in restored_states:
                        self.last_written_orders[oid] = restored_states[oid]
                        self.logger.debug(f"Restored state for Order {oid} from InfluxDB")
                
                # 3. Compare with last known state
                last_snapshot = self.last_written_orders.get(oid)
                
                if last_snapshot == current_snapshot:
                    # self.logger.debug(f"Skipping duplicate order write for {oid}")
                    return

                # 4. Update cache and proceed to write
                self.last_written_orders[oid] = current_snapshot

            point.time(now)
            
            if self.write_api:
                self.write_api.write(bucket=bucket, record=point)
                self.logger.info(f"Written partial order update for Order {order_data.get('orderId')}. Fields: {list(order_data.keys())}")
        except Exception as e:
            self.logger.error(f"Failed to write partial order status: {e}")

    def get_last_order_states(self, order_ids: List[int]) -> Dict[int, Dict]:
        """
        Queries InfluxDB for the last known state of specific orderIds.
        """
        if not order_ids:
            return {}
            
        bucket = Config.DATA_BUCKET
        measurement = "orders"
        
        # Convert IDs to strings for tag matching
        id_strs = [f'"{oid}"' for oid in order_ids]
        id_filter = ", ".join(id_strs)
        
        query = f'''
    from(bucket: "{bucket}")
      |> range(start: -7d)
      |> filter(fn: (r) => r["_measurement"] == "{measurement}")
      |> filter(fn: (r) => contains(value: r["orderId"], set: [{id_filter}]))
      |> last()
      |> rename(columns: {{"accountId": "tag_accountId", "symbol": "tag_symbol", "orderId": "tag_orderId", "permId": "tag_permId"}})
      |> pivot(rowKey:["_time"], columnKey:["_field"], valueColumn:"_value")
    '''
        
        states = {}
        try:
            result = self.query_api.query(org=Config.INFLUXDB_ORG, query=query)
            for table in result:
                for r in table.records:
                    val1 = r.values.get("orderId")
                    val2 = r.values.get("tag_orderId")
                    oid_str = val1 if val1 is not None else (val2 if val2 is not None else 0)
                    try:
                        oid = int(oid_str)
                    except (ValueError, TypeError):
                        continue
                    if oid == 0: continue
                
                    # Reconstruct the snapshot from InfluxDB record
                    # Note: We must compare timestamps because historical tag collisions (like "wid:XXX" vs "HEM6...")
                    # can cause `last()` to return multiple rows for the same orderId.
                    current_time = r.get_time()
                    
                    if oid not in states:
                        states[oid] = {"_latest_time": None}
                    
                    last_time = states[oid].get("_latest_time")
                    
                    # If this record is newer (or equal) OR it's the very first row we process
                    if current_time and (last_time is None or current_time >= last_time):
                        states[oid]["_latest_time"] = current_time
                        for k, v in r.values.items():
                            if k in ["_time", "_start", "_stop", "_measurement", "result", "table"]:
                                continue
                            # Values in Influx are typed, but our current_snapshot might have specific types.
                            # For now, we store what we get.
                            if v is not None:
                                states[oid][k] = v
                    
            # Clean up internal tracking fields before returning
            for oid in list(states.keys()):
                states[oid].pop("_latest_time", None)
                
            return states
            
        except Exception as e:
            self.logger.error(f"Failed to fetch last order states: {e}")
            return {}
    def write_account_summary(self, summary: AccountSummary, is_paper: bool = True):
        """
        Writes account summary to the appropriate bucket.
        """
        bucket = Config.DATA_BUCKET
        measurement = "account"
        
        point = Point(measurement) \
            .tag("accountId", summary.account_code) \
            .field("net_liquidation", summary.net_liquidation) \
            .field("available_funds", summary.available_funds) \
            .field("buying_power", summary.buying_power) \
            .time(self._get_chicago_eod_time(datetime.now(timezone.utc)))
            
        # Write immediately or buffer? Account updates are less frequent, safe to write immediately or buffer.
        # Let's write immediately for account info to be fresh.
        try:
            if self.write_api:
                self.write_api.write(bucket=bucket, record=point)
                self.logger.debug(f"Written account summary to {bucket}")
        except Exception as e:
            self.logger.error(f"Failed to write account summary: {e}")

    def get_execution_context(self, exec_id: str) -> Optional[Dict]:
        """
        Queries InfluxDB to find the timestamp AND tags of an existing execution by ExecId.
        Used for crash recovery or context merging.
        Returns: {'timestamp': datetime, 'tags': {Symbol: ..., AccountId: ..., PermId: ...}}
        """
        bucket = Config.DATA_BUCKET
        # We need _time and the tags
        query = f'''
        from(bucket: "{bucket}")
          |> range(start: -7d)
          |> filter(fn: (r) => r["_measurement"] == "executions")
          |> filter(fn: (r) => r["ExecId"] == "{exec_id}")
          |> limit(n: 1)
        '''
        try:
            result = self.query_api.query(org=self.org, query=query)
            if result and len(result) > 0 and len(result[0].records) > 0:
                record = result[0].records[0]
                return {
                    'timestamp': record.get_time(),
                    'tags': {
                        'Symbol': record.values.get('Symbol'),
                        'AccountId': record.values.get('AccountId'),
                        'PermId': record.values.get('PermId'),
                        'ExecId': record.values.get('ExecId'), # Should match
                        'Strategy': record.values.get('Strategy')
                    }
                }
        except Exception as e:
            self.logger.error(f"Failed to query execution context for {exec_id}: {e}")
        return None

    def get_executions(self, strategy: str = None, symbol: str = None, start: str = "-30d") -> List[Dict]:
        """
        Queries executions for a specific strategy or symbol.
        Used for initializing PnL state in RODSIC_Strat.
        Returns a list of execution dicts sorted by time.
        """
        bucket = Config.DATA_BUCKET
        
        # Base filter
        filters = []
        filters.append('r["_measurement"] == "executions"')
        
        if strategy:
            filters.append(f'r["Strategy"] == "{strategy}"')
            
        if symbol:
            filters.append(f'r["Symbol"] == "{symbol}"')
            
        filter_str = " |> ".join([f'filter(fn: (r) => {f})' for f in filters])
        
        query = f'''
        from(bucket: "{bucket}")
          |> range(start: {start})
          |> {filter_str}
          |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
          |> sort(columns: ["_time"])
        '''
        
        results = []
        try:
            tables = self.query_api.query(org=self.org, query=query)
            for table in tables:
                for record in table.records:
                    # Reconstruct execution object
                    exec_data = {
                        "execId": record.values.get("ExecId"),
                        "symbol": record.values.get("Symbol"),
                        "account": record.values.get("AccountId"),
                        "side": record.values.get("Side"), # e.g. BOT/SLD
                        "qty": float(record.values.get("Quantity", 0)),
                        "price": float(record.values.get("FillPrice", 0)),
                        "time": record.get_time().isoformat(),
                        "strategy": record.values.get("Strategy"),
                        "permId": record.values.get("PermId")
                    }
                    results.append(exec_data)
        except Exception as e:
            self.logger.error(f"Failed to query executions: {e}")
            
        return results

    def write_execution(self, execution_data: Dict, is_paper: bool = True, timestamp: datetime = None):
        """
        Writes execution data.
        """
        # Bucket auto-selected by Config based on APP_MODE
        bucket = Config.DATA_BUCKET
        measurement = "executions"
        
        # Use provided timestamp or current time
        ts = timestamp if timestamp else datetime.now(timezone.utc)
        
        # Conversion from dict to Point
        point = Point(measurement) \
            .tag("AccountId", execution_data.get("accountId")) \
            .tag("Symbol", execution_data.get("symbol")) \
            .tag("PermId", execution_data.get("permId")) \
            .tag("ExecId", execution_data.get("execId")) \
            .tag("Strategy", execution_data.get("strategy")) \
            .field("OrderId", execution_data.get("orderId")) \
            .field("Side", execution_data.get("side")) \
            .field("Quantity", execution_data.get("quantity")) \
            .field("FillPrice", execution_data.get("fillPrice")) \
            .field("SecType", execution_data.get("secType")) \
            .field("AvgPrice", execution_data.get("avgPrice")) \
            .time(ts)

        try:
            if self.write_api:
                self.write_api.write(bucket=bucket, record=point)
                self.logger.info(f"Written execution to {bucket} with ts {ts}")
        except Exception as e:
            self.logger.error(f"Failed to write execution: {e}")

    def write_commission(self, comm_data: Dict, is_paper: bool = True, timestamp: datetime = None, extra_tags: Dict = None):
        """
        Writes commission data to the SAME measurement as executions, linked by ExecId.
        To merge into the same series, timestamp AND ALL TAGS must match.
        """
        bucket = Config.DATA_BUCKET
        measurement = "executions"
        
        if not timestamp:
            self.logger.error("No timestamp provided for commission write. Skipping to avoid data fragmentation.")
            return

        point = Point(measurement) \
            .tag("ExecId", comm_data.get("execId")) \
            .field("Currency", comm_data.get("currency")) \
            .field("Commission", comm_data.get("commission")) \
            .field("RealizedPNL", comm_data.get("realizedPNL")) \
            .field("Yield", comm_data.get("yield")) \
            .field("YieldRedemptionDate", comm_data.get("yieldRedemptionDate")) \
            .time(timestamp)
            
        # Apply extra tags to match execution record (Symbol, AccountId, etc)
        if extra_tags:
            for key, val in extra_tags.items():
                if val:
                    point.tag(key, val)

        try:
            if self.write_api:
                self.write_api.write(bucket=bucket, record=point)
                self.logger.info(f"Written commission update to {bucket} with MERGED ts {timestamp}")
        except Exception as e:
            self.logger.error(f"Failed to write commission: {e}")

    def get_last_execution_time(self) -> Optional[datetime]:
        """
        Retrieves the timestamp of the most recent execution stored in InfluxDB.
        Used to filter out duplicates during startup reconciliation.
        """
        bucket = Config.DATA_BUCKET
        query = f'''
        from(bucket: "{bucket}")
          |> range(start: -30d) 
          |> filter(fn: (r) => r["_measurement"] == "executions")
          |> last()
        '''
        try:
            result = self.query_api.query(org=self.org, query=query)
            if result and len(result) > 0 and len(result[0].records) > 0:
                # The time of the last record
                return result[0].records[0].get_time()
        except Exception as e:
            self.logger.error(f"Failed to get last execution time: {e}")
        return None

    def get_recent_executions_context(self) -> List[Dict]:
        """
        Retrieves ALL recent executions (last 48h) to populate the memory cache.
        
        Why ALL and not just incomplete ones?
        1. Robustness: Ensures we have the Context (Timestamp + Tags) for ANY late commission report, 
           even if we thought the execution was "done".
        2. Simplicity: Querying for "missing fields" in InfluxDB (Flux) is complex and slow.
           Fetching recent history is fast and ensures we never discard a valid commission due to missing context.
        """
        bucket = Config.DATA_BUCKET
        # Query last 2 days of executions
        # We need distinct ExecIds and their tags/timestamps
        query = f'''
        from(bucket: "{bucket}")
          |> range(start: -2d)
          |> filter(fn: (r) => r["_measurement"] == "executions")
          |> filter(fn: (r) => r["_field"] == "FillPrice") 
          |> unique(column: "ExecId")
        '''
        
        contexts = []
        try:
            result = self.query_api.query(org=self.org, query=query)
            for table in result:
                for record in table.records:
                    contexts.append({
                        'execId': record.values.get('ExecId'),
                        'timestamp': record.get_time(),
                        'tags': {
                            'Symbol': record.values.get('Symbol'),
                            'AccountId': record.values.get('AccountId'),
                            'PermId': record.values.get('PermId'),
                            'ExecId': record.values.get('ExecId')
                        }
                    })
        except Exception as e:
            self.logger.error(f"Failed to get incomplete executions: {e}")
        return contexts

    def get_last_portfolio_state(self) -> Dict[Tuple[str, str], float]:
        """
        Retrieves the last known portfolio state from InfluxDB to prevent duplicate writes.
        Returns: Dict[(Account, Symbol) -> Quantity]
        """
        bucket = Config.DATA_BUCKET
        measurement = "positions_reconciled"
        
        # 1. Find the timestamp of the last batch write (look back 7 days)
        # We group by measurement to find the global max time across all series
        query_time = f'''
        from(bucket: "{bucket}")
          |> range(start: -7d)
          |> filter(fn: (r) => r["_measurement"] == "{measurement}")
          |> group()
          |> max(column: "_time")
        '''
        
        try:
            time_result = self.query_api.query(org=self.org, query=query_time)
            last_time = None
            
            # Extract the single max timestamp
            for table in time_result:
                for record in table.records:
                    last_time = record.get_time()
                    break
                if last_time: break
            
            if not last_time:
                return {}
                
            # 2. Fetch all records at that EXACT timestamp
            # We use a tiny window around the timestamp to be safe (microseconds)
            # Flux range is exclusive for stop, so we add a bit.
            from datetime import timedelta
            t_start = last_time
            t_stop = last_time + timedelta(microseconds=1) 
            
            # Format explicitly or let client handle it? 
            # Client handles datetime objects in range() if using param binding, but here we inject.
            # Safer to use ISO format string if constructing raw query.
            # NOTE: influxdb-client-python doesn't easily support object param binding for raw Flux strings like this without separate Params object.
            # Use string formatting carefully.
            
            t_start_str = t_start.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
            t_stop_str = t_stop.strftime("%Y-%m-%dT%H:%M:%S.%fZ")

            query_data = f'''
            from(bucket: "{bucket}")
              |> range(start: {t_start_str}, stop: {t_stop_str})
              |> filter(fn: (r) => r["_measurement"] == "{measurement}")
              |> pivot(rowKey:["_time"], columnKey:["_field"], valueColumn:"_value")
            '''
            
            result = self.query_api.query(org=self.org, query=query_data)
            
            state = {}
            for table in result:
                for r in table.records:
                    # After pivot, fields are in values. 
                    # Tags (Account, Symbol) are also in values.
                    # We need Account, Symbol -> Quantity
                    qty = float(r.values.get("quantity", 0.0))
                    acc = r.values.get("account")
                    sym = r.values.get("symbol")
                    
                    if acc and sym:
                        state[(acc, sym)] = qty
                        
            return state
            
        except Exception as e:
            self.logger.error(f"Failed to get last portfolio state: {e}")
            return {}

    def close(self):
        """Closes the InfluxDB client and flushes buffer."""
        self.stop_monitor()
        
        with self.lock:
             self._flush_buffer_internal(Config.INFLUXDB_BUCKET_PRICES)
                 
        if self.client:
            self.client.close()

    def get_contract_data(self, g_con_id: str = None, symbol: str = None, bucket: str = "ib_prices_lab", start: str = "-30d") -> pd.DataFrame:
        """
        Queries contract data from InfluxDB.
        """
        # Filter logic
        f_string = ""
        if symbol:
             f_string = f'|> filter(fn: (r) => r["symbol"] == "{symbol}")'
        elif g_con_id:
             # Legacy or just in case
             f_string = f'|> filter(fn: (r) => r["gConId"] == "{g_con_id}")'
             
        query = f'''
        from(bucket: "{bucket}")
          |> range(start: {start})
          {f_string}
          |> filter(fn: (r) => r["_measurement"] == "precios")
          |> pivot(rowKey:["_time"], columnKey:["_field"], valueColumn:"_value")
          |> sort(columns: ["_time"])
        '''
        
        try:
            result = self.query_api.query_data_frame(query)
            if isinstance(result, list):
                if not result: return pd.DataFrame()
                result = pd.concat(result)
            return result
        except Exception as e:
            print(f"InfluxDB Query Error: {e}")
            return pd.DataFrame()

    def get_last_price_record(self, symbol: str) -> Dict[str, float]:
        """
        Fetches the single most recent pricing record from the precios measurement.
        Returns a dict: {'BID': float, 'ASK': float, 'LAST': float}
        """
        bucket = Config.INFLUXDB_BUCKET_PRICES
        query = f'''
        from(bucket: "{bucket}")
          |> range(start: -30d)
          |> filter(fn: (r) => r["_measurement"] == "precios")
          |> filter(fn: (r) => r["symbol"] == "{symbol}")
          |> last()
          |> pivot(rowKey:["_time"], columnKey:["_field"], valueColumn:"_value")
        '''
        
        try:
            df = self.query_api.query_data_frame(query)
            if isinstance(df, list) and df:
                df = df[0]
            
            if df is not None and not df.empty:
                row = df.iloc[-1]
                result = {
                    "BID": float(row.get("BID", 0.0)),
                    "ASK": float(row.get("ASK", 0.0)),
                    "LAST": float(row.get("LAST", 0.0))
                }
                self.logger.info(f"Mapped Price Record for {symbol}: {result}")
                return result
        except Exception as e:
            self.logger.error(f"Failed to fetch last record for {symbol}: {e}")
            
        return {"BID": 0.0, "ASK": 0.0, "LAST": 0.0}


    def get_realtime_synthetic_price(self, legs: List[Dict]) -> Dict[str, Optional[float]]:
        """
        Calculates current synthetic price from latest_prices cache.
        legs: List of dicts with 'symbol', 'ratio', 'action', 'g_con_id'.
        """
        if not legs:
            return {"bid": None, "ask": None, "last": None}

        spread_bid = 0.0
        spread_ask = 0.0
        spread_last = 0.0
        
        # We need ALL legs to have at least one price to calculate
        # However, for BID/ASK it's stricter.
        
        for leg in legs:
            l_gconid = leg.get('g_con_id') or leg.get('gConId')
            # If still missing, attempt normalized generation
            if not l_gconid:
                from utils import generate_g_con_id, parse_single_leg_details
                details = parse_single_leg_details(leg.get('symbol', ''))
                # generate_g_con_id now handles month code to number conversion
                l_gconid = generate_g_con_id(
                    details['product'], 
                    details['month'], 
                    details['year'], 
                    leg.get('secType', 'FUT')
                )

            prices = self.latest_prices.get(l_gconid, {})
            ratio = float(leg.get('ratio', 1))
            action = leg.get('action', 'BUY')
            
            self.logger.info(f"BAG Calculation: Processing leg {leg.get('symbol')} (gConId: {l_gconid}) - Prices: {prices}")
            
            # We treat 0.0 as potentially invalid for calculation to avoid skewed spreads
            l_bid = prices.get('BID')
            if (l_bid or 0.0) == 0.0: l_bid = None
            
            l_ask = prices.get('ASK')
            if (l_ask or 0.0) == 0.0: l_ask = None
            
            l_last = prices.get('LAST')
            if (l_last or 0.0) == 0.0: l_last = None
            
            # Fail fast if ANY pricing data is missing for ANY leg
            if l_bid is None and l_ask is None and l_last is None:
                self.logger.info(f"BAG Calculation: Incomplete data for leg {leg.get('symbol')}. Aborting spread calculation.")
                return {"bid": None, "ask": None, "last": None}
            # Also fail if they are all 0.0 (initial state or invalid data)
            if (l_bid or 0.0) == 0.0 and (l_ask or 0.0) == 0.0 and (l_last or 0.0) == 0.0:
                return {"bid": None, "ask": None, "last": None}
                
            if action == 'BUY':
                if spread_bid is not None and l_bid is not None: spread_bid += l_bid * ratio
                else: spread_bid = None
                
                if spread_ask is not None and l_ask is not None: spread_ask += l_ask * ratio
                else: spread_ask = None
                
                if spread_last is not None and l_last is not None: spread_last += l_last * ratio
                else: spread_last = None
            else: # SELL
                # Formula: Spread_Bid = BuyLegs_Bid - SellLegs_Ask
                # Formula: Spread_Ask = BuyLegs_Ask - SellLegs_Bid
                if spread_bid is not None and l_ask is not None: spread_bid -= l_ask * ratio
                else: spread_bid = None
                
                if spread_ask is not None and l_bid is not None: spread_ask -= l_bid * ratio
                else: spread_ask = None
                
                if spread_last is not None and l_last is not None: spread_last -= l_last * ratio
                else: spread_last = None

            if spread_bid is None and spread_ask is None and spread_last is None:
                break # Can't calculate anything
                
        res = {
            "bid": spread_bid,
            "ask": spread_ask,
            "last": spread_last
        }
        self.logger.info(f"BAG Calculation: Success! Calculated Spread: {res}")
        return res
