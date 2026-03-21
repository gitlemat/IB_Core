
import unittest
from unittest.mock import MagicMock
import sys
import os
from datetime import datetime, timezone

# Adjust path to import src
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "src")))

# Mock Config
try:
    from config import Config
except ImportError:
    from unittest.mock import MagicMock
    Config = MagicMock()
    sys.modules['config'] = MagicMock(Config=Config)

if not hasattr(Config, 'INFLUXDB_BUCKET_PRICES'):
    Config.INFLUXDB_BUCKET_PRICES = "prices"
if not hasattr(Config, 'LOG_LEVEL'):
    Config.LOG_LEVEL = "INFO"
if not hasattr(Config, 'INFLUXDB_URL'):
    Config.INFLUXDB_URL = "http://localhost:8086"
if not hasattr(Config, 'INFLUXDB_TOKEN'):
    Config.INFLUXDB_TOKEN = "token"
if not hasattr(Config, 'INFLUXDB_ORG'):
    Config.INFLUXDB_ORG = "org"
if not hasattr(Config, 'DATA_BUCKET'):
    Config.DATA_BUCKET = "data"

from db_client import DatabaseClient
from models import AccumulatedTickRecord

class TestPriceOptimization(unittest.TestCase):
    def setUp(self):
        self.client = DatabaseClient()
        self.client.write_api = MagicMock()
        
    def test_price_deduplication(self):
        gid = "gid_123"
        symbol = "ESM6"
        
        # 1. First Record (New) -> Should Queue
        r1 = AccumulatedTickRecord(
            time=datetime.now(timezone.utc),
            contract_g_con_id=gid,
            bid_price=10.0, ask_price=11.0, last_price=10.5,
            bid_size=1, ask_size=1, last_size=1
        )
        self.client._queue_price_record(r1, symbol)
        
        self.assertEqual(len(self.client.write_buffer), 1)
        self.assertEqual(self.client.last_queued_prices[gid], (10.0, 11.0, 10.5))
        
        # 2. Second Record (Same Prices) -> Should Duplicate Filter
        r2 = AccumulatedTickRecord(
            time=datetime.now(timezone.utc),
            contract_g_con_id=gid,
            bid_price=10.0, ask_price=11.0, last_price=10.5,
            bid_size=5, ask_size=5, last_size=5 # Sizes changed, but we dedupe on price
        )
        self.client._queue_price_record(r2, symbol)
        
        self.assertEqual(len(self.client.write_buffer), 1) # Still 1
        
        # 3. Third Record (Price Change) -> Should Queue
        r3 = AccumulatedTickRecord(
            time=datetime.now(timezone.utc),
            contract_g_con_id=gid,
            bid_price=10.0, ask_price=11.0, last_price=10.75, # Changed Last
            bid_size=1, ask_size=1, last_size=1
        )
        self.client._queue_price_record(r3, symbol)
        
        self.assertEqual(len(self.client.write_buffer), 2)
        self.assertEqual(self.client.last_queued_prices[gid], (10.0, 11.0, 10.75))

if __name__ == '__main__':
    unittest.main()
