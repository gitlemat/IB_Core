
import unittest
from unittest.mock import MagicMock, patch
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
if not hasattr(Config, 'DATA_BUCKET'):
    Config.DATA_BUCKET = "data"
if not hasattr(Config, 'INFLUXDB_URL'):
    Config.INFLUXDB_URL = "http://localhost:8086"
if not hasattr(Config, 'INFLUXDB_TOKEN'):
    Config.INFLUXDB_TOKEN = "token"
if not hasattr(Config, 'INFLUXDB_ORG'):
    Config.INFLUXDB_ORG = "org"
if not hasattr(Config, 'WATCHLIST_FILE'):
    Config.WATCHLIST_FILE = "watchlist.json"
if not hasattr(Config, 'IB_HOST'):
    Config.IB_HOST = "127.0.0.1"
if not hasattr(Config, 'IB_PORT'):
    Config.IB_PORT = 4001
if not hasattr(Config, 'IB_CLIENT_ID'):
    Config.IB_CLIENT_ID = 1

from ib_connector import IBConnector
from db_client import DatabaseClient
from models import AccountSummary

class TestAccountSummaryWrite(unittest.TestCase):
    def setUp(self):
        # Patch DatabaseClient to avoid real InfluxDB connection
        with patch('ib_connector.DatabaseClient') as MockDB:
            self.connector = IBConnector()
            self.connector.db_client = MockDB.return_value
            self.connector.client = MagicMock() # Mock IBClient
            self.connector.client.is_paper = True
            
    def test_handle_account_summary_end(self):
        # Setup accumulated data in wrapper
        self.connector.wrapper.account_summary = {
            "DU12345": {
                "NetLiquidation": "100500.25",
                "AvailableFunds": "50000.00",
                "BuyingPower": "200000.00"
            }
        }
        
        # Trigger the handler
        self.connector.handle_account_summary_end(9001)
        
        # Verify db_client.write_account_summary was called
        self.connector.db_client.write_account_summary.assert_called_once()
        
        # Check arguments
        args, kwargs = self.connector.db_client.write_account_summary.call_args
        summary_obj = args[0]
        
        self.assertIsInstance(summary_obj, AccountSummary)
        self.assertEqual(summary_obj.account_code, "DU12345")
        self.assertEqual(summary_obj.net_liquidation, 100500.25)
        self.assertEqual(summary_obj.available_funds, 50000.00)
        
    def test_handle_empty_summary(self):
        self.connector.wrapper.account_summary = {}
        self.connector.handle_account_summary_end(9001)
        self.connector.db_client.write_account_summary.assert_not_called()

if __name__ == '__main__':
    unittest.main()
