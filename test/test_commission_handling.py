
import unittest
from unittest.mock import MagicMock, patch
import sys
import os

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

# Mock is_paper_trading method
Config.is_paper_trading = MagicMock(return_value=True)

# Mock CommissionReport since we might be in an environment without ibapi installed properly for tests or dummy class
try:
    from ibapi.commission_report import CommissionReport
except ImportError:
    class CommissionReport:
        def __init__(self):
            self.execId = ""
            self.commission = 0.0
            self.currency = ""
            self.realizedPNL = 0.0
            self.yield_ = 0.0
            self.yieldRedemptionDate = 0

from ib_connector import IBConnector

class TestCommissionHandling(unittest.TestCase):
    def setUp(self):
        # Patch DatabaseClient to avoid real InfluxDB connection
        with patch('ib_connector.DatabaseClient') as MockDB:
            self.connector = IBConnector()
            self.connector.db_client = MockDB.return_value
            self.connector.client = MagicMock() # Mock IBClient
            self.connector.client.is_paper = True
            
    def test_commission_report_flow(self):
        # Create a dummy commission report
        report = CommissionReport()
        report.execId = "EXEC12345"
        report.commission = 1.5
        report.currency = "USD"
        report.realizedPNL = 10.0
        report.yield_ = 0.0
        report.yieldRedemptionDate = 20230101
        
        # Trigger the callback on the wrapper
        self.connector.wrapper.commissionReport(report)
        
        # Verify db_client.write_commission was called
        self.connector.db_client.write_commission.assert_called_once()
        
        # Check arguments passed to write_commission
        args, kwargs = self.connector.db_client.write_commission.call_args
        comm_data = args[0]
        self.assertEqual(comm_data['execId'], "EXEC12345")

        # Now we want to verify the Point construction logic in db_client (which we mocked)
        # But we can't test db_client logic here because we mocked it in setUp!
        # We need a test that uses real db_client logic but mocks the write_api.
        
class TestDBClientCommissions(unittest.TestCase):
    def setUp(self):
        self.client = IBConnector().db_client # Get a real DB Client instance via connector or direct
        # Actually better to instantiate directly to avoid complex deps
        from db_client import DatabaseClient
        self.client = DatabaseClient()
        self.client.write_api = MagicMock()
        
    def test_merged_storage_structure(self):
        comm_data = {
            "execId": "EXEC_MERGE_TEST",
            "commission": 2.0,
            "currency": "EUR",
            "realizedPNL": 50.0
        }
        
        self.client.write_commission(comm_data)
        
        self.client.write_api.write.assert_called_once()
        call_args = self.client.write_api.write.call_args
        point = call_args[1]['record']
        
        # Verify Target Measurement
        self.assertEqual(point._name, "executions")
        
        # Verify Tags
        self.assertEqual(point._tags["ExecId"], "EXEC_MERGE_TEST")
        self.assertEqual(point._tags["Currency"], "EUR")
        
        # Verify Fields
        self.assertEqual(point._fields["Commission"], 2.0)
        self.assertEqual(point._fields["RealizedPNL"], 50.0)

if __name__ == '__main__':
    unittest.main()
