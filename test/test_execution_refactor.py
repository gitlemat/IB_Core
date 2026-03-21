
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

# Mock is_paper_trading method
Config.is_paper_trading = MagicMock(return_value=True)

# Mock IBAPI classes
class MockContract:
    def __init__(self):
        self.symbol = "AAPL"
        self.secType = "STK"
        self.currency = "USD"
        self.exchange = "SMART"
        self.localSymbol = "AAPL"

class MockExecution:
    def __init__(self):
        self.execId = "EXEC_TEST_001"
        self.orderId = 101
        self.time = "20231027  10:00:00" # Standard IB format
        self.side = "BOT"
        self.shares = 10.0
        self.price = 150.0
        self.permId = 12345
        self.avgPrice = 150.0
        self.accountId = "DU123"

class MockCommissionReport:
    def __init__(self):
        self.execId = "EXEC_TEST_001"
        self.commission = 1.0
        self.currency = "USD"
        self.realizedPNL = 0.0
        self.yield_ = 0.0
        self.yieldRedemptionDate = 0

from ib_connector import IBConnector

class TestExecutionRefactor(unittest.TestCase):
    def setUp(self):
        with patch('ib_connector.DatabaseClient') as MockDB:
            self.connector = IBConnector()
            self.connector.db_client = MockDB.return_value
            self.connector.client = MagicMock()
            self.connector.client.is_paper = True
            
    def test_standard_flow_timestamp_merging(self):
        """
        Verify that execution timestamp AND TAGS are cached and reused for commission.
        """
        contract = MockContract()
        execution = MockExecution()
        
        # 1. Handle Execution
        self.connector.handle_execution(9001, contract, execution)
        
        # Verify write_execution called
        self.connector.db_client.write_execution.assert_called_once()
        exec_args, exec_kwargs = self.connector.db_client.write_execution.call_args
        
        # Check parsed timestamp (2023-10-27 10:00:00 UTC)
        expected_ts = datetime(2023, 10, 27, 10, 0, 0, tzinfo=timezone.utc)
        self.assertEqual(exec_kwargs['timestamp'], expected_ts)
        
        # Verify logic cached the context (timestamp + tags)
        self.assertIn("EXEC_TEST_001", self.connector.execution_context)
        context = self.connector.execution_context["EXEC_TEST_001"]
        self.assertEqual(context['timestamp'], expected_ts)
        self.assertEqual(context['tags']['Symbol'], "AAPL")
        self.assertEqual(context['tags']['AccountId'], "DU123")
        
        # 2. Handle Commission
        report = MockCommissionReport()
        self.connector.handle_commission_report(report)
        
        # Verify write_commission called with SAME timestamp AND EXTRA TAGS
        self.connector.db_client.write_commission.assert_called_once()
        comm_args, comm_kwargs = self.connector.db_client.write_commission.call_args
        self.assertEqual(comm_kwargs['timestamp'], expected_ts)
        self.assertEqual(comm_kwargs['extra_tags']['Symbol'], "AAPL")
        
    def test_crash_recovery_flow(self):
        """
        Verify that if cache is empty, we recover context (ts + tags) from DB.
        """
        # Setup: Cache is empty
        self.connector.execution_context = {}
        
        # Mock DB get_execution_context to return a recovering context
        recovered_ts = datetime(2023, 10, 27, 12, 30, 0, tzinfo=timezone.utc)
        recovered_context = {
            'timestamp': recovered_ts,
            'tags': {'Symbol': 'GOOG', 'AccountId': 'DU999', 'PermId': 12345, 'ExecId': 'EXEC_TEST_001'}
        }
        self.connector.db_client.get_execution_context.return_value = recovered_context
        
        # Handle Commission
        report = MockCommissionReport()
        self.connector.handle_commission_report(report)
        
        # Verify we queried DB
        self.connector.db_client.get_execution_context.assert_called_with("EXEC_TEST_001")
        
        # Verify write_commission used the recovered timestamp and tagss
        self.connector.db_client.write_commission.assert_called_once()
        comm_args, comm_kwargs = self.connector.db_client.write_commission.call_args
        self.assertEqual(comm_kwargs['timestamp'], recovered_ts)
        self.assertEqual(comm_kwargs['extra_tags']['Symbol'], 'GOOG')
        
    def test_discard_flow(self):
        """
        Verify that if context is missing in Cache AND DB, we discard.
        """
        self.connector.execution_context = {}
        self.connector.db_client.get_execution_context.return_value = None # Not found
        
        report = MockCommissionReport()
        self.connector.handle_commission_report(report)
        
        # Verify write_commission NOT called
        self.connector.db_client.write_commission.assert_not_called()

if __name__ == '__main__':
    unittest.main()
