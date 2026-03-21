
import unittest
from unittest.mock import MagicMock, patch
import sys
import os
from datetime import datetime, timezone, timedelta

# Adjust path to import src
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "src")))

# Mock Config
try:
    from config import Config
except ImportError:
    from unittest.mock import MagicMock
    Config = MagicMock()
    sys.modules['config'] = MagicMock(Config=Config)

# Ensure mocks have necessary attrs
if not hasattr(Config, 'INFLUXDB_BUCKET_PRICES'): Config.INFLUXDB_BUCKET_PRICES = "prices"
if not hasattr(Config, 'WATCHLIST_FILE'): Config.WATCHLIST_FILE = "watchlist.json"
if not hasattr(Config, 'LOG_LEVEL'): Config.LOG_LEVEL = "INFO"
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
    def __init__(self, execId, time_str):
        self.execId = execId
        self.orderId = 101
        self.time = time_str
        self.side = "BOT"
        self.shares = 10.0
        self.price = 150.0
        self.permId = 12345
        self.avgPrice = 150.0
        self.accountId = "DU123"

from ib_connector import IBConnector

class TestReconciliation(unittest.TestCase):
    def setUp(self):
        with patch('ib_connector.DatabaseClient') as MockDB:
            self.connector = IBConnector()
            self.connector.db_client = MockDB.return_value
            self.connector.client = MagicMock()
            self.connector.client.is_paper = True
            
            # Setup logger to avoid noise
            self.connector.logger = MagicMock()

    def test_reconciliation_flow(self):
        """
        Verify startup reconciliation logic:
        1. Load incomplete/recent executions to cache.
        2. Set last_db_execution_time.
        3. Trigger reqExecutions.
        4. Filter incoming executions based on time.
        """
        
        # --- PHASE 1: Startup ---
        
        # Mock Incomplete Executions (ExecId: CACHED_001)
        mock_incomplete = [{
            'execId': 'CACHED_001',
            'timestamp': datetime(2023, 10, 27, 10, 0, 0, tzinfo=timezone.utc),
            'tags': {'Symbol': 'MSFT', 'AccountId': 'DU123', 'PermId': 111, 'ExecId': 'CACHED_001'}
        }]
        self.connector.db_client.get_recent_executions_context.return_value = mock_incomplete
        
        # Mock Last Execution Time (12:00 UTC)
        last_time = datetime(2023, 10, 27, 12, 0, 0, tzinfo=timezone.utc)
        self.connector.db_client.get_last_execution_time.return_value = last_time
        
        # Run Reconciliation
        self.connector.reconcile_executions()
        
        # Verify: Cache loaded
        self.assertIn('CACHED_001', self.connector.execution_context)
        self.assertEqual(self.connector.execution_context['CACHED_001']['tags']['Symbol'], 'MSFT')
        
        # Verify: Client requested executions
        self.connector.client.reqExecutions.assert_called_once()
        
        # --- PHASE 2: Handling Incoming Executions ---
        
        contract = MockContract()
        
        # Case A: Old Execution (Before last_time) -> Should be SKIPPED
        # Time: 11:00 UTC (last_time is 12:00)
        exec_old = MockExecution("OLD_001", "20231027  11:00:00")
        
        self.connector.handle_execution(9999, contract, exec_old)
        
        # Verify: NOT written to DB
        # Note: write_execution might be called with timestamp arg now
        # We need to check call args to be sure passed execution data is not for OLD_001
        # But simplify: assert not called if we assume only this call happens
        # Or better: check list of calls
        
        # Verify: Still Cached (for robustness)
        self.assertIn("OLD_001", self.connector.execution_context)
        
        # Case B: New Execution (After last_time) -> Should be WRITTEN
        # Time: 13:00 UTC
        exec_new = MockExecution("NEW_001", "20231027  13:00:00")
        
        self.connector.handle_execution(9999, contract, exec_new)
        
        # Verify: Written to DB
        # write_execution called once (only for NEW_001)
        self.connector.db_client.write_execution.assert_called_once()
        args, kwargs = self.connector.db_client.write_execution.call_args
        self.assertEqual(args[0]['execId'], "NEW_001")
        
        
if __name__ == '__main__':
    unittest.main()
