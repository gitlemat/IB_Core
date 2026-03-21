
import unittest
from unittest.mock import MagicMock, patch
import sys
import os

# Adjust path to import src
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "src")))

# Helper to mock Config before importing modules that use it at top-level
from unittest.mock import MagicMock
import sys

# Mock Config if not already present or if we need to inject attributes
# Since we are running outside the app context, we might need to manually ensure Config has what we need
# before importing watchlist
try:
    from config import Config
except ImportError:
    # If config import fails (unlikely given sys.path), we mock it
    Config = MagicMock()
    sys.modules['config'] = MagicMock(Config=Config)

# Ensure WATCHLIST_FILE exists on Config
if not hasattr(Config, 'WATCHLIST_FILE'):
    Config.WATCHLIST_FILE = "dummy_watchlist.json"

from ib_connector import IBConnector
from utils import create_contract

class TestOrdersAccount(unittest.TestCase):

    def setUp(self):
        # Mock dependencies
        self.mock_logger = MagicMock()
        
        # Patch Config.WATCHLIST_FILE since WatchlistManager uses it at import/init time
        # Note: If it's used at default arg time, we might need to patch strictly before import or mock Config class attribute
        self.config_patcher = patch('config.Config.WATCHLIST_FILE', 'dummy_watchlist.json')
        self.config_patcher.start()
        
        # Patch Logger to return mock
        patcher = patch('logger.LoggerSetup.get_logger', return_value=self.mock_logger)
        self.mock_get_logger = patcher.start()
        self.addCleanup(patcher.stop)
        
        # Mock DB Client and Watchlist
        with patch('ib_connector.DatabaseClient'), \
             patch('ib_connector.WatchlistManager'), \
             patch('ib_connector.PortfolioManager'):
             
            self.connector = IBConnector()
            
        # Mock Wrapper and Client
        self.connector.wrapper = MagicMock()
        self.connector.client = MagicMock()
        
        # Mock account summary keys
        self.connector.wrapper.account_summary = {'AccountA': {}, 'AccountB': {}}
        
    def test_place_order_with_account(self):
        """Test placing an order with a specific account ID."""
        contract_data = {"symbol": "ES", "secType": "FUT"}
        order_data = {
            "action": "BUY", "qty": 1, "oType": "LMT", "LmtPrice": 4000,
            "accountId": "AccountB"
        }
        
        # Mock create_contract and create_order used internally
        # But we can rely on real utils if imported. 
        # ib_connector imports them.
        
        oid = self.connector.place_simple_order(contract_data, order_data)
        
        # Verify client.placeOrder called
        self.connector.client.placeOrder.assert_called_once()
        args = self.connector.client.placeOrder.call_args[0]
        # args: (order_id, contract, order)
        order_obj = args[2]
        
        self.assertEqual(order_obj.account, "AccountB")
        
    def test_place_order_default_account(self):
        """Test placing an order without account ID defaults to first sorted account."""
        contract_data = {"symbol": "ES", "secType": "FUT"}
        order_data = {
            "action": "BUY", "qty": 1, "oType": "LMT", "LmtPrice": 4000
            # No accountId
        }
        
        # Expected default: sorted keys are ['AccountA', 'AccountB'], so 'AccountA'
        
        oid = self.connector.place_simple_order(contract_data, order_data)
        
        self.connector.client.placeOrder.assert_called_once()
        order_obj = self.connector.client.placeOrder.call_args[0][2]
        
        self.assertEqual(order_obj.account, "AccountA")

    def test_place_oca_order_account(self):
        """Test placing OCA order propagates account to children."""
        contract_data = {"symbol": "ES"}
        oca_data = {
            "actionSL": "SELL", "actionTP": "SELL", "qty": 1, 
            "LmtPriceSL": 3900, "LmtPrice": 4100,
            "accountId": "AccountB"
        }
        
        self.connector.place_oca_order(None, contract_data, oca_data)
        
        # Should initiate 2 orders
        self.assertEqual(self.connector.client.placeOrder.call_count, 2)
        
        # Check first call (SL)
        args1 = self.connector.client.placeOrder.call_args_list[0][0]
        order1 = args1[2]
        self.assertEqual(order1.account, "AccountB")
        
        # Check second call (TP)
        args2 = self.connector.client.placeOrder.call_args_list[1][0]
        order2 = args2[2]
        self.assertEqual(order2.account, "AccountB")

    def test_place_bracket_order_account(self):
        """Test placing Bracket order propagates account to parent and children."""
        contract_data = {"symbol": "ES"}
        bracket_data = {
            "action": "BUY", "qty": 1, "LmtPrice": 4000,
            "LmtPriceSL": 3900, "LmtPriceTP": 4100,
            "accountId": "AccountB"
        }
        
        self.connector.place_bracket_order(None, contract_data, bracket_data)
        
        # Should initiate 3 orders
        self.assertEqual(self.connector.client.placeOrder.call_count, 3)
        
        # Check parent
        order_p = self.connector.client.placeOrder.call_args_list[0][0][2]
        self.assertEqual(order_p.account, "AccountB")
        
        # Check SL
        order_sl = self.connector.client.placeOrder.call_args_list[1][0][2]
        self.assertEqual(order_sl.account, "AccountB")

if __name__ == '__main__':
    unittest.main()
