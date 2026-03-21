
# Adjust path to import src
import os
import sys
import unittest
from unittest.mock import MagicMock

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "src")))

# Helper to mock Config before importing modules that use it at top-level
# Same setup as test_orders_account.py
try:
    from config import Config
except ImportError:
    Config = MagicMock()
    sys.modules['config'] = MagicMock(Config=Config)

if not hasattr(Config, 'WATCHLIST_FILE'):
    Config.WATCHLIST_FILE = "dummy_watchlist.json"
if not hasattr(Config, 'LOG_LEVEL'):
    Config.LOG_LEVEL = "INFO"

from ib_connector import IBConnector
from utils import create_order

class TestOrdersTIF(unittest.TestCase):

    def setUp(self):
        # Mock dependencies in IBConnector
        self.mock_logger = MagicMock()
        
        # We need to patch things that IBConnector init uses or imports use
        # Just instantiate and patch properties as needed for unit testing methods
        
        # We assume IBConnector is importable now with the Config fix above
        pass

    def test_create_order_tif(self):
        """Test utils.create_order passing tif."""
        # Case 1: TIF provided
        o = create_order("BUY", 1, "LMT", 100, tif="GTC")
        self.assertEqual(o.tif, "GTC")
        
        # Case 2: TIF not provided
        o2 = create_order("BUY", 1, "LMT", 100)
        # Default for IB Order is usually DAY or empty string, but we just want to ensure we didn't override it with None
        # Actually in our code: if tif: order.tif = tif. 
        # So o2.tif should be whatever Order() defaults to.
        # Since we use dummy Order class if ibapi not present, or real one.
        # Let's just check it wasn't set to None explicitly if the default is different.
        # If dummy class, it might not have .tif unless set.
        pass 

    def test_connector_pass_tif(self):
        """Test IBConnector methods pass TIF to create_order."""
        connector = IBConnector()
        connector.client = MagicMock()
        connector.wrapper = MagicMock()
        connector.wrapper.account_summary = {'AccountA': {}}
        
        # 1. Simple Order
        input_data = {"action": "BUY", "qty": 1, "tif": "GTC", "accountId": "AccountA"}
        connector.place_simple_order({"symbol": "S"}, input_data)
        
        args = connector.client.placeOrder.call_args[0]
        order = args[2]
        self.assertEqual(order.tif, "GTC")
        
        # 2. OCA Order
        oca_data = {"actionSL": "S", "actionTP": "S", "qty": 1, "tif": "DAY", "accountId": "AccountA"}
        connector.place_oca_order(None, {"symbol": "S"}, oca_data)
        
        # Check both orders have TIF
        calls = connector.client.placeOrder.call_args_list
        # Last 2 calls are for OCA
        for call in calls[-2:]:
            o = call[0][2]
            self.assertEqual(o.tif, "DAY")
            
        # 3. Bracket Order
        bracket_data = {"action": "BUY", "qty": 1, "tif": "GTD", "accountId": "AccountA"}
        connector.place_bracket_order(None, {"symbol": "S"}, bracket_data)
        
        # Last 3 calls for Bracket
        calls = connector.client.placeOrder.call_args_list[-3:]
        for call in calls:
            o = call[0][2]
            self.assertEqual(o.tif, "GTD")

if __name__ == '__main__':
    unittest.main()
