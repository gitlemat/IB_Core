
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

if not hasattr(Config, 'LOG_LEVEL'): Config.LOG_LEVEL = "INFO"
if not hasattr(Config, 'WATCHLIST_FILE'): Config.WATCHLIST_FILE = "watchlist.json"

from portfolio_manager import PortfolioManager
from db_client import DatabaseClient
from watchlist import WatchlistManager

class MockContract:
    def __init__(self, symbol):
        self.symbol = symbol
        self.localSymbol = symbol
        self.secType = "STK"
        self._g_con_id = symbol # Simple ID

class TestPortfolioOptimization(unittest.TestCase):
    def setUp(self):
        self.mock_db = MagicMock(spec=DatabaseClient)
        self.mock_wl = MagicMock(spec=WatchlistManager)
        # Mock get_contracts to avoid issues
        self.mock_wl.get_contracts.return_value = {}
        
        self.pm = PortfolioManager(self.mock_db, self.mock_wl)
        
        # Setup Logger
        self.pm.logger = MagicMock()
        
    def test_deduplication_no_change(self):
        """Verify NO write if state matches DB."""
        # 1. Setup DB state
        self.mock_db.get_last_portfolio_state.return_value = {
            ('DU123', 'Fixed.AAPL'): 10.0
        }
        
        # 2. Update with SAME position
        self.pm.raw_positions = {'DU123': {'Fixed.AAPL': 10.0}}
        self.pm.raw_avg_costs = {'DU123': {'Fixed.AAPL': 150.0}}
        self.pm.raw_contracts = {'Fixed.AAPL': MockContract('Fixed.AAPL')}
        
        self.pm.reconcile({})
        
        # 3. Verify
        self.mock_db.write_positions.assert_not_called()
        
    def test_write_on_change(self):
        """Verify write if quantity changes."""
        # 1. Setup DB state
        self.mock_db.get_last_portfolio_state.return_value = {
            ('DU123', 'Fixed.AAPL'): 10.0
        }
        
        # 2. Update with NEW quantity
        self.pm.raw_positions = {'DU123': {'Fixed.AAPL': 15.0}}
        self.pm.raw_avg_costs = {'DU123': {'Fixed.AAPL': 150.0}}
        self.pm.raw_contracts = {'Fixed.AAPL': MockContract('Fixed.AAPL')}
        
        self.pm.reconcile({})
        
        # 3. Verify
        self.mock_db.write_positions.assert_called_once()
        args = self.mock_db.write_positions.call_args[0][0]
        self.assertEqual(len(args), 1)
        self.assertEqual(args[0]['qty'], 15.0)
        
    def test_graceful_close(self):
        """Verify write 0 if position disappears."""
        # 1. Setup DB state (Was 10)
        self.mock_db.get_last_portfolio_state.return_value = {
            ('DU123', 'Fixed.AAPL'): 10.0
        }
        
        # 2. Update empty (Closed)
        self.pm.raw_positions = {}
        
        self.pm.reconcile({})
        
        # 3. Verify Write 0
        self.mock_db.write_positions.assert_called_once()
        args = self.mock_db.write_positions.call_args[0][0]
        self.assertEqual(args[0]['symbol'], 'Fixed.AAPL')
        self.assertEqual(args[0]['qty'], 0.0)
        
    def test_no_spam_zero(self):
        """Verify NO write if 0 and already 0."""
        # 1. Setup DB state (Was 0)
        self.mock_db.get_last_portfolio_state.return_value = {
            ('DU123', 'Fixed.AAPL'): 0.0
        }
        
        # 2. Update empty
        self.pm.raw_positions = {}
        
        self.pm.reconcile({})
        
        # 3. Verify NO write
        self.mock_db.write_positions.assert_not_called()
        
    def test_reconciled_type_rename(self):
        """Verify Strategy becomes RECONCILED."""
        pass

if __name__ == '__main__':
    unittest.main()
