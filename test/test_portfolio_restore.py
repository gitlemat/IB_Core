
import unittest
from unittest.mock import MagicMock
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

if not hasattr(Config, 'WATCHLIST_FILE'):
    Config.WATCHLIST_FILE = "dummy_watchlist.json"
if not hasattr(Config, 'LOG_LEVEL'):
    Config.LOG_LEVEL = "INFO"
if not hasattr(Config, 'INFLUXDB_URL'):
    Config.INFLUXDB_URL = "http://localhost:8086"
if not hasattr(Config, 'INFLUXDB_TOKEN'):
    Config.INFLUXDB_TOKEN = "token"
if not hasattr(Config, 'INFLUXDB_ORG'):
    Config.INFLUXDB_ORG = "org"

from portfolio_manager import PortfolioManager

class ContractMock:
    def __init__(self, symbol, secType="FUT", g_con_id=None):
        self.symbol = symbol
        self.localSymbol = symbol
        self.secType = secType
        self.currency = "USD"
        if g_con_id:
            self._g_con_id = g_con_id

class TestPortfolioRestore(unittest.TestCase):
    def setUp(self):
        self.mock_db = MagicMock()
        self.mock_watchlist = MagicMock()
        self.pm = PortfolioManager(self.mock_db, self.mock_watchlist)
        
    def test_startup_restore_lazy_load(self):
        # 1. Setup Mock DB to return a known state
        # State: Acct1, HEM6, LEG, 10
        known_state = {("Acct1", "HEM6", "LEG", 10.0)}
        self.mock_db.get_last_portfolio_state.return_value = known_state
        
        # 2. Check Init State
        self.assertEqual(len(self.pm.last_reconciled_set), 0)
        self.assertFalse(self.pm.initial_state_loaded)
        
        # 3. Process Position Update that MATCHES known state
        c1 = ContractMock("HEM6", g_con_id="gid1")
        # In PM reconcile, symbol is contract.symbol. c1.symbol="HEM6"
        # We need g_con_id "gid1" to map to "HEM6" in logic?
        # PM sets self.raw_contracts["gid1"] = c1.
        # Reconcile uses contract.symbol. Correct.
        
        self.pm.on_position_update("Acct1", c1, 10, 50.0)
        
        active_subs = {}
        
        # 4. First Reconcile
        self.pm.reconcile(active_subs)
        
        # Expectations:
        # a) DB Load triggered
        self.mock_db.get_last_portfolio_state.assert_called_once()
        self.assertTrue(self.pm.initial_state_loaded)
        self.assertEqual(self.pm.last_reconciled_set, known_state)
        
        # b) Write NOT triggered (because current == restored)
        self.mock_db.write_positions.assert_not_called()
        
        # 5. Second Reconcile with CHANGE
        self.pm.on_position_update("Acct1", c1, 15, 50.0) # Change qty
        self.pm.reconcile(active_subs)
        
        self.mock_db.write_positions.assert_called_once()
        self.mock_db.get_last_portfolio_state.assert_called_once() # Still only once

if __name__ == '__main__':
    unittest.main()
