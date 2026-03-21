import sys
import os
import unittest
from unittest.mock import MagicMock, patch

# Mock dependencies
sys.modules['logger'] = MagicMock()
sys.modules['db_client'] = MagicMock()
sys.modules['watchlist'] = MagicMock()
sys.modules['utils'] = MagicMock()

# Add src to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "src")))

from portfolio_manager import PortfolioManager

class TestPortfolioManager(unittest.TestCase):
    def test_multi_account_reconciliation(self):
        db_mock = MagicMock()
        wl_mock = MagicMock()
        pm = PortfolioManager(db_mock, wl_mock)
        
        # Setup data: 
        # AccountA has 10 LEG1
        # AccountB has 20 LEG1
        
        # Mock generate_g_con_id and utils
        with patch('portfolio_manager.generate_g_con_id', side_effect=lambda a,b,c,d: f"{a}-ID"), \
             patch('portfolio_manager.parse_single_leg_details', return_value={'product': 'LEG1', 'month': '', 'year': ''}):
             
             contract_mock = MagicMock()
             contract_mock.symbol = "LEG1"
             contract_mock._g_con_id = "LEG1-ID"
             contract_mock.secType = "FUT"
             
             # Call new signature: on_position_update(account, contract, pos, avg)
             pm.on_position_update("AccountA", contract_mock, 10.0, 100.0)
             pm.on_position_update("AccountB", contract_mock, 20.0, 100.0)
             
             # Reconcile (No BAGs defined, just pure legs)
             pm.reconcile({})
             
             # Verify internal storage structure
             self.assertIn("AccountA", pm.reconciled_positions)
             self.assertIn("AccountB", pm.reconciled_positions)
             
             self.assertEqual(pm.reconciled_positions["AccountA"]["LEG1-ID"], 10.0)
             self.assertEqual(pm.reconciled_positions["AccountB"]["LEG1-ID"], 20.0)
             
             # Verify DB write call structure
             # Should have called write_positions with a list containing both accounts
             self.assertTrue(db_mock.write_positions.called)
             args, _ = db_mock.write_positions.call_args
             written_list = args[0]
             
             acc_a_entry = next((x for x in written_list if x['account'] == 'AccountA'), None)
             acc_b_entry = next((x for x in written_list if x['account'] == 'AccountB'), None)
             
             self.assertIsNotNone(acc_a_entry)
             self.assertIsNotNone(acc_b_entry)
             self.assertEqual(acc_a_entry['qty'], 10.0)
             self.assertEqual(acc_b_entry['qty'], 20.0)
             
             print("Test passed: Multi-account positions stored and written correctly.")

if __name__ == "__main__":
    unittest.main()
