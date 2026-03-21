import sys
import os
import unittest
from unittest.mock import MagicMock, patch

# Mock LoggerSetup before importing IBWrapper
sys.modules['logger'] = MagicMock()
sys.modules['utils'] = MagicMock()

# Add src to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "src")))

from ib_wrapper import IBWrapper

class TestIBWrapperAccounts(unittest.TestCase):
    def test_account_summary_multiple_accounts(self):
        # We need to mock LoggerSetup.get_logger specifically if it's called in __init__
        with patch('ib_wrapper.LoggerSetup') as mock_logger_setup:
            mock_logger_setup.get_logger.return_value = MagicMock()
            
            wrapper = IBWrapper()
            
            # Simulate callbacks for Account A
            wrapper.accountSummary(1001, "AccountA", "NetLiquidation", "100000", "USD")
            wrapper.accountSummary(1001, "AccountA", "TotalCashValue", "50000", "USD")
            
            # Simulate callbacks for Account B
            wrapper.accountSummary(1001, "AccountB", "NetLiquidation", "200000", "USD")
            wrapper.accountSummary(1001, "AccountB", "TotalCashValue", "80000", "USD")
            
            # Verify storage
            self.assertIn("AccountA", wrapper.account_summary)
            self.assertIn("AccountB", wrapper.account_summary)
            
            self.assertEqual(wrapper.account_summary["AccountA"]["NetLiquidation"], "100000")
            self.assertEqual(wrapper.account_summary["AccountA"]["TotalCashValue"], "50000")
            
            self.assertEqual(wrapper.account_summary["AccountB"]["NetLiquidation"], "200000")
            self.assertEqual(wrapper.account_summary["AccountB"]["TotalCashValue"], "80000")
            
            print("Test passed: Multiple accounts handled correctly in account_summary.")

if __name__ == "__main__":
    unittest.main()
