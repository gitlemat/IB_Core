import sys
import os
import asyncio
import unittest
from unittest.mock import MagicMock

# Mock dependencies
sys.modules['logger'] = MagicMock()
sys.modules['utils'] = MagicMock()
sys.modules['config'] = MagicMock()
sys.modules['models'] = MagicMock()
sys.modules['ib_connector'] = MagicMock()

# Add src to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "src")))

from api import list_unique_contracts
from fastapi import HTTPException

# Need to mock clean_float in api.py context
def mock_clean_float(x): return x
import api
api.clean_float = mock_clean_float

class TestApiContracts(unittest.IsolatedAsyncioTestCase):
    async def test_list_unique_contracts_filtering(self):
        mock_request = MagicMock()
        mock_connector = MagicMock()
        mock_request.app.state.ib_connector = mock_connector
        
        # Setup Data
        # Contract 1: Held by AccountA
        # Contract 2: Held by AccountB
        
        c1 = MagicMock()
        c1.symbol = "C1"
        c1.secType = "STK"
        c1.currency = "USD"
        
        c2 = MagicMock()
        c2.symbol = "C2"
        c2.secType = "STK"
        c2.currency = "USD"
        
        mock_connector.active_subscriptions = {
            "ID1": {"contract": c1},
            "ID2": {"contract": c2}
        }
        
        # Portfolio Manager positions
        mock_connector.portfolio_manager.reconciled_positions = {
            "AccountA": {"ID1": 10.0},
            "AccountB": {"ID2": 20.0}
        }
        mock_connector.portfolio_manager.reconciled_avg_costs = {
            "AccountA": {"ID1": 100.0},
            "AccountB": {"ID2": 200.0}
        }
        
        mock_connector.watchlist_manager.get_contracts.return_value = []
        mock_connector.order_id_to_g_con_id = {}
        # Mock database client latest prices
        mock_connector.db_client.latest_prices.get.return_value = {}

        # Test 1: Query for AccountA -> Should only return C1
        res_a = await list_unique_contracts(mock_request, accountId="AccountA")
        print(f"Res A: {len(res_a)}")
        self.assertEqual(len(res_a), 1)
        self.assertEqual(res_a[0]['gConId'], "ID1")
        self.assertEqual(res_a[0]['positions'][0]['accountId'], "AccountA")
        
        # Test 2: Query for ALL -> Should return both
        res_all = await list_unique_contracts(mock_request, accountId="ALL")
        print(f"Res ALL: {len(res_all)}")
        self.assertEqual(len(res_all), 2)
        
        # Test 3: Query for AccountB -> Should only return C2
        res_b = await list_unique_contracts(mock_request, accountId="AccountB")
        self.assertEqual(len(res_b), 1)
        self.assertEqual(res_b[0]['gConId'], "ID2")

        print("Test passed: API filtering logic correct.")

if __name__ == "__main__":
    unittest.main()
