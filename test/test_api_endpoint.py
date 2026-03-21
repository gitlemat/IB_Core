import sys
import os
import asyncio
import unittest
from unittest.mock import MagicMock

# Mock dependencies before importing api
sys.modules['logger'] = MagicMock()
sys.modules['utils'] = MagicMock()
sys.modules['config'] = MagicMock()
sys.modules['models'] = MagicMock()
sys.modules['ib_connector'] = MagicMock()

# Add src to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "src")))

from api import get_account_detail
from fastapi import HTTPException

class TestApiEndpoint(unittest.IsolatedAsyncioTestCase):
    async def test_get_account_detail(self):
        # Mock Request and Connector
        mock_request = MagicMock()
        mock_connector = MagicMock()
        mock_request.app.state.ib_connector = mock_connector
        
        # Setup data
        mock_connector.wrapper.account_summary = {
            "AccountA": {"NetLiquidation": "100"},
            "AccountB": {"NetLiquidation": "200"}
        }
        
        # Test success
        print("Testing successful retrieval for AccountA...")
        result = await get_account_detail("AccountA", mock_request)
        self.assertEqual(result, {"NetLiquidation": "100"})
        print("Success.")
        
        # Test failure (404)
        print("Testing 404 for missing AccountC...")
        with self.assertRaises(HTTPException) as cm:
            await get_account_detail("AccountC", mock_request)
        self.assertEqual(cm.exception.status_code, 404)
        print("Success.")
        
        print("Test passed: Endpoint logic correct.")

if __name__ == "__main__":
    unittest.main()
