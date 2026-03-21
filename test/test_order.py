import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'src')))

from ib_connector import IBConnector
from unittest.mock import MagicMock

class Contract:
    def __init__(self):
        self.symbol = "AAPL"
        self.secType = "STK"
        self.currency = "USD"
        self.exchange = "SMART"
        self.lastTradeDateOrContractMonth = ""
        self.localSymbol = ""
class Order:
    def __init__(self):
        self.account = "U123"
        self.action = "BUY"
        self.totalQuantity = 100.0
        self.orderType = "LMT"
        self.tif = "DAY"
        self.lmtPrice = 150.0
        self.auxPrice = 0.0
        self.permId = 123456

c = IBConnector()
c.open_orders_synced = True

cont = Contract()
ord = Order()

c.wrapper = MagicMock()
c.wrapper.account_summary = {}
c.order_service = MagicMock()
c.wrapper.open_orders = {1: {'order': ord, 'contract': cont}}
c.wrapper.order_statuses = {1: {'status': "Submitted", 'filled': 0.0, 'remaining': 100.0, 'lastFillPrice': 0.0}}

# Mocking the delegated call
c.order_service.check_and_write_order_status.side_effect = [ (True, {"status": "Submitted"}), (False, {}) ]

has_changed, delta = c._check_and_write_order_status(1)
print(f"First call: has_changed={has_changed}")

has_changed2, delta2 = c._check_and_write_order_status(1)
print(f"Second call: has_changed={has_changed2}")
