import sys
import os
import logging
from typing import Dict, Optional, List, Any

# Ensure we can import from ibapi
# Assuming ibapi is in the parent directory of 'app'
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "ibapi")))

try:
    from ibapi.wrapper import EWrapper
    from ibapi.common import TickerId, TickAttrib
    from ibapi.contract import Contract, ContractDetails
    from ibapi.order import Order
    from ibapi.order_state import OrderState
    from ibapi.execution import Execution
    from ibapi.commission_report import CommissionReport
except ImportError as e:
    logging.error(f"Failed to import IBAPI packages: {e}")
    # We define dummy classes/types to avoid immediate crash during development if ibapi is missing
    class EWrapper: pass
    TickerId = int
    TickAttrib = object
    Contract = object
    ContractDetails = object
    Order = object
    OrderState = object
    Execution = object
    CommissionReport = object

from logger import LoggerSetup
from utils import map_tick_type

class IBWrapper(EWrapper):
    """
    Callback wrapper for the Interactive Brokers API.
    Handles responses from the TWS/Gateway.
    """
    
    def __init__(self):
        super().__init__()
        self.logger = LoggerSetup.get_logger("IBWrapper")
        
        # State variables
        self.next_order_id: Optional[int] = None
        self.account_summary: Dict[str, Dict[str, str]] = {}
        # Stores {orderId: {'order': Order, 'contract': Contract, 'state': OrderState}}
        self.open_orders: Dict[int, Dict] = {}
        self.order_statuses: Dict[int, Any] = {}
        self.contract_details: Dict[int, ContractDetails] = {}
        
        # This will hold the reference to the main app to send data to Influx
        # In a real app, use a proper signal/slot or queue mechanism
        self.app_context = None 

    def error(self, *args, **kwargs):
        """
        Called when an error or message occurs.
        IBAPI can sometimes call this with varying number of arguments (e.g., just an Exception, or reqId+errorCode+errorString).
        """
        reqId = kwargs.get('reqId', args[0] if len(args) > 0 else -1)
        errorCode = kwargs.get('errorCode', args[1] if len(args) > 1 else -1)
        errorString = kwargs.get('errorString', args[2] if len(args) > 2 else "")
        advancedOrderRejectJson = kwargs.get('advancedOrderRejectJson', args[3] if len(args) > 3 else "")

        if len(args) == 1 and isinstance(args[0], Exception):
            # Signature: error(Exception)
            errorString = str(args[0])
            errorCode = -1
            reqId = -1

        # Specific filtering of common non-critical codes (2104, 2106, 2158)
        if errorCode in [2104, 2106, 2158]:
            self.logger.info(f"[RECEIVE] IB Notification. Id: {reqId}, Code: {errorCode}, Msg: {errorString}")
        else:
            self.logger.error(f"[RECEIVE] IB Error. Id: {reqId}, Code: {errorCode}, Msg: {errorString}")
            
        # Forward error to connector to handle blocked requests
        if self.app_context and hasattr(self.app_context, 'on_error'):
            self.app_context.on_error(reqId, errorCode, errorString)

    def nextValidId(self, orderId: int):
        """
        Called after the connection is established. Provides the next valid Order ID.
        """
        self.logger.info(f"Connection Established. Next Valid Order ID: {orderId}")
        self.next_order_id = orderId
        # Notify the app that we are ready
        if self.app_context:
            self.app_context.on_connected()

    def tickPrice(self, reqId: int, tickType: int, price: float, attrib: TickAttrib):
        """
        Market data price tick.
        """

        self.logger.debug(f"[RECEIVE] tickPrice. ReqId: {reqId}, Type: {tickType}, Price: {price}")
        if self.app_context:
            self.app_context.handle_tick_price(reqId, tickType, price)

    def tickSize(self, reqId: TickerId, tickType: int, size: int):
        """
        Market data tick size callback.
        tickType: 0=Bid Size, 3=Ask Size, 5=Last Size, 8=Volume
        """
        # Skip if this ID is flagged as NoPersist (e.g. BAG)
        if self.app_context and reqId in self.app_context.non_persisted_req_ids:
            return

        self.logger.debug(f"Tick Size. ReqId: {reqId}, Type: {tickType}, Size: {size}")
        if self.app_context:
            self.app_context.handle_tick_size(reqId, tickType, size)

    def accountSummary(self, reqId: int, account: str, tag: str, value: str, currency: str):
        """
        Callback for reqAccountSummary.
        """
        self.logger.info(f"Account Summary. Account: {account}, Tag: {tag}, Value: {value}")
        if account not in self.account_summary:
            self.account_summary[account] = {}
        self.account_summary[account][tag] = value
        
    def accountSummaryEnd(self, reqId: int):
        self.logger.info(f"Account Summary Received for ReqId: {reqId}")
        if self.app_context:
            self.app_context.handle_account_summary_end(reqId)

    def orderStatus(self, orderId: int, status: str, filled: float, remaining: float, 
                    avgFillPrice: float, permId: int, parentId: int, lastFillPrice: float, 
                    clientId: int, whyHeld: str, mktCapPrice: float):
        """
        Updates the status of an order.
        """
        self.logger.info(f"[RECEIVE] orderStatus. Id: {orderId}, Status: {status}, Filled: {filled}, Remaining: {remaining}, AvgFill: {avgFillPrice}, LastFill: {lastFillPrice}, ParentId: {parentId}")
        
        self.order_statuses[orderId] = {
            "status": status,
            "filled": filled,
            "remaining": remaining,
            "lastFillPrice": lastFillPrice,
            "avgFillPrice": avgFillPrice
        }
        
        if self.app_context:
            self.app_context.handle_order_status(
                orderId, status, filled, remaining, avgFillPrice, permId, parentId, lastFillPrice, clientId
            )

    def openOrder(self, orderId: int, contract: Contract, order: Order, orderState: OrderState):
        """
        Called for all open orders.
        """
        price_str = ""
        if order.orderType == "LMT":
            price_str = f", LmtPrice: {order.lmtPrice}"
        elif order.orderType == "STP":
            price_str = f", AuxPrice: {order.auxPrice}"
            
        self.logger.info(f"[RECEIVE] openOrder. Id: {orderId}, Symbol: {contract.symbol}, Action: {order.action}, Type: {order.orderType}{price_str}, Status: {orderState.status}")
        
        self.open_orders[orderId] = {
            'order': order,
            'contract': contract,
            'state': orderState
        }
        
        # Dynamic Resolution for BAG legs
        if contract.secType == "BAG" and contract.comboLegs:
            if self.app_context:
                for leg in contract.comboLegs:
                    self.app_context.subscribe_contract_by_conid(leg.conId)
        
        # Dynamic Subscription: Ensure we are watching this contract
        if self.app_context:
            self.app_context.handle_open_order(orderId, contract, order, orderState)
        
        
    def openOrderEnd(self):
        """
        Called when all open orders have been received.
        """
        self.logger.info("[RECEIVE] openOrderEnd. Open Orders Sync Complete.")
        if self.app_context:
            self.app_context.on_open_orders_end()
            
    def execDetails(self, reqId: int, contract: Contract, execution: Execution):
        """
        Called when an execution occurs.
        """
        self.logger.info(f"Execution Details. ReqId: {reqId}, Symbol: {contract.symbol}, Side: {execution.side}, Shares: {execution.shares}, Price: {execution.price}")
        if self.app_context:
            self.app_context.handle_execution(reqId, contract, execution)

    def execDetailsEnd(self, reqId: int):
        """
        Called when all executions have been delivered.
        """
        self.logger.info(f"Execution Details End. ReqId: {reqId}")

    def commissionReport(self, commissionReport: CommissionReport):
        """
        Called when a commission report is received.
        """
        self.logger.info(f"Commission Report. ExecId: {commissionReport.execId}, Commission: {commissionReport.commission} {commissionReport.currency}, RealizedPNL: {commissionReport.realizedPNL}")
        if self.app_context:
            self.app_context.handle_commission_report(commissionReport)

    def contractDetails(self, reqId: int, contractDetails: ContractDetails):
        """
        Receives contract details.
        """
        self.logger.info(f"Contract Details Received. ReqId: {reqId}, Symbol: {contractDetails.contract.symbol}, LocalSymbol: {contractDetails.contract.localSymbol}, Multiplier: {contractDetails.contract.multiplier}, ConId: {contractDetails.contract.conId}")
        self.contract_details[reqId] = contractDetails
        
        # Populate Symbol Cache in App Context
        # We store True/Local Symbol to help naming
        if self.app_context:
            # Prefer localSymbol (e.g. "ESM6") over symbol ("ES") for Futures
            name = contractDetails.contract.localSymbol if contractDetails.contract.localSymbol else contractDetails.contract.symbol
            # Store rich details for API usage
            # Store rich details for API usage
            # We store both localSymbol and symbol (underlying) to allow robust matching
            self.app_context.symbol_cache[contractDetails.contract.conId] = {
                "localSymbol": contractDetails.contract.localSymbol,
                "symbol": contractDetails.contract.symbol,
                "expiry": contractDetails.contract.lastTradeDateOrContractMonth,
                "secType": contractDetails.contract.secType,
                "currency": contractDetails.contract.currency,
                "exchange": contractDetails.contract.exchange,
                "multiplier": contractDetails.contract.multiplier,
                "primaryExchange": contractDetails.contract.primaryExchange
            }
            # print(f"DEBUG: Cache Updated for {contractDetails.contract.conId}: Local='{contractDetails.contract.localSymbol}'")
            
            # Populate Product Exchange Cache for instant future resolution
            if contractDetails.contract.symbol and contractDetails.contract.exchange:
                 self.app_context.contract_service.product_exchange_cache[contractDetails.contract.symbol] = contractDetails.contract.exchange

            # Re-trigger pending subscriptions and pricing (BAGs might be waiting for this leg)
            self.app_context._check_pending_single_legs()
            self.app_context._check_pending_bags()
            self.app_context._update_dependent_bags_by_conid(contractDetails.contract.conId)

    def contractDetailsEnd(self, reqId: int):
        self.logger.debug(f"Contract Details End. ReqId: {reqId}")
        if self.app_context:
            self.app_context.on_contract_details_end(reqId)

    def position(self, account: str, contract: Contract, position: float, avgCost: float):
        """
        Callback for position updates.
        """
        self.logger.info(f"[RECEIVE] position. Account: {account}, Symbol: {contract.symbol}, LocalSymbol: {contract.localSymbol}, Pos: {position}, AvgCost: {avgCost}")
        if self.app_context:
            self.app_context.handle_position(account, contract, position, avgCost)

    def positionEnd(self):
        """
        End of position updates.
        """
        self.logger.info("[RECEIVE] positionEnd")
        if self.app_context:
            self.app_context.handle_position_end()

    def connectionClosed(self):
        """
        Called when connection is closed.
        """
        self.logger.warning("IB Connection Closed.")
        if self.app_context:
            self.app_context.on_disconnected()



