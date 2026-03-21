from pydantic import BaseModel, Field
from typing import Optional, List, Dict
from datetime import datetime

class IBContract(BaseModel):
    """
    Represents a financial contract (Future, Stock, Spread, etc.).
    Uses specific naming conventions to match IB but with clearer variable names.
    """
    symbol: str = Field(..., description="The symbol of the contract (e.g., ES, CL)")
    sec_type: str = Field(..., description="Security Type (FUT, STK, BAG for spreads)")
    last_trade_date_or_contract_month: str = Field("", description="Expiration date (YYYYMM or YYYYMMDD)")
    strike: float = Field(0.0, description="Strike price for Options")
    right: str = Field("", description="Right (Put/Call) for Options")
    multiplier: str = Field("", description="Multiplier for the contract (e.g., 50 for ES)")
    exchange: str = Field("CME", description="Exchange where the contract is traded")
    currency: str = Field("USD", description="Currency of the contract")
    local_symbol: str = Field("", description="Local symbol on the exchange")
    trading_class: str = Field("", description="Trading class name")
    con_id: int = Field(0, description="Interactive Brokers Unique Contract Identifier")
    g_con_id: str = Field("", description="Our Global Unique Contract Identifier")
    
    # New refined fields
    product: str = Field("", description="Product code (e.g., HE for Lean Hogs)")
    month: str = Field("", description="Contract month name (e.g., June)")
    year: str = Field("", description="Contract year (e.g., 2026)")

class TickData(BaseModel):
    """
    Represents a market data tick (Price or Volume).
    """
    time: datetime = Field(default_factory=datetime.now, description="Timestamp of the tick")
    contract_g_con_id: str = Field(..., description="Global Contract ID associated with this tick")
    tick_type: str = Field(..., description="Type of tick (BID, ASK, LAST, VOLUME)")
    price: Optional[float] = Field(None, description="Price value")
    size: Optional[float] = Field(None, description="Size/Volume value")

class AccumulatedTickRecord(BaseModel):
    """
    Represents a complete record for InfluxDB containing Bid, Ask, Last, and Sizes.
    """
    time: datetime = Field(..., description="Timestamp")
    contract_g_con_id: str = Field(..., description="Global Contract ID")
    bid_price: Optional[float] = None
    ask_price: Optional[float] = None
    last_price: Optional[float] = None
    bid_size: Optional[float] = None
    ask_size: Optional[float] = None
    last_size: Optional[float] = None
    volume: Optional[float] = None # Day volume
    
    def is_complete(self) -> bool:
        """Returns True if all required fields are present."""
        return all(x is not None for x in [
            self.bid_price, self.ask_price, self.last_price, 
            self.bid_size, self.ask_size, self.last_size
        ])


class IBOrder(BaseModel):
    """
    Represents an Order sent to or received from IB.
    """
    order_id: int = Field(..., description="Unique Order ID")
    client_id: int = Field(..., description="Client ID who placed the order")
    perm_id: int = Field(..., description="Permanent Order ID from IB")
    action: str = Field(..., description="BUY or SELL")
    total_quantity: float = Field(..., description="Quantity of contracts")
    order_type: str = Field(..., description="Order Type (LMT, MKT, STP)")
    lmt_price: float = Field(0.0, description="Limit Price")
    aux_price: float = Field(0.0, description="Stop Price")
    status: str = Field("Unknown", description="Current Order Status")
    filled: float = Field(0.0, description="Amount filled")
    remaining: float = Field(0.0, description="Amount remaining")
    avg_fill_price: float = Field(0.0, description="Average fill price")
    last_fill_price: float = Field(0.0, description="Price of the last fill")
    parent_id: int = Field(0, description="ID of parent order (for brackets)")
    
class AccountSummary(BaseModel):
    """
    Represents a summary of the account values.
    """
    account_code: str
    net_liquidation: float = 0.0
    total_cash_value: float = 0.0
    settled_cash: float = 0.0
    accrued_cash: float = 0.0
    buying_power: float = 0.0
    equity_with_loan_value: float = 0.0
    previous_equity_with_loan_value: float = 0.0
    gross_position_value: float = 0.0
    req_tequity: float = 0.0
    req_tmargin: float = 0.0
    available_funds: float = 0.0
    excess_liquidity: float = 0.0
    cushion: float = 0.0
    full_init_margin_req: float = 0.0
    full_maint_margin_req: float = 0.0
    full_available_funds: float = 0.0
    full_excess_liquidity: float = 0.0
