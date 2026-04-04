import hashlib
from typing import Dict, Any, List, Optional
import sys
import os

# Ensure ibapi can be imported
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "ibapi")))

try:
    from ibapi.contract import Contract
    from ibapi.order import Order
except ImportError:
    class Contract: pass
    class Order: pass



def generate_g_con_id(product: str, month: str, year: str, sec_type: str = "FUT", raw_fallback: str = "") -> str:
    """
    Generates a unique Global Contract ID (gConId) based on hashed contract properties.
    Normalizes Month Codes (F, G, H, J, K, M, N, Q, U, V, X, Z) to 2-digit numbers (01-12).
    """
    if sec_type != "BAG":
        # Normalize Month
        month_to_num = {
            "F": "01", "G": "02", "H": "03", "J": "04", "K": "05", "M": "06",
            "N": "07", "Q": "08", "U": "09", "V": "10", "X": "11", "Z": "12"
        }
        m = month.upper()
        norm_month = month_to_num.get(m, m.zfill(2))
        raw_str = f"{product}-{norm_month}-{year}-{sec_type}"
    else:
        raw_str = raw_fallback
        
    return hashlib.md5(raw_str.encode('utf-8')).hexdigest()

def parse_single_leg_details(symbol_str: str) -> Dict[str, Any]:
    """
    Parses a single leg symbol string like HEM6 or HE26M into its components.
    Logic:
    - Year: Last 1-2 digits.
    - Month: Next non-numeric character (F, G, H, J, K, M, N, Q, U, V, X, Z).
    - Product: Remaining prefix.
    """
    import re
    
    month_names = {
        "F": "January", "G": "February", "H": "March", "J": "April",
        "K": "May", "M": "June", "N": "July", "Q": "August",
        "U": "September", "V": "October", "X": "November", "Z": "December"
    }
    
    # 1. Extract year (1 or 2 digits at the end)
    match_year = re.search(r"(\d+)$", symbol_str)
    if not match_year:
        return {"product": symbol_str, "month": "", "year": "", "month_name": ""}
        
    year_str = match_year.group(1)
    # Convert year digit(s) to full year (assuming 20xx)
    full_year = f"20{year_str}" if len(year_str) == 2 else f"202{year_str}"
    
    # 2. Extract month (the character just before the year digits)
    prefix_before_year = symbol_str[:match_year.start()]
    if not prefix_before_year:
        return {"product": symbol_str, "month": "", "year": full_year, "month_name": ""}
        
    month_code = prefix_before_year[-1].upper()
    month_name = month_names.get(month_code, "Unknown")
    
    # 3. Extract product (everything before the month code)
    product = prefix_before_year[:-1]
    
    return {
        "product": product,
        "month": month_code,
        "month_name": month_name,
        "year": full_year
    }

def map_tick_type(tick_type_id: int) -> str:
    """
    Maps IB TickType integer to a readable string (BID, ASK, LAST, VOLUME).
    Returns 'UNKNOWN' if not one of the tracked types.
    """
    # Common Tick Types
    # 1: Bid Price, 2: Ask Price, 4: Last Price, 8: Volume
    # 0: Bid Size, 3: Ask Size, 5: Last Size
    # 66, 67, 68 are delayed counterparts
    mapping = {
        0: "BID_SIZE",
        1: "BID",
        2: "ASK",
        3: "ASK_SIZE",
        4: "LAST",
        5: "LAST_SIZE",
        8: "VOLUME",
        66: "BID",
        67: "ASK",
        68: "LAST",
        69: "BID_SIZE",
        70: "ASK_SIZE",
        71: "LAST_SIZE",
        74: "VOLUME"  # Delayed Volume
    }
    return mapping.get(tick_type_id, "UNKNOWN")

def log_ib_object(logger, prefix: str, obj):
    """
    Logs all non-private attributes of an IB object in a structured format.
    Useful for inspecting Contract and Order objects before sending them.
    """
    import inspect
    
    attrs = []
    # Get all members that are not methods/special
    for name, value in inspect.getmembers(obj):
        if not name.startswith('_') and not inspect.ismethod(value):
            # Format value if it's a list (like comboLegs)
            if isinstance(value, list) and len(value) > 0:
                value_str = "[" + ", ".join([str(v) for v in value]) + "]"
            else:
                value_str = str(value)
            attrs.append(f"{name}={value_str}")
    
    full_msg = f"{prefix}: {obj.__class__.__name__} | " + " | ".join(attrs)
    logger.info(full_msg)

def create_contract(data: Dict[str, Any]) -> Contract:
    """
    Creates an IB Contract object from a dictionary.
    Ensures both keys for expiry and localSymbol are preserved.
    """
    def s(val): return val if val is not None else ""

    contract = Contract()
    # IB expects the root symbol (product) like 'HE' for futures, 
    # not the full legacy string like 'HEV6'.
    contract.symbol = s(data.get('product', data.get('symbol', '')))
    contract.secType = s(data.get('secType', 'FUT'))
    
    contract.exchange = s(data.get('exchange', ''))
    
    contract.currency = s(data.get('currency', 'USD'))
    # Support both key names for expiry
    contract.lastTradeDateOrContractMonth = s(data.get('lastTradeDateOrContractMonth', data.get('expiry', '')))
    contract.strike = float(data.get('strike', 0.0))
    contract.right = s(data.get('right', ''))
    contract.multiplier = s(data.get('multiplier', ''))
    contract.localSymbol = s(data.get('localSymbol', ''))
    # If it's a TWS contract being populated from cache, we might have conId
    if 'conId' in data:
        contract.conId = data['conId']
    return contract

def create_order(action: str, quantity: float, order_type: str, lmt_price: float = 0.0, aux_price: float = 0.0, tif: str = None, order_ref: str = "") -> Order:
    """
    Creates an IB Order object.
    """
    order = Order()
    order.action = action
    order.totalQuantity = quantity
    order.orderType = order_type
    
    order.lmtPrice = lmt_price
    order.auxPrice = aux_price
    
    if tif:
        order.tif = tif
        
    if order_ref:
        order.orderRef = order_ref
        
    return order

def get_short_symbol(symbol: str, expiry: str) -> str:
    """
    Generates a short symbol like HEM6 (Lean Hogs Jun 2026).
    Pattern: Symbol + MonthCode + YearDigit
    """
    if not expiry or len(expiry) < 6:
        return symbol # Fallback
        
    year = expiry[:4]
    month = expiry[4:6]
    
    # Month Codes
    month_codes = {
        "01": "F", "02": "G", "03": "H", "04": "J", "05": "K", "06": "M",
        "07": "N", "08": "Q", "09": "U", "10": "V", "11": "X", "12": "Z"
    }
    
    code = month_codes.get(month, "")
    year_digit = year[-1]
    
    return f"{symbol}{code}{year_digit}"

def generate_spread_symbol_str(legs: List[Dict[str, Any]]) -> str:
    """
    Generates spread symbol string e.g. HEM6-2HEN6+HEQ6.
    """
    if not legs:
        return "UNKNOWN_SPREAD"
        
    parts = []
    first = True
    
    for leg in legs:
        symbol = leg.get('symbol', '')
        # Try different keys for expiry
        expiry = leg.get('lastTradeDateOrContractMonth', '') or leg.get('expiry', '')
        
        short_sym = get_short_symbol(symbol, expiry)
        
        ratio = int(leg.get('ratio', 1))
        action = leg.get('action', 'BUY')
        
        prefix = ""
        if action == 'BUY':
            if not first:
                prefix = "+"
            if ratio > 1:
                prefix += str(ratio)
        else: # SELL
            prefix = "-"
            if ratio > 1:
                prefix += str(ratio)
                
        parts.append(f"{prefix}{short_sym}")
        first = False
        
    return "".join(parts)


def parse_contract_symbol(symbol_str: str) -> List[Dict[str, Any]]:
    """
    Parses a symbol string (possibly multileg) into a list of leg dicts.
    Format: [Ratio]SymbolMonthYear[+/-][Ratio]SymbolMonthYear...
    Example: HEM6, HEM6-HEN6, HEM6-2HEN6+HEQ6
    """
    import re
    
    legs = []
    # Pattern to match: [+/-][Ratio]SymbolShortDate
    # ShortDate is exactly 2 characters (MonthCode + YearDigit)
    # Symbol can be 1-3 characters
    # Ratio can be multiple digits
    
    # We use a regex that looks for signs to split legs
    # But first, if no sign at start, pretend it's +
    if not symbol_str.startswith('+') and not symbol_str.startswith('-'):
        symbol_str = "+" + symbol_str
        
    # Regex: ([+-])(\d*)([A-Z0-9:]+)
    pattern = r"([+-])(\d*)([A-Z0-9:]+)"
    matches = re.finditer(pattern, symbol_str)
    
    month_to_num = {
        "F": "01", "G": "02", "H": "03", "J": "04", "K": "05", "M": "06",
        "N": "07", "Q": "08", "U": "09", "V": "10", "X": "11", "Z": "12"
    }
    
    for match in matches:
        action_char = match.group(1)
        ratio_str = match.group(2)
        full_sym = match.group(3) # e.g. HEM6, HE26M, MGC:G6...
        
        action = 'BUY' if action_char == '+' else 'SELL'
        ratio = int(ratio_str) if ratio_str else 1
        
        # Determine Symbol Components
        if ":" in full_sym:
            # Format Symbol:DateCode (legacy/explicit)
            base_sym, date_code = full_sym.split(":")
            details = parse_single_leg_details(full_sym.replace(":", "")) # Try to parse
        else:
            details = parse_single_leg_details(full_sym)
            
        product = details.get('product', '')
        month_code = details.get('month', '')
        month_name = details.get('month_name', '')
        year = details.get('year', '')
        
        # Re-derive expiry for IB compatibility
        expiry = ""
        if year and month_code:
            month_to_num = {
                "F": "01", "G": "02", "H": "03", "J": "04", "K": "05", "M": "06",
                "N": "07", "Q": "08", "U": "09", "V": "10", "X": "11", "Z": "12"
            }
            m_num = month_to_num.get(month_code, "01")
            expiry = f"{year}{m_num}"

        # Generate unique gConId
        g_con_id = generate_g_con_id(product, month_code, year, sec_type="FUT")
            
        legs.append({
            "symbol": full_sym, # Keep full symbol as reference
            "product": product,
            "month": month_code,
            "month_name": month_name,
            "year": year,
            "g_con_id": g_con_id,
            "expiry": expiry,
            "lastTradeDateOrContractMonth": expiry,
            "ratio": ratio,
            "action": action,
            "secType": "FUT",
            "exchange": "",
            "currency": "USD"
        })
        
    return legs



def clean_float(value: Any) -> Optional[float]:
    """
    Sanitizes float values for JSON serialization.
    Converts NaN/Inf to None. Handles string inputs too.
    """
    import math
    try:
        if value is None:
            return None
        f_val = float(value)
        if math.isnan(f_val) or math.isinf(f_val):
            return None
        return f_val
    except (ValueError, TypeError):
        return None

def get_bracket_reference() -> str:
    """Generates a pseudo-random unique reference for bracket groups."""
    import time
    import random
    return f"BRK_{int(time.time())}_{random.randint(100, 999)}"

def calculate_display_price(ticks: Dict[str, Any]) -> Optional[float]:
    """
    Calculates a single display price from a ticks dictionary.
    Priority: LAST > mid(BID, ASK) > BID > ASK
    """
    if not ticks:
        return None
        
    # Get values, handling both upper and lower case keys
    last = ticks.get('LAST') or ticks.get('last')
    bid = ticks.get('BID') or ticks.get('bid')
    ask = ticks.get('ASK') or ticks.get('ask')
    
    # Sanitize inputs
    last = clean_float(last)
    bid = clean_float(bid)
    ask = clean_float(ask)
    
    if last and last > 0:
        return last
        
    if bid and ask and bid > 0 and ask > 0:
        return (bid + ask) / 2
        
    if bid and bid > 0:
        return bid
        
    if ask and ask > 0:
        return ask
        
    return None

