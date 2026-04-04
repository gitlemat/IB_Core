
import sys
import os
from typing import Dict, Any

# Add src to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "src")))

try:
    from utils import parse_contract_symbol, create_contract
except ImportError as e:
    print(f"Import Error: {e}")
    sys.exit(1)

def test_hev6_logic():
    print("Testing HEV6 resolution logic...")
    
    symbol_str = "HEV6"
    legs = parse_contract_symbol(symbol_str)
    print(f"Parsed Legs: {legs}")
    
    if not legs:
        print("FAIL: No legs parsed")
        return

    contract_data = legs[0].copy()
    contract = create_contract(contract_data)
    
    print(f"Created Contract: Symbol='{contract.symbol}', Expiry='{contract.lastTradeDateOrContractMonth}', SecType='{contract.secType}', Exchange='{contract.exchange}'")
    
    if contract.symbol == "HE" and contract.lastTradeDateOrContractMonth == "202610":
        print("PASS: Contract correctly structured for IB.")
    else:
        print(f"FAIL: Contract malformed. Symbol='{contract.symbol}', Expiry='{contract.lastTradeDateOrContractMonth}'")

if __name__ == "__main__":
    test_hev6_logic()
