import sys
import os

# Add project root to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), ".")))

from IB_Core.src.utils import parse_contract_symbol, generate_g_con_id, parse_single_leg_details

def test_parsing():
    test_symbols = [
        "HEM6",     # Product: HE, Month: M (June), Year: 2026
        "HEN26",    # Product: HE, Month: N (July), Year: 2026
        "HE26M",    # (Assuming this format might exist)
        "ESZ25",    # Product: ES, Month: Z (Dec), Year: 2025
        "+HEM6-HEN6", # BAG
    ]
    
    for sym in test_symbols:
        print(f"\nTesting symbol: {sym}")
        legs = parse_contract_symbol(sym)
        for i, leg in enumerate(legs):
            print(f"  Leg {i+1}:")
            print(f"    Product: {leg['product']}")
            print(f"    Month: {leg['month']} ({leg['month_name']})")
            print(f"    Year: {leg['year']}")
            print(f"    gConId: {leg['g_con_id']}")
            print(f"    Expiry: {leg['expiry']}")

if __name__ == "__main__":
    test_parsing()
