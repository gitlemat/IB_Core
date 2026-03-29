from typing import Dict, List, Any, Optional
from collections import namedtuple
import math
from logger import LoggerSetup
from db_client import DatabaseClient
from watchlist import WatchlistManager
from utils import generate_g_con_id, parse_single_leg_details

Position = namedtuple('Position', ['account_id', 'g_con_id', 'position', 'avg_cost'])

class PortfolioManager:
    """
    Manages portfolio positions and reconciles them into Strategies (Spreads/Butterflies).
    """

    def __init__(self, db_client: DatabaseClient, watchlist_manager: WatchlistManager):
        self.logger = LoggerSetup.get_logger("PortfolioManager")
        self.db_client = db_client
        self.watchlist_manager = watchlist_manager
        
        # Raw positions from IB: map[account][gConId] -> quantity
        self.raw_positions: Dict[str, Dict[str, float]] = {}
        self.raw_contracts: Dict[str, Any] = {} # gConId -> Contract used for display

        # Reconciled positions (after collapsing legs into strategies)
        # map[account][gConId] -> quantity
        self.reconciled_positions: Dict[str, Dict[str, float]] = {}
        
        # Average costs
        self.raw_avg_costs: Dict[str, Dict[str, float]] = {} # account -> gConId -> float
        self.reconciled_avg_costs: Dict[str, Dict[str, float]] = {} # account -> gConId -> float
        
        # State tracking for DB optimization
        self.last_portfolio_state: Dict[tuple, float] = {} # (Account, Symbol) -> Quantity
        self.initial_state_loaded = False

    def on_position_update(self, account: str, contract: Any, position: float, avg_cost: float, connector=None):
        """
        Callback when a position update is received.
        """
        g_con_id = None
        # 1. Prefer the stable ID attached to the contract object
        if hasattr(contract, '_g_con_id'):
            g_con_id = contract._g_con_id
        elif connector:
            # Use centralized service for matching IDs
            g_con_id = connector.contract_service.get_g_con_id(contract)
        else:
            # Fallback to legacy generation (single-legs only)
            details = parse_single_leg_details(contract.localSymbol or contract.symbol)
            g_con_id = generate_g_con_id(
                details['product'], 
                details['month'], 
                details['year'], 
                contract.secType
            )
        
        if account not in self.raw_positions:
            self.raw_positions[account] = {}
        if account not in self.raw_avg_costs:
            self.raw_avg_costs[account] = {}

        if position == 0:
            if g_con_id in self.raw_positions[account]:
                del self.raw_positions[account][g_con_id]
            if g_con_id in self.raw_avg_costs[account]:
                del self.raw_avg_costs[account][g_con_id]
        else:
            self.raw_positions[account][g_con_id] = float(position)
            self.raw_contracts[g_con_id] = contract
            self.raw_avg_costs[account][g_con_id] = float(avg_cost)

    def reconcile(self, active_subscriptions: Dict[str, Any]):
        """
        Reconciles raw positions into consolidated Strategies (Spreads/Butterflies).
        Writes the result to InfluxDB and updates internal reconciled_positions map.
        """
        self.logger.debug("Reconciling Portfolio...")
        
        reconciled_positions_list_all = [] # List of dicts for DB
        self.reconciled_positions = {} # Map account -> gConId -> qty for API
        self.reconciled_avg_costs = {}

        # 1. Get Strategy Definitions from the active registry
        strategies = []
        for g_con_id, data in active_subscriptions.items():
            contract = data.get('contract')
            legs = data.get('legs') # Metadata we stored in subscribe_contract
            
            if contract and contract.secType == 'BAG' and legs:
                strategies.append((g_con_id, data))

        # Iterate over EACH account
        for account, account_positions in self.raw_positions.items():
            self.reconciled_positions[account] = {}
            self.reconciled_avg_costs[account] = {}
            
            # Working copy of positions to "consume"
            remaining_positions = account_positions.copy()
            
            for strat_g_con_id, data in strategies:
                strategy = data['contract']
                legs = data['legs']
                    
                # Calculate max possible strategies
                possible_counts = []
                leg_ids = [] # Cache leg IDs to avoid redundant generation
                
                for leg in legs:
                    # Prefer existing ID in meta if available
                    leg_g_con_id = leg.get('g_con_id') or leg.get('gConId')
                    if not leg_g_con_id:
                        leg_g_con_id = generate_g_con_id(
                            leg.get('product', ''),
                            leg.get('month', ''),
                            leg.get('year', ''),
                            leg.get('secType', 'FUT')
                        )
                    leg_ids.append(leg_g_con_id)
                    
                    required_qty = leg.get('ratio', 1)
                    required_action = leg.get('action', 'BUY')
                    current_qty = remaining_positions.get(leg_g_con_id, 0)
                    
                    if required_action == 'BUY':
                        if current_qty > 0:
                            possible_counts.append(int(current_qty // required_qty))
                        else:
                            possible_counts.append(0)
                    else: # SELL
                        if current_qty < 0:
                            possible_counts.append(int(abs(current_qty) // required_qty))
                        else:
                            possible_counts.append(0)
                
                strategy_qty = min(possible_counts) if possible_counts else 0
                
                if strategy_qty > 0:
                    # Record for DB
                    reconciled_positions_list_all.append({
                        "symbol": strategy.symbol if hasattr(strategy, 'symbol') else 'Unknown Strategy',
                        "qty": strategy_qty,
                        "type": "STRATEGY",
                        "account": account
                    })
                    # Record for internal map
                    self.reconciled_positions[account][strat_g_con_id] = float(strategy_qty)
                    
                    # Calculate synthetic average cost for the strategy
                    # Strategy Avg Cost = Sum(BuyLeg_Avg * Ratio) - Sum(SellLeg_Avg * Ratio)
                    strat_avg_cost = 0.0
                    for i, leg in enumerate(legs):
                        l_id = leg_ids[i]
                        # Access cost per account
                        l_avg = self.raw_avg_costs[account].get(l_id, 0.0)
                        l_ratio = float(leg.get('ratio', 1))
                        l_action = leg.get('action', 'BUY')
                        
                        if l_action == 'BUY':
                            strat_avg_cost += l_avg * l_ratio
                        else:
                            strat_avg_cost -= l_avg * l_ratio
                    self.reconciled_avg_costs[account][strat_g_con_id] = strat_avg_cost
                    
                    # Consume Legs
                    for i, leg in enumerate(legs):
                        leg_g_con_id = leg_ids[i]
                        ratio = leg.get('ratio', 1)
                        action = leg.get('action', 'BUY')
                        
                        if action == 'BUY':
                            remaining_positions[leg_g_con_id] -= (strategy_qty * ratio)
                        else:
                            remaining_positions[leg_g_con_id] += (strategy_qty * ratio)
                            
            # 2. Add remaining unallocated legs
            for g_con_id, qty in remaining_positions.items():
                if abs(qty) > 0.0001: # Use epsilon for float comparison
                    contract = self.raw_contracts.get(g_con_id)
                    symbol = contract.symbol if contract else g_con_id
                    
                    # New Logic: We only care about this LEG if it wasn't fully consumed
                    # by a Reconciled Position? 
                    # The 'remaining_positions' logic already subtracts the consumed parts.
                    # So 'qty' here IS the unallocated part.
                    # User request: "The positions of type 'LEG' should not be included if they're part of a reconciled position."
                    # This implies: if I had 10 legs and used 10 for a Strategy, remaining is 0.
                    # We write 0 if previously non-zero. 
                    
                    reconciled_positions_list_all.append({
                        "symbol": symbol,
                        "qty": qty,
                        "type": "LEG",
                        "account": account
                    })
                    # Record for internal map
                    self.reconciled_positions[account][g_con_id] = float(qty)
                    self.reconciled_avg_costs[account][g_con_id] = self.raw_avg_costs[account].get(g_con_id, 0.0)
                    
            # Change type Strategy -> RECONCILED for strategies (handled in loop above via type substitution?)
            # Wait, the 'strategies' loop above (lines 135-144) sets 'type': 'Strategy'.
            # We must fix that too.
            
        # --- WRITE LOGIC (Deduplication + Graceful Close) ---
        
        # 1. Fetch Last State (Lazy Load)
        if not self.initial_state_loaded:
             self.last_portfolio_state = self.db_client.get_last_portfolio_state()
             self.initial_state_loaded = True
             self.logger.info(f"Loaded {len(self.last_portfolio_state)} positions from DB history.")

        # 2. Build Current State Dict: (Account, Symbol) -> {Qty, Type}
        # We process 'reconciled_positions_list_all' which contains current non-zero positions.
        # Note: We need to handle the 'RECONCILED' type change here or in the loop above.
        
        updates_to_write = []
        current_keys = set()
        
        for p in reconciled_positions_list_all:
            acc = p['account']
            sym = p['symbol']
            qty = p['qty']
            typ = p['type']
            
            # User Request: "Change the type Strategy to type RECONCILED"
            if typ == 'Strategy': typ = 'RECONCILED'
            p['type'] = typ # Update dict for writing
            
            key = (acc, sym)
            current_keys.add(key)
            
            # Check Change
            last_qty = float(self.last_portfolio_state.get(key, 0.0))
            qty = float(qty)
            
            if abs(qty - last_qty) > 0.0001:
                # Changed!
                updates_to_write.append(p)
                self.last_portfolio_state[key] = qty # Update Cache
        
        # 3. Check for Disappeared Positions (Graceful Close)
        # Any key in last_state that is NOT in current (meaning qty is now 0)
        # AND last_qty was != 0, needs a "0" write to close it in Influx.
        
        for key, last_qty in list(self.last_portfolio_state.items()):
            if key not in current_keys:
                if abs(last_qty) > 0.0001:
                    # It disappeared (became 0)
                    acc, sym = key
                    # We assume type is LEG or RECONCILED? We don't track type in simplified state dict
                    # Just picking LEG as default for close, type matters less for 0 qty
                    # But ideally we should match previous type. 
                    # State dict in DBClient returns only Qty.
                    # We'll rely on 'LEG' as safe default or 'RECONCILED' if it looks complex?
                    # Let's say 'LEG' for closure.
                    
                    updates_to_write.append({
                        "symbol": sym,
                        "qty": 0.0,
                        "type": "LEG", # or previous type if we had it
                        "account": acc
                    })
                    self.last_portfolio_state[key] = 0.0 # Update Cache to 0
        
        # 4. Write Updates
        if updates_to_write:
            self.logger.info(f"Writing {len(updates_to_write)} portfolio updates to InfluxDB.")
            self.db_client.write_positions(updates_to_write) # A dia de hoy esta funcio no hace nada.
        else:
            self.logger.debug("No portfolio changes detected.")

    def get_positions(self, g_con_id: Optional[str] = None) -> List[Position]:
        """
        Returns a list of Position objects for the given g_con_id (or all if None).
        Uses reconciled positions.
        """
        results = []
        for account, positions in self.reconciled_positions.items():
            for gid, qty in positions.items():
                if g_con_id and gid != g_con_id:
                    continue
                    
                avg = self.reconciled_avg_costs.get(account, {}).get(gid, 0.0)
                results.append(Position(account, gid, qty, avg))
        return results

