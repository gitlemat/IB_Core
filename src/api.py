from fastapi import APIRouter, Request, HTTPException, Depends, Body, Query, WebSocket, WebSocketDisconnect
from dotenv import dotenv_values, set_key
from typing import List, Dict, Any, Optional
from models import IBContract
from ib_connector import IBConnector
from config import Config
from utils import (
    create_contract, clean_float, 
    parse_single_leg_details, parse_contract_symbol,
    calculate_display_price
)
from connection_manager import ConnectionManager
import pandas as pd
import json

router = APIRouter(prefix="/restAPI")
manager = ConnectionManager()

def get_connector(request: Request) -> IBConnector:
    """Helper to retrieve the IBConnector instance from IB_Core state."""
    connector = request.app.state.ib_connector
    if connector.ws_manager is None:
        connector.set_ws_manager(manager)
    return connector

async def check_ready(request: Request):
    """Dependency to check if IBConnector is ready."""
    connector = get_connector(request)
    if getattr(connector, 'is_ready', False):
        return connector
    raise HTTPException(status_code=503, detail="System initializing. Please wait.")

@router.get("/System/Ready", summary="IBCore listo para recibir peticiones")
async def system_ready(request: Request):
    """Devuelve si el sistema está listo para recibir peticiones."""
    connector = get_connector(request)
    return {"ready": getattr(connector, 'is_ready', False)}

@router.get("/System/Health", summary="Core Service Connectivity Health")
async def system_health(request: Request):
    """Devuelve los indicadores de estado del sistema."""
    connector = get_connector(request)
    is_ready = getattr(connector, 'is_ready', False)
    is_connected = getattr(connector, 'connected', False)
    started = getattr(connector, 'started', False)
    
    # Mode from config
    account_mode = "PAPER" if Config.is_paper_trading() else "LIVE"
    
    if is_ready:
        status = "ready"
        details = "Connected to TWS and nextValidId provisioned."
    elif started:
        status = "connecting"
        details = "Starting up or waiting for Interactive Brokers connection."
    else:
        status = "error"
        details = "IB_Core engine is stopped or disconnected."
        
    return {
        "status": status,
        "is_ready": is_ready,
        "is_connected": is_connected,
        "account_mode": account_mode,
        "details": details
    }

# --- Environment Configuration ---

@router.get("/Config", summary="Obtiene Config de IBCore")
async def get_config():
    """Obtiene los parámetros actuales de .env."""
    from config import _env_path
    if not _env_path:
        raise HTTPException(status_code=404, detail=".env file not found")
    return dotenv_values(_env_path)

@router.post("/Config", summary="Actualiza Config de IBCore")
async def update_config(payload: Dict[str, Any] = Body(...)):
    """Actualiza los parámetros de .env sin destruir comentarios/formato."""
    from config import _env_path
    if not _env_path:
        raise HTTPException(status_code=404, detail=".env file not found")
    
    for key, value in payload.items():
        set_key(_env_path, key, str(value))
        
    return {"status": "success", "updated_keys": list(payload.keys())}

# --- Account ---

@router.get("/Account/ALL", summary="Obtiene el summary de todas las cuentas")
async def get_account_summary(request: Request):
    """
    Obtiene el summary completo de todas las cuentas.
    """
    connector = get_connector(request)
    summary = connector.wrapper.account_summary.copy()
    connector.logger.info(f"API: Serving Account Summary for {len(summary)} accounts")
    
    # Enrich with type from Config (derived from .env)
    conn_type = "PAPER" if Config.is_paper_trading() else "LIVE"
    
    for acc_id, data in summary.items():
        data["type"] = conn_type
            
    return summary

@router.get("/Account/{accountId}", summary="Obtiene el summary de una cuenta específica")
async def get_account_detail(accountId: str, request: Request):
    """
    Obtiene el summary de una cuenta específica.
    """
    connector = get_connector(request)
    if accountId not in connector.wrapper.account_summary:
        raise HTTPException(status_code=404, detail=f"Account '{accountId}' not found")
    return connector.wrapper.account_summary[accountId]

# --- Orders ---

@router.get("/Orders/ListAll", summary="Lista todas las ordenes")
async def list_all_orders(request: Request, connector: IBConnector = Depends(check_ready)):
    """
    Lista todas las ordenes abiertas.
    Bloquea hasta que el sistema esté listo.
    """
    # Connector is injected by check_ready
    # Convert internal order objects to dicts for JSON response
    # This is a simplified representation
    from utils import generate_spread_symbol_str
    
    orders_data = {}
    for oid, data in connector.wrapper.open_orders.items():
        order = data['order']
        contract = data['contract']
        # state = data['state']
        
        symbol_display = connector.get_readable_contract_name(contract)
                
        # Status details
        status_info = connector.wrapper.order_statuses.get(oid, {})
        # If it's still a string (old state or race condition), handle it
        if isinstance(status_info, str):
            status_info = {"status": status_info}

        legs_data = []
        if contract.secType == "BAG" and contract.comboLegs:
            for leg in contract.comboLegs:
                # Try to resolve leg symbol from cache
                leg_symbol = str(leg.conId) # Fallback
                cached_leg = connector.symbol_cache.get(leg.conId)
                if cached_leg:
                     if isinstance(cached_leg, dict):
                         leg_symbol = cached_leg.get('localSymbol') or cached_leg.get('symbol', str(leg.conId))
                     elif isinstance(cached_leg, str):
                         leg_symbol = cached_leg

                legs_data.append({
                    "conId": leg.conId,
                    "symbol": leg_symbol,
                    "ratio": leg.ratio,
                    "action": leg.action,
                    "exchange": leg.exchange
                })

        orders_data[oid] = {
             "accountId": order.account,
             "symbol": symbol_display,
             "secType": contract.secType,
             "legs": legs_data,
             "action": order.action,
             "totalQuantity": order.totalQuantity,
             "orderType": order.orderType,
             "tif": order.tif,
             "lmtPrice": order.lmtPrice,
             "auxPrice": order.auxPrice,
             "permId": order.permId,
             "parentId": order.parentId,
             "orderRef": order.orderRef,
             "status": status_info.get("status", "Unknown"),
             "filled": status_info.get("filled", 0.0),
             "remaining": status_info.get("remaining", order.totalQuantity),
             "lastFillPrice": status_info.get("lastFillPrice", 0.0)
        }
    return orders_data

@router.post("/Orders/RequestOpenOrders", summary="Solicita a IB que actualice las ordenes")
async def req_open_orders(request: Request):
    """
    Solicita a IB que actualice las ordenes abiertas y elimine las stale.
    """
    connector = get_connector(request)
    connector.request_open_orders_sync()
    return {"status": "Request Sent"}

@router.post("/Orders/PlaceOrder", summary="Crea una orden")
async def place_order(body: Dict[str, Any] = Body(...), request: Request = None):
    """
    Crea una orden simple.
    Body: {'symbol': '', 'action': '', 'oType': '', 'LmtPrice': '', 'qty': '', 'accountId': 'Optional', 'tif': 'Optional'}
    """
    connector = get_connector(request)
    # We treat 'body' as both contract source and order source for simplicity
    # In reality, might want to split or infer contract from symbol
    
    # Check if we need to infer secType? Default to FUT
    if 'secType' not in body:
        body['secType'] = 'FUT'
    
    # 1. Parse Contract
    contract = create_contract(body)
    
    # NEW: Smart Resolution for Multileg Spreads (inferred BAG)
    if contract.symbol and contract.secType == "FUT" and any(char in contract.symbol for char in ['+', '-']):
        connector.logger.info(f"API: Detected potential spread in PlaceOrder: {contract.symbol}. Resolving legs...")
        connector.resolve_bag_contract(contract)
        if contract.secType == "BAG" and not contract.comboLegs:
            raise HTTPException(status_code=400, detail=f"Failed to resolve legs for spread: {contract.symbol}")
    
    # 2. Extract specific parameters
    action = body.get('action')
    qty = float(body.get('qty', 0))
    o_type = body.get('oType', 'LMT')
    price = float(body.get('LmtPrice', body.get('price', 0)))
    aux_price = float(body.get('auxPrice', 0))
    tif = body.get('tif')
    order_ref = body.get('orderRef', '')
    account_id = body.get('accountId')
    
    if not all([action, qty]):
        raise HTTPException(status_code=400, detail="Missing required order parameters (action, qty)")

    oid = connector.place_simple_order(
        contract, action, qty, o_type, 
        price=price, lmtPrice=price, auxPrice=aux_price,
        tif=tif, order_ref=order_ref, account_id=account_id
    )
    return {"status": "Order Placed", "orderId": oid}

@router.post("/Orders/PlaceOCA", summary="Crea una orden OCA")
async def place_oca(body: Dict[str, Any] = Body(...), request: Request = None):
    """
    Crea una orden OCA.
    Body: {'symbol': '', 'actionSL': '', 'actionTP': '', 'LmtPriceSL': '', 'LmtPrice': '', 'qty': '', 'accountId': 'Optional', 'tif': 'Optional'}
    """
    connector = get_connector(request)
    if 'secType' not in body:
        body['secType'] = 'FUT'
        
    order_ids = connector.place_oca_order(None, body, body)
    return {"status": "OCA Placed", "orderIds": order_ids}

@router.post("/Orders/PlaceBracket", summary="Crea una orden Bracket")
async def place_bracket(body: Dict[str, Any] = Body(...), request: Request = None):
    """
    Crea una orden Bracket.
    Body: {'symbol': '', 'action': '', 'LmtPriceSL': '', 'LmtPriceTP': '', 'LmtPrice': '', 'qty': '', 'accountId': 'Optional', 'tif': 'Optional'}
    """
    connector = get_connector(request)
    if 'secType' not in body:
        body['secType'] = 'FUT'
        
    # 1. Parse Contract
    contract = create_contract(body)
    
    # NEW: Smart Resolution for Multileg Spreads (inferred BAG)
    if contract.symbol and contract.secType == "FUT" and any(char in contract.symbol for char in ['+', '-']):
        connector.logger.info(f"API: Detected potential spread in PlaceBracket: {contract.symbol}. Resolving legs...")
        connector.resolve_bag_contract(contract)
        if contract.secType == "BAG" and not contract.comboLegs:
            raise HTTPException(status_code=400, detail=f"Failed to resolve legs for spread: {contract.symbol}")
    
    # 2. Extract positional arguments for connector.place_bracket_order
    action = body.get('action')
    qty = float(body.get('qty', 0))
    price = float(body.get('LmtPrice', 0))
    tp_price = float(body.get('LmtPriceTP', 0))
    sl_price = float(body.get('LmtPriceSL', 0))
    
    # NEW: Extract metadata
    tif = body.get('tif')
    order_ref = body.get('orderRef', '')
    account_id = body.get('accountId')
    
    if not all([action, qty]):
        raise HTTPException(status_code=400, detail="Missing action or qty in bracket order request.")

    import traceback
    try:
        # Call with arguments as defined in ib_connector.py
        order_ids = connector.place_bracket_order(
            contract, action, qty, price, tp_price, sl_price,
            tif=tif, order_ref=order_ref, account_id=account_id
        )
        return {"status": "Bracket Placed", "orderIds": order_ids}
    except Exception as e:
        err_msg = traceback.format_exc()
        raise HTTPException(status_code=500, detail=f"Internal Error in place_bracket_order: {err_msg}")

@router.delete("/Orders/{orderId}", summary="Cancela una orden")
async def cancel_order(orderId: int, request: Request):
    """
    Cancela una orden.
    """
    connector = get_connector(request)
    connector.cancel_order(orderId)
    return {"status": "Cancel Request Sent", "orderId": orderId}

@router.post("/Orders/{orderId}/Update", summary="Actualiza una orden")
async def update_order(orderId: int, body: Dict[str, Any], request: Request):
    """
    Actualiza una orden existente.
    Body example: {"qty": 2.0, "LmtPrice": 150.5}
    """
    connector = get_connector(request)
    
    # Extract parameters from body
    qty = body.get('qty')
    lmt_price = body.get('LmtPrice')
    
    if qty is None or lmt_price is None:
        raise HTTPException(status_code=400, detail="Missing 'qty' or 'LmtPrice' in request body.")
        
    try:
        qty = float(qty)
        lmt_price = float(lmt_price)
    except (ValueError, TypeError):
        raise HTTPException(status_code=400, detail="Invalid numeric values for 'qty' or 'LmtPrice'.")

    # Call connector to transmit modification
    success = connector.modify_order(orderId, qty, lmt_price)
    
    if not success:
        raise HTTPException(status_code=404, detail=f"Order {orderId} not found in cache. It may have been filled, cancelled or is not yet synced.")
        
    return {"status": "Update Request Sent", "orderId": orderId}

# --- Executions ---

@router.get("/Executions", summary="Obtiene las ejecuciones")
async def get_executions(request: Request, strategy: Optional[str] = Query(None), symbol: Optional[str] = Query(None), days: int = 30):
    """
    Obtiene las ultimas ejecuciones filtradas por estrategia/símbolo.
    """
    connector = get_connector(request)
    start_time = f"-{days}d"
    
    executions = connector.db_client.get_executions(strategy=strategy, symbol=symbol, start=start_time)
    return executions


# --- Contracts & Watchlist ---

@router.get("/Contract/WatchList", summary="Obtiene la Watchlist")
async def get_watchlist(request: Request):
    """
    Obtiene la Watchlist actual.
    """
    connector = get_connector(request)
    return connector.watchlist_manager.get_contracts()

@router.post("/Contract/WatchList", summary="Agrega contratos a la Watchlist")
async def add_to_watchlist(contracts: Any = Body(...), request: Request = None):
    """
    Agrega contratos a la Watchlist.
    """
    connector = get_connector(request)
    added_count = 0
    
    # Ensure it's a list
    if not isinstance(contracts, list):
        contracts = [contracts]
        
    for c in contracts:
        # Extract symbol
        if isinstance(c, dict):
            symbol = c.get('symbol')
        else:
            symbol = str(c)
            
        if not symbol:
            continue
            
        if connector.watchlist_manager.add_contract(symbol):
            # Trigger subscription
            legs = parse_contract_symbol(symbol)
            if len(legs) == 1:
                connector.subscribe_contract(legs[0])
            else:
                connector.subscribe_contract({"symbol": symbol, "secType": "BAG", "legs": legs})
            added_count += 1
            
    if added_count > 0:
        connector.reconcile_portfolio()

    return {"status": "Updated", "added": added_count}

@router.delete("/Contract/WatchList/{symbol}", summary="Elimina un contrato de la Watchlist")
async def delete_from_watchlist(symbol: str, request: Request):
    """
    Elimina un contrato de la Watchlist.
    """
    connector = get_connector(request)
    
    # Try to remove from manager
    if connector.watchlist_manager.remove_contract(symbol):
        # Optional: Unsubscribe? 
        # For now, we keep subscription active in memory until restart or leverage logic to unsubscribe if no other reason to track.
        # But 'active_subscriptions' logic is complex (shared by positions/orders).
        # Simplest is just remove from WatchList persistence.
        return {"status": "Removed", "symbol": symbol}
        
    raise HTTPException(status_code=404, detail=f"Symbol {symbol} not found in watchlist")

@router.get("/Contract/ListAllUnique", summary="Lista todos los contratos con detalles")
async def list_unique_contracts(request: Request, accountId: Optional[str] = Query(None)):
    """
    Obtiene una lista de contratos que están siendo monitoreados (Watchlist + Posiciones + Ordenes).
    La fuente es el registro interno active_subscriptions.
    """
    connector = get_connector(request)
    
    # 0. Get all managed accounts (for Watchlist global view)
    all_managed_accounts = list(connector.wrapper.account_summary.keys())

    # 1. Map gConIds to Accounts having active orders
    gcon_to_order_accounts = {}
    for oid, data in connector.wrapper.open_orders.items():
        g_con_id = connector.order_id_to_g_con_id.get(oid)
        if g_con_id:
            if g_con_id not in gcon_to_order_accounts:
                gcon_to_order_accounts[g_con_id] = set()
            gcon_to_order_accounts[g_con_id].add(data['order'].account)

    active_order_g_con_ids = set(gcon_to_order_accounts.keys())

    # 2. Identify contracts in watchlist
    watchlist_symbols = connector.watchlist_manager.get_contracts()
    
    results = []
    for g_con_id, data in connector.active_subscriptions.items():
        contract = data['contract']
        readable = connector.get_readable_contract_name(contract)
        is_in_watchlist = (contract.symbol in watchlist_symbols) or (readable in watchlist_symbols)
        
        # Determine relevant accounts for this contract
        # Start with accounts that have actual positions
        relevant_accounts = set()
        
        # A. Accounts with Reconciled Positions
        for acc, positions in connector.portfolio_service.reconciled_positions.items():
             if g_con_id in positions:
                 relevant_accounts.add(acc)
        
        # B. Accounts with Active Orders (even if 0 pos)
        if g_con_id in gcon_to_order_accounts:
            relevant_accounts.update(gcon_to_order_accounts[g_con_id])
            
        # C. If in Watchlist, include ALL accounts (so it shows up everywhere)
        if is_in_watchlist:
            relevant_accounts.update(all_managed_accounts)

        # Apply Requested Filter
        if accountId and accountId != "ALL":
             if accountId in relevant_accounts:
                 relevant_accounts = {accountId}
             else:
                 relevant_accounts = set()

        # Build Positions List
        positions_list = []
        total_abs_position = 0.0
        
        for acc in relevant_accounts:
            qty = connector.portfolio_service.reconciled_positions.get(acc, {}).get(g_con_id, 0.0)
            avg = connector.portfolio_service.reconciled_avg_costs.get(acc, {}).get(g_con_id, 0.0)
            
            # We add it even if qty is 0, to signify "This account 'knows' about this contract"
            positions_list.append({
                "accountId": acc,
                "qty": clean_float(qty),
                "avgPrice": clean_float(avg)
            })
            total_abs_position += abs(qty)

        # Filtering Logic:
        # If no relevant accounts found (after filter), skip
        if not positions_list:
            continue

        readable_symbol = connector.get_readable_contract_name(contract)
        
        # Pricing (Unified Cache)
        if contract.secType == "BAG" and g_con_id not in connector.db_client.latest_prices:
            # Proactively calculate if cache is empty on startup
            connector.market_data_service.recalculate_bag_price(g_con_id, data)
            
        prices = connector.db_client.latest_prices.get(g_con_id, {})
        bid = prices.get('BID')
        ask = prices.get('ASK')
        last = calculate_display_price(prices) or 0.0

        # A. Normalized Multiplier for PnL
        multiplier = connector.get_contract_multiplier(contract, data)

        item = {
            "gConId": g_con_id,
            "symbol": readable_symbol,
            "secType": contract.secType,
            "currency": contract.currency,
            "conId": 0 if contract.secType == "BAG" else (getattr(contract, 'conId', None)),
            "multiplier": clean_float(multiplier or 1),
            "positions": positions_list,
            "bid": clean_float(bid),
            "ask": clean_float(ask),
            "last": clean_float(last)
        }
        
        # 4. Refine based on secType
        if contract.secType != "BAG":
            item["product"] = data.get("product", "")
            item["month"] = data.get("month", "")
            item["year"] = data.get("year", "")
            item["lastorderdate"] = contract.lastTradeDateOrContractMonth
        else:
            item["legs"] = connector.expand_bag_legs(contract, data.get("legs"))
            
        results.append(item)

    return results

@router.get("/Contract/Sync/{symbol:path}", summary="Obtiene los datos de un contrato inmediatamente")
async def get_contract_sync(symbol: str, request: Request):
    """
    Obtiene los datos de un contrato inmediatamente.
    Bloquea hasta que los datos estén disponibles o hasta el tiempo máximo.
    """
    connector = get_connector(request)
    connector.logger.info(f"API: Received Sync Request for {symbol}")
    
    # We use a 6 second timeout to be safe within typical HTTP limits (though request might timeout sooner if not configured)
    data = connector.get_sync_contract_data(symbol, timeout=6.0)
    
    # We don't raise an exception on 'error' (like timeout) anymore, because 
    # we want to return the contract metadata even if ticks are missing.
    # The client can check for 'ticks' null or 'error' key.
         
    # Sanitize floats and normalize keys to lowercase
    if data.get("ticks"):
        normalized_ticks = {}
        for k, v in data["ticks"].items():
            normalized_ticks[k.lower()] = clean_float(v)
        data["ticks"] = normalized_ticks
        
    # Sanitize legs ticks
    if data.get("legs"):
        for leg in data["legs"]:
            if leg.get("ticks"):
                norm_leg_ticks = {}
                for k, v in leg["ticks"].items():
                    norm_leg_ticks[k.lower()] = clean_float(v)
                leg["ticks"] = norm_leg_ticks
            
    return data

@router.get("/Contract/SyncActive/{product}", summary="Descubre todos los contratos activos de un producto")
async def get_active_contracts(product: str, request: Request, secType: str = Query("FUT")):
    """
    Busca y devuelve una lista de todos los contratos activos para un producto (p.ej. HE, LE).
    Bloquea hasta que todos los detalles han sido recibidos de IB.
    """
    connector = get_connector(request)
    connector.logger.info(f"API: Discovering all active '{secType}' for product: {product}")
    
    # Timeout at 12 seconds to be safe (IB discovery can be slow for many expirations)
    results = connector.contract_service.resolve_all_contracts_sync(product, sec_type=secType, timeout=12.0)
    
    return {
        "product": product,
        "secType": secType,
        "count": len(results),
        "contracts": results
    }

@router.get("/Contract/{gConId}", summary="Obtiene la información de un contrato")
async def get_contract_info(gConId: str, request: Request, accountId: Optional[str] = Query(None)):
    """
    Obtiene la información de un contrato.
    """
    connector = get_connector(request)
    
    if gConId in connector.active_subscriptions:
        sub_data = connector.active_subscriptions[gConId]
        contract = sub_data['contract']
        
        # Pricing (Unified Cache)
        positions_list = []
        
        target_accounts = connector.portfolio_service.reconciled_positions.keys()
        if accountId and accountId != "ALL":
             if accountId in connector.portfolio_service.reconciled_positions:
                 target_accounts = [accountId]
             else:
                 target_accounts = []
        
        for acc in target_accounts:
            qty = connector.portfolio_service.reconciled_positions[acc].get(gConId, 0.0)
            avg = connector.portfolio_service.reconciled_avg_costs[acc].get(gConId, 0.0)
            if qty != 0:
                positions_list.append({
                    "accountId": acc,
                    "qty": clean_float(qty),
                    "avgPrice": clean_float(avg)
                })

        pricing = connector.db_client.latest_prices.get(gConId, {})
        bid = pricing.get('BID')
        ask = pricing.get('ASK')
        last = pricing.get('LAST')

        # Normalized Multiplier
        multiplier = connector.get_contract_multiplier(contract, sub_data)

        # Convert object to dict
        c_dict = {
             "symbol": connector.get_readable_contract_name(contract),
             "secType": contract.secType,
             "exchange": contract.exchange,
             "currency": contract.currency,
             "strike": contract.strike,
             "right": contract.right,
             "conId": 0 if contract.secType == "BAG" else (contract.conId if hasattr(contract, 'conId') else None),
             "multiplier": clean_float(multiplier or 1),
             "positions": positions_list,
             "bid": clean_float(bid),
             "ask": clean_float(ask),
             "last": clean_float(last)
        }
        
        if contract.secType != "BAG":
            c_dict["lastTradeDateOrContractMonth"] = contract.lastTradeDateOrContractMonth
            c_dict["product"] = sub_data.get("product", "")
            c_dict["month"] = sub_data.get("month", "")
            c_dict["year"] = sub_data.get("year", "")
        
        if contract.secType == "BAG":
            c_dict["legs"] = connector.expand_bag_legs(contract, sub_data.get("legs"))
            
        return {"gConId": gConId, "source": "ActiveRegistry", "contract": c_dict}

    raise HTTPException(status_code=404, detail="Contract not found in Active Registry")



@router.get("/Contract/{gConId}/LastTicks", summary="Obtiene los últimos ticks de un contrato")
async def get_contract_last(gConId: str, request: Request):
    """
    Obtiene los últimos ticks de un contrato.
    """
    connector = get_connector(request)
    target_symbol = None
    
    if gConId in connector.active_subscriptions:
        sub_data = connector.active_subscriptions[gConId]
        contract = sub_data['contract']
        
        # BAG Synthetic Price (Real-time)
        if contract.secType == "BAG" and "legs" in sub_data:
            prices = connector.db_client.get_realtime_synthetic_price(sub_data['legs'])
            return {
                "last": prices.get("last"),
                "bid": prices.get("bid"),
                "ask": prices.get("ask"),
                "time": str(datetime.now())
            }
            
        target_symbol = connector.get_readable_contract_name(contract)
        
    if not target_symbol:
        return {"last": None}

    df = connector.db_client.get_contract_data(
        symbol=target_symbol, 
        bucket=Config.INFLUXDB_BUCKET_PRICES,
        start="-1h" 
    )
    if not df.empty:
        last_row = df.iloc[-1]
        return {
            "last": clean_float(last_row.get("LAST")), 
            "bid": clean_float(last_row.get("BID")), 
            "ask": clean_float(last_row.get("ASK")), 
            "time": str(last_row.get("_time"))
        }
    return {"last": None}

@router.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    # Ensure connector has manager reference immediately
    connector = websocket.app.state.ib_connector
    if connector.ws_manager is None:
        connector.set_ws_manager(manager)
        
    try:
        while True:
            data = await websocket.receive_text()
            try:
                msg = json.loads(data)
                action = msg.get("action")
                
                if action == "subscribe":
                    topic = msg.get("topic")
                    if topic:
                        await manager.subscribe(websocket, topic)
                        
                elif action == "unsubscribe":
                    topic = msg.get("topic")
                    if topic:
                        await manager.unsubscribe(websocket, topic)
                        
            except json.JSONDecodeError:
                pass
            except Exception as e:
                print(f"WS Error: {e}")
                
    except WebSocketDisconnect:
        manager.disconnect(websocket)
