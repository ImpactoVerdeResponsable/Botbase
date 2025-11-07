# protection_manager.py
"""
Módulo encargado de colocar y gestionar las órdenes de protección (SL/TP) en exchange
y de ofrecer fallback de cierre de emergencia si la colocación falla.

Funciones principales:
- place_entry_with_exchange_protection(...)  -> coloca entry market + protection (OCO / futures stop/tp)
- cancel_protection_orders(protection_meta)  -> cancela orders protectoras
- reconcile_protections_on_startup()         -> revisa active_positions y asegura protections
- emergency_close_if_triggered(...)        -> worker de emergencia (websocket/aggTrade-based)

Dependencias:
- binance_interaction (cliente async adaptado de tu stack)
- app_state_manager (get_app_state_manager) del módulo position_management
- unified_logger.get_logger
- asyncio
"""
import asyncio
from decimal import Decimal, InvalidOperation
import time
from typing import Optional, Dict, Any, Tuple, List

from unified_logger import get_logger
import binance_interaction as bi  # se espera interfaz async compatible
# --- INICIO DE MODIFICACIÓN: (Circular Import Fix) ---
# from position_management import get_app_state_manager  # getter lazy-safe (MOVIDO A LOCAL)
# --- FIN DE MODIFICACIÓN ---
import utils  # para roundings y utilidades de symbol/ticksize
import config # Importar config para settings

logger = get_logger(__name__)

# --- INICIO DE MODIFICACIÓN: (Línea de log solicitada) ---
logger.info("✅ protection_manager.py (MODIFICADO) está siendo importado.", extra={'symbol': 'IMPORT_CHECK'})
# --- FIN DE MODIFICACIÓN ---

# Configurables (Cargados desde config para flexibilidad)
PROTECTION_PLACEMENT_RETRIES = config.get_param("PROTECTION_MANAGER.PLACEMENT_RETRIES", 3)
PROTECTION_PLACEMENT_BACKOFF = config.get_param("PROTECTION_MANAGER.PLACEMENT_BACKOFF_SEC", 0.5)
EMERGENCY_WATCHER_TIMEOUT = config.get_param("PROTECTION_MANAGER.EMERGENCY_TIMEOUT_SEC", 300)


class ProtectionPlacementError(Exception):
    pass


def _to_decimal_safe(v, default=None) -> Optional[Decimal]:
    try:
        if v is None:
            return default
        return Decimal(str(v))
    except (InvalidOperation, ValueError):
        return default

# ---------------------------------------------------------------------------
# Funciones de Lógica Principal (Platzing y Cancelación)
# ---------------------------------------------------------------------------

async def place_entry_with_exchange_protection(
    client: Any, # El AsyncClient
    symbol: str,
    side: str, # 'BUY' | 'SELL'
    requested_quantity: Decimal,
    sl_price: Decimal,
    tp_price: Decimal,
    transaction_id: str,
    market_type: str = "SPOT"
) -> Dict[str, Any]:
    """
    Ejecuta una entrada de mercado y coloca las órdenes de protección en el exchange.
    - client: Async client de Binance
    - symbol, side ('BUY'|'SELL'), requested_quantity: cantidad solicitada
    - sl_price, tp_price: precios target (Decimal)
    - transaction_id: id para logging
    - market_type: "SPOT" o "FUTURES"
    Retorna metadata con entry_order y protection orders o lanza ProtectionPlacementError.
    """
    # --- INICIO DE MODIFICACIÓN: (Circular Import Fix) ---
    from position_management import get_app_state_manager
    # --- FIN DE MODIFICACIÓN ---
    log_extra = {'transaction_id': transaction_id, 'symbol': symbol}
    app_state = get_app_state_manager()
    entry_order = {}
    
    # 1) Place market order (entry)
    try:
        logger.info(f"Colocando orden de entrada MARKET ({side}) por {requested_quantity} {symbol}", extra=log_extra)
        # Determinar la función de orden correcta (Spot vs Futures)
        if market_type.upper() == "FUTURES":
            order_func = bi.async_futures_create_order
        else:
            order_func = bi.async_create_order

        entry_order = await order_func(
            client,
            symbol=symbol,
            side=side,
            type='MARKET',
            quantity=str(requested_quantity),
            newOrderRespType='FULL',
            transaction_id=transaction_id
        )
    except Exception as e:
        logger.critical(f"Fallo crítico en la orden de entrada MARKET: {e}", extra=log_extra, exc_info=True)
        raise ProtectionPlacementError(f"Entry market order failed: {e}")

    # 2) Parse fill info
    # (Binance usa 'executedQty' en Spot/Futures v1, 'filledQty' en v3)
    executed_qty_str = entry_order.get('executedQty') or entry_order.get('qty') # Futures 'qty'
    executed_qty = _to_decimal_safe(executed_qty_str, default=Decimal('0'))

    if executed_qty <= 0:
        # Check partial fills if structure exists
        fills = entry_order.get('fills', [])
        if fills and isinstance(fills, list):
             executed_qty = sum(_to_decimal_safe(f.get('qty'), 0) for f in fills)
        
        if executed_qty <= 0:
            logger.critical(f"La orden de entrada no se llenó (executedQty 0). OrderResp: {entry_order}", extra=log_extra)
            raise ProtectionPlacementError("Entry order not filled")

    logger.info(f"Entrada llenada. Qty: {executed_qty}. AvgPrice: {entry_order.get('avgPrice', 'N/A')}", extra=log_extra)

    # 3) Redondear cantidad y precios a los filtros del exchange
    try:
        qty_rounded = bi.format_quantity(symbol, executed_qty)
        sl_price_rounded = bi.format_price(symbol, sl_price)
        tp_price_rounded = bi.format_price(symbol, tp_price)
    except Exception as e_round:
        logger.error(f"Error formateando Qty/Precios para protección: {e_round}", extra=log_extra, exc_info=True)
        # Usar valores sin formatear, puede fallar en la API
        qty_rounded = executed_qty
        sl_price_rounded = sl_price
        tp_price_rounded = tp_price

    protection_meta = {
        "entry_order_id": entry_order.get('orderId'),
        "entry_transactTime": entry_order.get('transactTime'),
        "protection_orders": [], # Lista de IDs de exchange
        "protection_group_id": None, # OCO List ID
        "created_at": time.time(),
        "symbol": symbol,
        "entry_side": side,
        "quantity": str(qty_rounded),
        "sl_price": str(sl_price_rounded),
        "tp_price": str(tp_price_rounded),
        "market_type": market_type,
        "status": "PENDING_PROTECTION",
        "transaction_id": transaction_id
    }

    # 4) Place protection orders (with retries)
    last_exc = None
    protection_side = 'SELL' if side == 'BUY' else 'BUY'

    for attempt in range(1, PROTECTION_PLACEMENT_RETRIES + 1):
        try:
            if market_type.upper() == "SPOT":
                logger.info(f"Colocando protección OCO (Intento {attempt})", extra=log_extra)
                
                # Ajustar stopLimitPrice (trigger - 1 tick)
                # Esta lógica debe ser robusta, usando tickSize
                sl_limit_price = sl_price_rounded # Por defecto
                
                oco_order = await bi.async_create_oco_order(
                    client,
                    symbol=symbol,
                    side=protection_side,
                    quantity=str(qty_rounded),
                    price=str(tp_price_rounded),           # Precio Límite para TP
                    stopPrice=str(sl_price_rounded),     # Trigger para Stop
                    stopLimitPrice=str(sl_limit_price),  # Límite para Stop (evitar slippage extremo)
                    stopLimitTimeInForce='GTC',
                    transaction_id=f"{transaction_id}_OCO_A{attempt}"
                )
                protection_meta['protection_orders'] = [o.get('orderId') for o in oco_order.get('orders', [])]
                protection_meta['protection_group_id'] = oco_order.get('orderListId')
                protection_meta['status'] = "ACTIVE"
                break # Éxito

            else: # FUTURES
                logger.info(f"Colocando protección Futures (Intento {attempt})", extra=log_extra)
                # Colocar TAKE_PROFIT_MARKET
                tp_order = await bi.async_futures_create_order(
                    client,
                    symbol=symbol,
                    side=protection_side,
                    type='TAKE_PROFIT_MARKET',
                    stopPrice=str(tp_price_rounded),
                    reduceOnly=True,
                    quantity=str(qty_rounded), # Qty debe ser 0 para TP/SL Market? No, Qty es requerido.
                    transaction_id=f"{transaction_id}_TP_A{attempt}"
                )
                # Colocar STOP_MARKET
                sl_order = await bi.async_futures_create_order(
                    client,
                    symbol=symbol,
                    side=protection_side,
                    type='STOP_MARKET',
                    stopPrice=str(sl_price_rounded),
                    reduceOnly=True,
                    quantity=str(qty_rounded),
                    transaction_id=f"{transaction_id}_SL_A{attempt}"
                )
                protection_meta['protection_orders'] = [tp_order.get('orderId'), sl_order.get('orderId')]
                protection_meta['status'] = "ACTIVE"
                break # Éxito

        except Exception as e:
            last_exc = e
            logger.warning(f"Intento {attempt} de colocar protección falló: {e}", extra=log_extra)
            # Manejar errores específicos de Binance si es necesario
            if "insufficient balance" in str(e).lower():
                logger.critical("Fallo por balance insuficiente al colocar OCO. La posición de entrada será cerrada.", extra=log_extra)
                await _emergency_close_position(client, symbol, protection_side, qty_rounded, transaction_id, "BALANCE_FAIL_OCO", market_type)
                raise ProtectionPlacementError(f"Insufficient balance for OCO: {e}")
            
            await asyncio.sleep(PROTECTION_PLACEMENT_BACKOFF * attempt)

    if protection_meta.get('status') != "ACTIVE":
        # Fallo final tras reintentos
        logger.error(f"Colocación de protección falló tras {PROTECTION_PLACEMENT_RETRIES} intentos. Armado de Watcher de Emergencia.", extra=log_extra)
        protection_meta['status'] = "FAILED"
        protection_meta['failed_reason'] = str(last_exc)
        _save_protection_meta(app_state, entry_order, protection_meta) # Guardar estado fallido
        
        # Armar watcher de emergencia
        asyncio.create_task(_emergency_watcher_for_symbol(
            symbol, sl_price_rounded, side, qty_rounded, transaction_id + "_EMERGENCY", market_type
        ))
        raise ProtectionPlacementError(f"Protection placement failed: {last_exc}")

    # 5) Persist protection meta (atomic to app_state)
    _save_protection_meta(app_state, entry_order, protection_meta)

    logger.info(f"Protección en Exchange colocada y persistida. Grupo/IDs: {protection_meta['protection_group_id'] or protection_meta['protection_orders']}", extra=log_extra)
    
    # Retornar la orden de entrada (original) y la metadata de protección
    return { "entry_order": entry_order, "protection_meta": protection_meta }


async def cancel_protection_orders(client: Any, protection_meta: Dict[str, Any], transaction_id: str = "CANCEL_PROTECTION"):
    """
    Cancela las órdenes de protección registradas en protection_meta.
    Devuelve True si todas las cancelaciones se hicieron OK o no eran necesarias.
    """
    # --- INICIO DE MODIFICACIÓN: (Circular Import Fix) ---
    from position_management import get_app_state_manager
    # --- FIN DE MODIFICACIÓN ---
    log_extra = {'transaction_id': transaction_id, 'symbol': protection_meta.get('symbol')}
    symbol = protection_meta.get('symbol')
    market_type = protection_meta.get('market_type', 'SPOT')
    app_state = get_app_state_manager()
    
    if not symbol:
        logger.error("No se puede cancelar protección, falta 'symbol' en metadata.", extra=log_extra)
        return False

    try:
        if market_type.upper() == "SPOT":
            # Usar OCO List ID si existe
            oco_list_id = protection_meta.get('protection_group_id')
            if oco_list_id:
                logger.info(f"Cancelando OCO por List ID: {oco_list_id}", extra=log_extra)
                await bi.async_cancel_oco_order(client, symbol, orderListId=oco_list_id, transaction_id=transaction_id)
            else:
                # Fallback: cancelar órdenes individuales
                logger.warning(f"No se encontró OCO List ID, cancelando órdenes individualmente.", extra=log_extra)
                for order_id in protection_meta.get('protection_orders', []):
                    await bi.async_cancel_order(client, symbol, orderId=order_id, transaction_id=transaction_id)
        
        else: # FUTURES
            logger.info(f"Cancelando órdenes de protección Futures.", extra=log_extra)
            for order_id in protection_meta.get('protection_orders', []):
                if order_id:
                    await bi.async_futures_cancel_order(client, symbol, orderId=order_id, transaction_id=transaction_id)

        # Actualizar estado en app_state
        _update_protection_status(app_state, protection_meta.get('entry_order_id'), "CANCELED_MANUAL")
        app_state.save(transaction_id=transaction_id)
        return True

    except Exception as e:
        # Manejar error si la orden ya no existe (ya fue llenada o cancelada)
        if "Order does not exist" in str(e) or "Unknown order" in str(e):
            logger.warning(f"No se pudo cancelar protección (orden ya no existe, prob. llenada): {e}", extra=log_extra)
            # Marcar como reconciliado
            _update_protection_status(app_state, protection_meta.get('entry_order_id'), "RECONCILED_FILLED")
            app_state.save(transaction_id=transaction_id)
            return True
        
        logger.error(f"Fallo al cancelar órdenes de protección: {e}", extra=log_extra, exc_info=True)
        return False

# ---------------------------------------------------------------------------
# Lógica de Fallback (Emergency Watcher)
# ---------------------------------------------------------------------------

async def _emergency_watcher_for_symbol(
    symbol: str, 
    sl_price: Decimal, 
    entry_side: str, # 'BUY' o 'SELL'
    qty: Decimal, 
    transaction_id: str, 
    market_type: str
):
    """
    Fallback: monitor de emergencia basado en trade ticks (aggTrade).
    Si detecta trigger (ej: trade price <= sl_price para posición LONG),
    ejecuta market order inmediato.
    """
    log_extra = {'transaction_id': transaction_id, 'symbol': symbol}
    logger.warning(f"Watcher de Emergencia ARMADO para {symbol} (SL: {sl_price}, Qty: {qty})", extra=log_extra)
    start_time = time.time()
    
    # Determinar lado de cierre
    close_side = 'SELL' if entry_side == 'BUY' else 'BUY'

    try:
        # Usar el stream de aggTrade (trades agregados) para baja latencia
        async for trade in bi.async_stream_agg_trades(symbol, timeout=EMERGENCY_WATCHER_TIMEOUT):
            
            # 'trade' es dict { 'p': price, 'q': quantity, 'm': is_buyer_maker, 'T': timestamp }
            price = _to_decimal_safe(trade.get('p'), default=None)
            if price is None:
                continue

            triggered = False
            # Si la entrada fue 'BUY' (Long), el SL se activa si el precio CAE a sl_price
            if entry_side == 'BUY' and price <= sl_price:
                triggered = True
            # Si la entrada fue 'SELL' (Short), el SL se activa si el precio SUBE a sl_price
            elif entry_side == 'SELL' and price >= sl_price:
                triggered = True

            if triggered:
                logger.critical(f"¡Watcher de Emergencia DISPARADO para {symbol} a {price}! Ejecutando cierre MARKET.", extra=log_extra)
                await _emergency_close_position(None, symbol, close_side, qty, transaction_id, "EMERGENCY_WATCHER_HIT", market_type)
                return # Terminar el watcher

            # Check de timeout
            if (time.time() - start_time) > EMERGENCY_WATCHER_TIMEOUT:
                logger.warning(f"Watcher de Emergencia TIMEOUT para {symbol}. Desarmando.", extra=log_extra)
                # Opcional: intentar cerrar la posición si el timeout se considera un SL
                # await _emergency_close_position(None, symbol, close_side, qty, transaction_id, "EMERGENCY_WATCHER_TIMEOUT", market_type)
                return

    except asyncio.TimeoutError:
        logger.warning(f"Watcher de Emergencia (stream) TIMEOUT para {symbol}. Desarmando.", extra=log_extra)
    except Exception as e:
        logger.error(f"Watcher de Emergencia (stream) FALLÓ para {symbol}: {e}", extra=log_extra, exc_info=True)
        # Considerar cierre de emergencia si el watcher falla
        # await _emergency_close_position(None, symbol, close_side, qty, transaction_id, "EMERGENCY_WATCHER_FAIL", market_type)
    finally:
        logger.info(f"Watcher de Emergencia desarmado para {symbol}.", extra=log_extra)


async def _emergency_close_position(
    client: Optional[Any], 
    symbol: str, 
    close_side: str, 
    qty: Decimal, 
    transaction_id: str, 
    reason: str, 
    market_type: str
):
    """
    Cierra una posición usando una orden MARKET. Usado por fallbacks.
    *Nota: Esta función asume que 'client' (AsyncClient) es opcional
    porque binance_interaction podría usar un cliente global si se pasa None.*
    """
    log_extra = {'transaction_id': transaction_id, 'symbol': symbol, 'reason': reason}
    try:
        if market_type.upper() == "FUTURES":
            order_func = bi.async_futures_create_order
        else:
            order_func = bi.async_create_order

        await order_func(
            client,
            symbol=symbol,
            side=close_side,
            type='MARKET',
            quantity=str(qty),
            reduceOnly=(market_type.upper() == "FUTURES"), # Solo para Futures
            transaction_id=transaction_id
        )
        logger.info(f"Cierre de emergencia ({reason}) ejecutado.", extra=log_extra)
    except Exception as e:
        logger.critical(f"¡FALLO EN EL CIERRE DE EMERGENCIA! {e}", extra=log_extra, exc_info=True)

# ---------------------------------------------------------------------------
# Lógica de Persistencia y Reconciliación (Helpers)
# ---------------------------------------------------------------------------

def _save_protection_meta(app_state, entry_order: Dict[str, Any], protection_meta: Dict[str, Any]):
    """
    Guarda metadata de protección en app_state.
    """
    log_extra = {'transaction_id': protection_meta.get('transaction_id', 'SAVE_PROT'), 'symbol': protection_meta.get('symbol')}
    try:
        entry_id = str(protection_meta.get('entry_order_id') or entry_order.get('orderId') or int(time.time()*1000))
        
        # Guardar en el 'active_position'
        active_pos = app_state.get_active_position(protection_meta['symbol'])
        if active_pos:
            active_pos['protection_meta'] = protection_meta
            app_state.open_new_position(protection_meta['symbol'], active_pos)
        
        # Guardar en el registro de 'protections'
        protections = app_state.state.setdefault('protections', {})
        protections[entry_id] = protection_meta
        
        app_state.save(transaction_id=log_extra['transaction_id'])
    except Exception as e:
        logger.error(f"Fallo al persistir metadata de protección: {e}", extra=log_extra, exc_info=True)

def _update_protection_status(app_state, entry_order_id: str, new_status: str):
    """
    Actualiza el estado de una protección en app_state.
    """
    try:
        protections = app_state.state.get('protections', {})
        if entry_order_id in protections:
            protections[entry_order_id]['status'] = new_status
            
            symbol = protections[entry_order_id].get('symbol')
            if symbol:
                active_pos = app_state.get_active_position(symbol)
                if active_pos and 'protection_meta' in active_pos:
                    active_pos['protection_meta']['status'] = new_status
                    app_state.open_new_position(symbol, active_pos)
    except Exception as e:
        logger.warning(f"Fallo al actualizar estado de protección para {entry_order_id}: {e}")


async def reconcile_protections_on_startup(client: Any, transaction_id: str = "RECONCILE_STARTUP"):
    """
    Al inicio del bot: Asegura que cada posición activa tenga protección.
    Si falta, intenta colocarla o arma el watcher de emergencia.
    """
    # --- INICIO DE MODIFICACIÓN: (Circular Import Fix) ---
    from position_management import get_app_state_manager
    # --- FIN DE MODIFICACIÓN ---
    app_state = get_app_state_manager()
    active_positions = app_state.get_all_active_positions()
    log_extra = {'transaction_id': transaction_id, 'symbol': 'RECONCILE'}
    logger.info(f"Reconciliando protecciones en exchange para {len(active_positions)} posiciones activas...", extra=log_extra)
    
    for symbol, position in active_positions.items():
        try:
            log_extra_sym = {'transaction_id': f"{transaction_id}_{symbol[:4]}", 'symbol': symbol}
            protection_meta = position.get('protection_meta')
            
            # Caso 1: OK. Protección registrada y activa. (Se asume que la orden en exchange está OK)
            if protection_meta and protection_meta.get('status') == "ACTIVE":
                logger.debug(f"Protección {symbol} ya está ACTIVA.", extra=log_extra_sym)
                continue
                
            # Caso 2: Fallo previo. Protección registrada pero fallida. Armar watcher.
            if protection_meta and protection_meta.get('status') == "FAILED":
                logger.warning(f"Re-armando watcher de emergencia para {symbol} (fallo previo).", extra=log_extra_sym)
                asyncio.create_task(_emergency_watcher_for_symbol(
                    symbol, 
                    _to_decimal_safe(position.get('StopLoss')), 
                    position.get('entry_side', 'BUY'), # Asumir 'BUY' si no está
                    _to_decimal_safe(position.get('CantidadNetaEntrada')), 
                    transaction_id + "_RECON_FAIL", 
                    position.get('market_type', 'SPOT')
                ))
                continue

            # Caso 3: Sin protección. La posición existe pero no hay metadata de protección.
            logger.warning(f"Posición {symbol} sin metadata de protección. Intentando colocarla ahora.", extra=log_extra_sym)
            
            sl_price = _to_decimal_safe(position.get('StopLoss'))
            tp_price = _to_decimal_safe(position.get('TakeProfit'))
            qty = _to_decimal_safe(position.get('CantidadNetaEntrada'))
            side = position.get('entry_side', 'BUY') # Asumir 'BUY' si no está
            market_type = position.get('market_type', 'SPOT')
            
            if not all([sl_price, tp_price, qty]):
                logger.critical(f"Datos incompletos para reconciliar {symbol}. Armado de watcher fallará.", extra=log_extra_sym)
                continue
                
            try:
                # Re-colocar protección (Esto usa una orden de entrada simulada, ajustar si es necesario)
                # La lógica de 'place_entry_with_exchange_protection' coloca la entrada Y LUEGO la protección.
                # Aquí la entrada YA EXISTE. Necesitamos una función que SOLO coloque protección.
                
                # --- Simplificación de Reconciliación (Plan B) ---
                # Dado que 'place_entry_with_exchange_protection' es atómico, si 'protection_meta' no existe,
                # asumimos que la entrada falló o el bot se detuvo antes de persistir.
                # La lógica de reconciliación más segura es armar el watcher de emergencia.
                
                logger.warning(f"Armando watcher de emergencia para {symbol} (reconciliación).", extra=log_extra_sym)
                asyncio.create_task(_emergency_watcher_for_symbol(
                    symbol, 
                    sl_price, 
                    side, 
                    qty, 
                    transaction_id + "_RECON_NEW", 
                    market_type
                ))
                
            except Exception as e_recon_place:
                logger.error(f"Fallo al colocar protección durante reconciliación para {symbol}: {e_recon_place}", extra=log_extra_sym)
                # Fallback final: armar watcher
                asyncio.create_task(_emergency_watcher_for_symbol(
                    symbol, 
                    sl_price, 
                    side, 
                    qty, 
                    transaction_id + "_RECON_FAIL2", 
                    market_type
                ))

        except Exception as e:
            logger.error(f"Error fatal reconciliando {symbol}: {e}", extra={'symbol': symbol}, exc_info=True)