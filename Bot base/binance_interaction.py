# binance_interaction.py
from unified_logger import get_logger
import time
from decimal import Decimal, ROUND_DOWN
from binance.client import Client
from binance import AsyncClient # <--- MODIFICACIÓN: Importar AsyncClient
from binance.enums import *
from binance.exceptions import BinanceAPIException, BinanceOrderException, BinanceRequestException
import pandas as pd
import datetime
import asyncio
import aiohttp # <--- MODIFICACIÓN: Importar aiohttp para streams
import json # <--- MODIFICACIÓN: Importar json para streams
from typing import Optional # <--- MODIFICACIÓN: Importar Optional

import config
import utils
# --- INICIO DE MODIFICACIÓN: (Fase 4.0) ---
import csv_writer # Importar el nuevo writer para execution_logs
# --- FIN DE MODIFICACIÓN ---
from exceptions import (
    InitializationError,
   
    InsufficientDataError,
    FiltersNotFoundError,
    OrderPlacementError,
    CriticalBinanceError
)

logger = get_logger(__name__)
client = None
async_client = None # <--- MODIFICACIÓN: Añadir cliente asíncrono global

# --- INICIO DE CORRECCIÓN (NameError) ---
exchange_filters = {} # Inicializar el diccionario global
# --- FIN DE CORRECCIÓN ---

# --- Funciones Síncronas (Usadas por el loop POLLING) ---


def initialize_binance_client(transaction_id="INIT_CLIENT"):
    global client, async_client
    log_extra = {'transaction_id': transaction_id, 'symbol': 'BINANCE_CLIENT'}
    if client is None:
        try:
            api_key = config.BINANCE_API_KEY
            api_secret = config.BINANCE_API_SECRET
            if not api_key or not api_secret:
                raise InitializationError("Claves API de Binance no encontradas.")
            
            # Cliente Síncrono
            client = Client(api_key, api_secret, {"timeout": 20})
            client.ping()
            logger.info("Cliente de Binance SÍNCRONO inicializado exitosamente.", extra=log_extra)

            # Cliente Asíncrono (para modo EVENT)
            # NOTA: Se inicializa aquí, pero la conexión real la maneja asyncio
            async_client = AsyncClient(api_key, api_secret)
            logger.info("Cliente de Binance ASÍNCRONO listo.", extra=log_extra)

        except Exception as e:
            logger.critical(f"Error al inicializar cliente Binance: {e}", 
 extra=log_extra)
            raise CriticalBinanceError(f"Error al inicializar cliente Binance: {e}") from e

# <--- MODIFICACIÓN: Función para obtener el cliente asíncrono ---
def get_async_client() -> Optional[AsyncClient]:
    """Retorna la instancia global del AsyncClient."""
    if async_client is None:
        # Esto no debería pasar si initialize_binance_client se llamó primero
        logger.warning("get_async_client() llamado antes de initialize_binance_client. Intentando inicialización de emergencia.")
        try:
            initialize_binance_client(transaction_id="ASYNC_CLIENT_EMERGENCY")
        except Exception:
            return None
    return async_client
# --- FIN MODIFICACIÓN ---


def load_exchange_filters(symbols_list, transaction_id="LOAD_FILTERS_DEF"):
    global exchange_filters
    log_extra = {'transaction_id': transaction_id, 'symbol': 'EXCH_INFO'}
    if client is None: raise InitializationError("Cliente no disponible.")
    try:
        exchange_info = client.get_exchange_info()
        symbols_in_exchange = {s['symbol'] for s in exchange_info['symbols']}

        temp_filters = {}

        for symbol in symbols_list:
    
            if symbol in symbols_in_exchange:
                symbol_info = next((s for s in exchange_info['symbols'] if s['symbol'] == symbol), None)
                if symbol_info:
                    temp_filters[symbol] = {f['filterType']: f for f in symbol_info['filters']}

        exchange_filters.update(temp_filters)
        logger.info(f"Filtros cargados/actualizados para {len(temp_filters)} símbolos. Total en caché: {len(exchange_filters)}", extra=log_extra)
    except Exception as e:
        logger.error(f"Error al cargar filtros del exchange: {e}", extra=log_extra)
        raise CriticalBinanceError(f"Error al cargar filtros del exchange: {e}") from e

# --- Funciones de CPU (Seguras para Sync/Async) ---


def get_symbol_filters(symbol, transaction_id="GET_FILTERS"):
    if not exchange_filters or symbol not in exchange_filters:
        logger.warning(f"Filtros no encontrados en caché para {symbol}. Intentando carga de emergencia.",
                extra={'transaction_id': transaction_id, 'symbol': symbol})
        try:
            load_exchange_filters([symbol], transaction_id=f"{transaction_id}_EmergencyLoad")
            if symbol in exchange_filters:
                return exchange_filters[symbol]
        except Exception as e:
            logger.error(f"Carga de emergencia falló para {symbol}: {e}",
                      extra={'transaction_id': transaction_id, 'symbol': symbol})
            raise FiltersNotFoundError(f"Filtros no encontrados y carga de emergencia fallida para {symbol}.") from e

    if symbol not in exchange_filters:
         raise FiltersNotFoundError(f"Filtros no encontrados para {symbol}.")
    return exchange_filters[symbol]


def format_price(symbol, price_decimal, transaction_id="FMT_PRICE"):
    try:
        filters = get_symbol_filters(symbol, transaction_id)
        tick_size = Decimal(filters.get('PRICE_FILTER', {}).get('tickSize', '0.00000001'))
        if tick_size == Decimal(0): # Evitar división por cero si el filtro es inválido
             return Decimal(price_decimal)
        return (Decimal(price_decimal) / tick_size).to_integral_value(rounding=ROUND_DOWN) * tick_size
    except Exception as e:
        log_extra = {'transaction_id': transaction_id, 'symbol': symbol}
        logger.error(f"Error al formatear el precio para {symbol}: {e}", extra=log_extra)
        raise


def format_quantity(symbol, quantity_decimal, transaction_id="FMT_QTY"):
    try:
        filters = get_symbol_filters(symbol, transaction_id)
        step_size = Decimal(filters.get('LOT_SIZE', {}).get('stepSize', '0.00000001'))
        if step_size == Decimal(0): # Evitar división por cero
            return Decimal(quantity_decimal)
        return (Decimal(quantity_decimal) / step_size).to_integral_value(rounding=ROUND_DOWN) * step_size
    except Exception as e:
        log_extra = {'transaction_id': transaction_id, 'symbol': symbol}
        logger.error(f"Error al formatear la cantidad para {symbol}: {e}", extra=log_extra)
        raise


def get_min_notional_and_qty(symbol, transaction_id="GET_FILTERS_MIN"):
    log_extra = {'transaction_id': transaction_id, 'symbol': symbol}
    try:
        filters = get_symbol_filters(symbol, transaction_id)
        min_qty = Decimal(filters.get('LOT_SIZE', {}).get('minQty', '0'))
        min_notional = Decimal(filters.get('NOTIONAL', filters.get('MIN_NOTIONAL', {})).get('minNotional', '0'))
   
        return min_qty, min_notional
    except Exception as e:
        logger.error(f"No se pudieron obtener filtros minQty/minNotional para {symbol}: {e}", extra=log_extra)
        return None, None

# --- Funciones Síncronas (Bloqueantes) ---


def get_klines_data(symbol, interval, limit=500, transaction_id="GET_KLINES"):
    log_extra = {'transaction_id': transaction_id, 'symbol': symbol}
    if client is None: raise InitializationError("Cliente no disponible.")
    try:
        klines = client.get_klines(symbol=symbol, interval=interval, limit=limit)
        if not klines:
            logger.warning(f"No se recibieron klines para {symbol} (intervalo {interval}).", extra=log_extra)
            return pd.DataFrame()
        df = pd.DataFrame(klines, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume', 'close_time', 'quote_asset_volume', 'number_of_trades', 'taker_buy_base_asset_volume', 'taker_buy_quote_asset_volume', 'ignore'])
        numeric_cols = ['open', 'high', 'low', 'close', 'volume']

        df[numeric_cols] = df[numeric_cols].apply(pd.to_numeric, errors='coerce')
        for col in numeric_cols:
            df[col] = df[col].map(lambda x: Decimal(str(x)) if pd.notna(x) else None, na_action='ignore')

        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
        df.attrs['symbol'] = symbol
        return df
    except KeyboardInterrupt:
        logger.warning("Interrupción por teclado durante la obtención de klines.", extra=log_extra)
        raise
    except Exception as e:
        logger.error(f"Error obteniendo klines para {symbol}: {e}", extra=log_extra)
        raise InsufficientDataError(f"Error obteniendo klines para {symbol}: {e}") from e


def place_market_order(symbol, side, quantity_asset, transaction_id="PLACE_ORDER", is_retying=False):
    log_extra = {'transaction_id': transaction_id, 'symbol': symbol}
    quantity_decimal = Decimal(str(quantity_asset))
    formatted_quantity = format_quantity(symbol, quantity_decimal)

    if not formatted_quantity or formatted_quantity <= Decimal(0):
        err_msg = f"Cantidad formateada inválida o cero ({formatted_quantity})"
        # --- INICIO DE MODIFICACIÓN: (Fase 4.0) ---
        csv_writer.log_execution(
            transaction_id=transaction_id, symbol=symbol, side=side,
       
            quantity=str(quantity_decimal), price="N/A",
            status="REJECTED_INTERNAL", error_message=err_msg
        )
        # --- FIN DE MODIFICACIÓN ---
        raise OrderPlacementError(err_msg, symbol=symbol)

    params_order = {'symbol': symbol, 'side': side, 'type': ORDER_TYPE_MARKET, 'quantity': str(formatted_quantity)}

    try:
        order = client.create_order(**params_order)

        if order and order.get('status') == 'FILLED':
          
            total_value = Decimal(order.get('cummulativeQuoteQty', '0'))
            total_quantity = Decimal(order.get('executedQty', '0'))
            total_commission_in_base = Decimal('0')
            base_asset, quote_asset = utils.split_symbol_pair(symbol)

            total_commission_usdt_equivalent = Decimal('0')

            for fill in order.get('fills', []):
                if fill.get('commissionAsset') == base_asset:
  
                    total_commission_in_base += Decimal(fill.get('commission', '0'))

                try:
                    fill_price = Decimal(fill.get('price', '0'))
                    fill_commission = Decimal(fill.get('commission', '0'))
                 
                    fill_commission_asset = fill.get('commissionAsset')

                    if fill_commission_asset == base_asset:
                        total_commission_usdt_equivalent += fill_commission * fill_price
                    elif fill_commission_asset == config.get_param("GLOBAL_SETTINGS.ASSET_PARA_CALCULO_CAPITAL", "USDT"):
                    
                        total_commission_usdt_equivalent += fill_commission
                except Exception as e_fill:
                    logger.warning(f"Error procesando comisión en fill: {e_fill}", extra=log_extra)

            avg_price = (total_value / total_quantity) if total_quantity > 0 else Decimal('0')
            net_quantity = total_quantity - total_commission_in_base if side == SIDE_BUY else total_quantity

     
            normalized_order = {
                'orderId': order.get('orderId'),
                'status': order.get('status'),
                'transactTime': order.get('transactTime'),
                'avgPrice': str(avg_price),
                'executedQty': str(total_quantity),
      
                'quoteValue': str(total_value),
                'netQuantityReceived': str(net_quantity),
                'fills': order.get('fills', []),
                'total_commission_usdt_equivalent': str(total_commission_usdt_equivalent)
            }
            logger.info(f"Orden de mercado colocada para {symbol}: ID={normalized_order.get('orderId')}, Estado={normalized_order.get('status')}", extra=log_extra)
     
            
            # --- INICIO DE MODIFICACIÓN: (Fase 4.0) ---
            csv_writer.log_execution(
                transaction_id=transaction_id, symbol=symbol, side=side,
                quantity=str(formatted_quantity), price=str(avg_price),
                status=order.get('status'), error_message=None
          
            )
            # --- FIN DE MODIFICACIÓN ---
            return normalized_order

        return order

    except BinanceAPIException as e:

        # --- INICIO DE MODIFICACIÓN: (Contingencia de Saldo) ---
        if e.code == -2010 and side == SIDE_BUY and not is_retying:
            logger.warning(f"⚠️ Saldo insuficiente detectado para {symbol} (Error -2010). Intentando ajuste de emergencia.", extra=log_extra)
            
            asset_capital = config.get_param("GLOBAL_SETTINGS.ASSET_PARA_CALCULO_CAPITAL", "USDT")
            current_usdt_balance = get_account_balance_for_asset(asset_capital, transaction_id=f"{transaction_id}_GetBalContingency")
            
            percent_to_use = Decimal("0.5")
            new_usdt_amount = current_usdt_balance * percent_to_use
          
            try:
                klines = get_klines_data(symbol, config.get_param("GLOBAL_SETTINGS.INTERVAL"), limit=1, transaction_id=transaction_id)
                current_price = klines['close'].iloc[-1]
            except Exception:
                current_price = Decimal('1.0')

            if current_price <= 0:
 
                raise OrderPlacementError("Error de precio en contingencia.", symbol=symbol)

            new_quantity_asset = new_usdt_amount / current_price
            
            min_qty, min_notional = get_min_notional_and_qty(symbol, transaction_id)
            new_notional = new_quantity_asset * current_price
            
       
            if new_notional < min_notional:
                err_msg = f"Ajuste de saldo falló: 50% del saldo ({new_notional:.2f} USDT) no cubre mínimo notional."
                # --- CORRECCIÓN DE f-string ROTO ---
                logger.critical(f"❌ AJUSTE FALLIDO. El 50% del saldo ({new_notional:.2f} USDT) es menor al mínimo notional exigido ({min_notional:.2f} USDT). Descartando orden.", extra=log_extra)
                # --- FIN DE CORRECCIÓN ---
                
                # --- INICIO DE MODIFICACIÓN: (Fase 4.0) ---
                csv_writer.log_execution(
                    transaction_id=transaction_id, symbol=symbol, side=side,
                    quantity=str(formatted_quantity), price="N/A",
                
                    status="REJECTED_CONTINGENCY", error_message=err_msg
                )
                # --- FIN DE MODIFICACIÓN ---
                raise OrderPlacementError(err_msg, symbol=symbol)
            
            logger.warning(f"✅ Reintentando orden con cantidad reducida (Nuevo USDT: {new_usdt_amount:.2f}).", extra=log_extra)
       
            return place_market_order(symbol, side, new_quantity_asset, transaction_id, is_retying=True)
        # --- FIN DE MODIFICACIÓN ---
        
        err_msg = f"Code: {e.code} - {e.message}"
        logger.error(f"Error de API de Binance (Code: {e.code}): {e.message}", extra=log_extra)
        # --- INICIO DE MODIFICACIÓN: (Fase 4.0) ---
        csv_writer.log_execution(
            transaction_id=transaction_id, symbol=symbol, side=side,
   
            quantity=str(formatted_quantity), price="N/A",
            status="REJECTED_API", error_message=err_msg
        )
        # --- FIN DE MODIFICACIÓN ---
        raise OrderPlacementError(f"Error de orden de Binance: {e.message}", symbol=symbol) from e

    except Exception as e:
        err_msg = f"Error inesperado: {str(e)}"
        logger.error(f"Error inesperado en colocación de orden: {e}", extra=log_extra, exc_info=True)
    
        # --- INICIO DE MODIFICACIÓN: (Fase 4.0) ---
        csv_writer.log_execution(
            transaction_id=transaction_id, symbol=symbol, side=side,
            quantity=str(formatted_quantity), price="N/A",
            status="FAILED_INTERNAL", error_message=err_msg
        )
        # --- FIN DE MODIFICACIÓN ---
        raise OrderPlacementError(f"Error inesperado en colocación de orden: {e}", symbol=symbol) from e


def place_limit_order(symbol, 
 side, quantity, price, transaction_id="PLACE_LIMIT_ORDER"):
    log_extra = {'transaction_id': transaction_id, 'symbol': symbol}
    quantity_decimal = Decimal(str(quantity))
    price_decimal = Decimal(str(price))

    formatted_quantity = format_quantity(symbol, quantity_decimal)
    formatted_price = format_price(symbol, price_decimal)

    if not formatted_quantity or formatted_quantity <= Decimal(0):
        err_msg = f"Cantidad límite formateada inválida ({formatted_quantity})"
        # --- INICIO DE MODIFICACIÓN: (Fase 4.0) ---
        csv_writer.log_execution(
            transaction_id=transaction_id, symbol=symbol, side=side,
 
            quantity=str(quantity_decimal), price=str(price_decimal),
            status="REJECTED_INTERNAL", error_message=err_msg
        )
        # --- FIN DE MODIFICACIÓN ---
        raise OrderPlacementError(err_msg, symbol=symbol)

    params_order = {
        'symbol': symbol,
        'side': side,
        'type': ORDER_TYPE_LIMIT,
        'timeInForce': TIME_IN_FORCE_GTC,
  
        'quantity': str(formatted_quantity),
        'price': str(formatted_price)
    }
    try:
        order = client.create_order(**params_order)
        logger.info(f"Orden LÍMITE colocada para {symbol}: ID={order.get('orderId')}, Precio={formatted_price}", extra=log_extra)
        # --- INICIO DE MODIFICACIÓN: (Fase 4.0) ---
        csv_writer.log_execution(
            transaction_id=transaction_id, symbol=symbol, side=side,
            quantity=str(formatted_quantity), price=str(formatted_price),
 
            status=order.get('status', 'NEW'), error_message=None
        )
        # --- FIN DE MODIFICACIÓN ---
        return order
    except BinanceOrderException as e:
        err_msg = f"Code: {e.code} - {e.message}"
        logger.error(f"Error de orden límite de Binance: {e.message}", extra=log_extra)
        # --- INICIO DE MODIFICACIÓN: (Fase 4.0) ---
       
        csv_writer.log_execution(
            transaction_id=transaction_id, symbol=symbol, side=side,
            quantity=str(formatted_quantity), price=str(formatted_price),
            status="REJECTED_API", error_message=err_msg
        )
        # --- FIN DE MODIFICACIÓN ---
        raise OrderPlacementError(f"Error de orden límite de Binance: {e.message}", symbol=symbol) from e
    except Exception as e:
        err_msg = f"Error inesperado: {str(e)}"
 
        logger.error(f"Error inesperado en colocación de orden límite: {e}", extra=log_extra, exc_info=True)
        # --- INICIO DE MODIFICACIÓN: (Fase 4.0) ---
        csv_writer.log_execution(
            transaction_id=transaction_id, symbol=symbol, side=side,
            quantity=str(formatted_quantity), price=str(formatted_price),
            status="FAILED_INTERNAL", error_message=err_msg
        )
        # --- FIN DE MODIFICACIÓN ---
        raise OrderPlacementError(f"Error inesperado en colocación de orden límite: {e}", symbol=symbol) from e


def get_account_balance_for_asset(asset, transaction_id="GET_BALANCE"):
    log_extra = {'transaction_id': transaction_id, 'symbol': asset}
    if client is None: raise InitializationError(f"Cliente no disponible para obtener balance de {asset}.")
    try:
        balance_info = client.get_asset_balance(asset=asset)
        if balance_info and 'free' in balance_info:
            return Decimal(balance_info['free'])
        return Decimal('0')
    except Exception as e:
        logger.error(f"Error obteniendo balance para {asset}: {e}", extra=log_extra)
        raise CriticalBinanceError(f"Error obteniendo balance para {asset}: {e}") from e


def get_all_tickers_data(transaction_id="GET_ALL_TICKERS"):
    log_extra = {'transaction_id': transaction_id, 'symbol': 'ALL_TICKERS'}
    if client is None: raise InitializationError("Cliente no disponible para obtener tickers.")
    try:
        tickers = client.get_ticker()
        return tickers
    except Exception as e:
        logger.error(f"Error al obtener todos los tickers de Binance: {e}", extra=log_extra)
        return []


def get_current_spread(symbol, transaction_id="GET_SPREAD"):
    log_extra = {'transaction_id': transaction_id, 'symbol': symbol}
    try:
        ticker = client.get_orderbook_ticker(symbol=symbol)
        bid = Decimal(ticker['bidPrice'])
        ask = Decimal(ticker['askPrice'])
        if ask > 0:
            spread_pct = ((ask - bid) / ask) * 100
            return spread_pct
        return Decimal('Infinity')
    except Exception as e:
        logger.warning(f"No se pudo obtener el spread para {symbol}: {e}", extra=log_extra)
        return Decimal('Infinity')


# --- Funciones Asíncronas (Wrappers para el loop EVENT) ---

async def async_get_klines_data(symbol, interval, limit=500, transaction_id="ASYNC_GET_KLINES"):
    """Wrapper Asíncrono para get_klines_data."""
    # <--- MODIFICACIÓN: Usar AsyncClient si está disponible ---
    aclient = get_async_client()
    log_extra = {'transaction_id': transaction_id, 'symbol': symbol}
    if aclient:
        try:
            klines = await aclient.get_klines(symbol=symbol, interval=interval, limit=limit)
            # Re-aplicar la lógica de formato de 'get_klines_data'
            if not klines:
                logger.warning(f"No se recibieron klines (async) para {symbol} (intervalo {interval}).", extra=log_extra)
                return pd.DataFrame()
            df = pd.DataFrame(klines, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume', 'close_time', 'quote_asset_volume', 'number_of_trades', 'taker_buy_base_asset_volume', 'taker_buy_quote_asset_volume', 'ignore'])
            numeric_cols = ['open', 'high', 'low', 'close', 'volume']
            df[numeric_cols] = df[numeric_cols].apply(pd.to_numeric, errors='coerce')
            for col in numeric_cols:
                df[col] = df[col].map(lambda x: Decimal(str(x)) if pd.notna(x) else None, na_action='ignore')
            df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
            df.attrs['symbol'] = symbol
            return df
        except Exception as e:
            logger.error(f"Error obteniendo klines (async) para {symbol}: {e}", extra=log_extra)
            raise InsufficientDataError(f"Error obteniendo klines (async) para {symbol}: {e}") from e

    # Fallback a to_thread si el cliente asíncrono falló
    return await asyncio.to_thread(
        get_klines_data,
        symbol=symbol,
        interval=interval,
        limit=limit,
        transaction_id=transaction_id
    )


async def async_place_market_order(symbol, side, quantity_asset, transaction_id="ASYNC_PLACE_ORDER", is_retying=False):
    """
    Wrapper Asíncrono para place_market_order.
    NOTA: Esta función mantiene la lógica de reintento de -2010 y logging en CSV,
    por lo que se mantiene el wrapper síncrono (to_thread).
    
    (MODIFICACIÓN FUTURA: Reimplementar 'place_market_order' de forma nativa asíncrona
    si 'async_create_order' no es suficiente).
    """
    return await asyncio.to_thread(
        place_market_order,
        symbol=symbol,
        side=side,
        quantity_asset=quantity_asset,
        transaction_id=transaction_id,
        is_retying=is_retying
    )


async def async_place_limit_order(symbol, side, quantity, price, transaction_id="ASYNC_PLACE_LIMIT"):
    """Wrapper Asíncrono para place_limit_order."""
    return await asyncio.to_thread(
        place_limit_order,
        symbol=symbol,
        side=side,
        quantity=quantity,
        price=price,
        transaction_id=transaction_id
    )


async def async_get_account_balance_for_asset(asset, transaction_id="ASYNC_GET_BALANCE"):
    """Wrapper Asíncrono para get_account_balance_for_asset."""
    # <--- MODIFICACIÓN: Usar AsyncClient ---
    aclient = get_async_client()
    log_extra = {'transaction_id': transaction_id, 'symbol': asset}
    if aclient:
        try:
            balance_info = await aclient.get_asset_balance(asset=asset)
            if balance_info and 'free' in balance_info:
                return Decimal(balance_info['free'])
            return Decimal('0')
        except Exception as e:
            logger.error(f"Error obteniendo balance (async) para {asset}: {e}", extra=log_extra)
            raise CriticalBinanceError(f"Error obteniendo balance (async) para {asset}: {e}") from e

    # Fallback
    return await asyncio.to_thread(
        get_account_balance_for_asset,
        asset=asset,
        transaction_id=transaction_id
    )


async def async_get_all_tickers_data(transaction_id="ASYNC_GET_TICKERS"):
    """Wrapper Asíncrono para get_all_tickers_data."""
    # <--- MODIFICACIÓN: Usar AsyncClient ---
    aclient = get_async_client()
    log_extra = {'transaction_id': transaction_id, 'symbol': 'ALL_TICKERS'}
    if aclient:
        try:
            return await aclient.get_ticker()
        except Exception as e:
            logger.error(f"Error al obtener todos los tickers (async) de Binance: {e}", extra=log_extra)
            return []
    
    # Fallback
    return await asyncio.to_thread(
        get_all_tickers_data,
        transaction_id=transaction_id
    )


async def async_get_current_spread(symbol, transaction_id="ASYNC_GET_SPREAD"):
    """Wrapper Asíncrono para get_current_spread."""
    # <--- MODIFICACIÓN: Usar AsyncClient ---
    aclient = get_async_client()
    log_extra = {'transaction_id': transaction_id, 'symbol': symbol}
    if aclient:
        try:
            ticker = await aclient.get_orderbook_ticker(symbol=symbol)
            bid = Decimal(ticker['bidPrice'])
            ask = Decimal(ticker['askPrice'])
            if ask > 0:
                spread_pct = ((ask - bid) / ask) * 100
                return spread_pct
            return Decimal('Infinity')
        except Exception as e:
            logger.warning(f"No se pudo obtener el spread (async) para {symbol}: {e}", extra=log_extra)
            return Decimal('Infinity')

    # Fallback
    return await asyncio.to_thread(
        get_current_spread,
        symbol=symbol,
        transaction_id=transaction_id
    )

async def async_load_exchange_filters(symbols_list, transaction_id="ASYNC_LOAD_FILTERS"):
    """Wrapper Asíncrono para load_exchange_filters."""
    # <--- MODIFICACIÓN: Usar AsyncClient ---
    global exchange_filters
    log_extra = {'transaction_id': transaction_id, 'symbol': 'EXCH_INFO_ASYNC'}
    aclient = get_async_client()
    if aclient:
        try:
            exchange_info = await aclient.get_exchange_info()
            symbols_in_exchange = {s['symbol'] for s in exchange_info['symbols']}
            temp_filters = {}
            for symbol in symbols_list:
                if symbol in symbols_in_exchange:
                    symbol_info = next((s for s in exchange_info['symbols'] if s['symbol'] == symbol), None)
                    if symbol_info:
                        temp_filters[symbol] = {f['filterType']: f for f in symbol_info['filters']}
            exchange_filters.update(temp_filters)
            logger.info(f"Filtros (async) cargados/actualizados para {len(temp_filters)} símbolos. Total en caché: {len(exchange_filters)}", extra=log_extra)
            return
        except Exception as e:
            logger.error(f"Error al cargar filtros (async) del exchange: {e}", extra=log_extra)
            raise CriticalBinanceError(f"Error al cargar filtros (async) del exchange: {e}") from e

    # Fallback
    return await asyncio.to_thread(
        load_exchange_filters,
        symbols_list=symbols_list,
        transaction_id=transaction_id
    )

# ---------------------------------------------------------------------------
# --- INICIO DE MODIFICACIÓN: Funciones Asíncronas para Protection Manager ---
# ---------------------------------------------------------------------------

async def async_create_order(
    client: AsyncClient, 
    symbol: str, 
    side: str, 
    type_: str, # Renombrado de 'type' a 'type_' para evitar conflicto de keyword
    quantity: str, 
    transaction_id: str, 
    **kwargs
):
    """
    Wrapper Asíncrono genérico para client.create_order (SPOT).
    Utilizado por el Protection Manager.
    """
    log_extra = {'transaction_id': transaction_id, 'symbol': symbol}
    if client is None:
        client = get_async_client()
        if client is None:
            raise InitializationError("AsyncClient no está inicializado.")
    
    params = {
        'symbol': symbol,
        'side': side,
        'type': type_,
        'quantity': quantity,
        **kwargs
    }
    try:
        # Usar la función nativa de la librería
        order = await client.create_order(**params)
        csv_writer.log_execution(
            transaction_id=transaction_id, symbol=symbol, side=side,
            quantity=quantity, price=params.get('price', "N/A"),
            status=order.get('status', 'NEW'), error_message=None
        )
        return order
    except (BinanceAPIException, BinanceOrderException) as e:
        err_msg = f"Code: {e.code} - {e.message}"
        logger.error(f"Error API/Order (async_create_order) para {symbol}: {err_msg}", extra=log_extra)
        csv_writer.log_execution(
            transaction_id=transaction_id, symbol=symbol, side=side,
            quantity=quantity, price=params.get('price', "N/A"),
            status="REJECTED_API", error_message=err_msg
        )
        raise
    except Exception as e:
        err_msg = f"Error inesperado (async_create_order) para {symbol}: {e}"
        logger.error(err_msg, extra=log_extra, exc_info=True)
        csv_writer.log_execution(
            transaction_id=transaction_id, symbol=symbol, side=side,
            quantity=quantity, price=params.get('price', "N/A"),
            status="FAILED_INTERNAL", error_message=err_msg
        )
        raise

async def async_create_oco_order(
    client: AsyncClient, 
    symbol: str, 
    side: str, 
    quantity: str, 
    price: str, 
    stopPrice: str, 
    stopLimitPrice: str, 
    transaction_id: str, 
    **kwargs
):
    """
    Wrapper Asíncrono para client.create_oco_order (SPOT).
    """
    log_extra = {'transaction_id': transaction_id, 'symbol': symbol}
    if client is None:
        client = get_async_client()
        if client is None:
            raise InitializationError("AsyncClient no está inicializado.")

    params = {
        'symbol': symbol,
        'side': side,
        'quantity': quantity,
        'price': price, # Precio Límite del TP
        'stopPrice': stopPrice, # Precio Trigger del SL
        'stopLimitPrice': stopLimitPrice, # Precio Límite del SL
        **kwargs
    }
    try:
        # La librería python-binance maneja OCO con 'order_oco'
        oco_order = await client.order_oco(
            symbol=symbol,
            side=side,
            quantity=quantity,
            price=price,
            stopPrice=stopPrice,
            stopLimitPrice=stopLimitPrice,
            **kwargs
        )
        logger.info(f"Orden OCO colocada exitosamente para {symbol}", extra=log_extra)
        # Loggear ambas órdenes (TP y SL)
        if oco_order.get('orders'): # 'orders' es la clave para OCO
            for order_report in oco_order.get('orderReports', []):
                o_type = order_report.get('type', 'UNKNOWN')
                csv_writer.log_execution(
                    transaction_id=f"{transaction_id}_{o_type}", symbol=symbol, side=side,
                    quantity=order_report.get('origQty', quantity), 
                    price=order_report.get('price') or order_report.get('stopPrice'),
                    status=order_report.get('status', 'NEW'), error_message=None
                )
        return oco_order
    except (BinanceAPIException, BinanceOrderException) as e:
        err_msg = f"Code: {e.code} - {e.message}"
        logger.error(f"Error API/Order (async_create_oco_order) para {symbol}: {err_msg}", extra=log_extra)
        csv_writer.log_execution(
            transaction_id=transaction_id, symbol=symbol, side=side,
            quantity=quantity, price=price,
            status="REJECTED_API", error_message=f"OCO Failure: {err_msg}"
        )
        raise
    except Exception as e:
        err_msg = f"Error inesperado (async_create_oco_order) para {symbol}: {e}"
        logger.error(err_msg, extra=log_extra, exc_info=True)
        csv_writer.log_execution(
            transaction_id=transaction_id, symbol=symbol, side=side,
            quantity=quantity, price=price,
            status="FAILED_INTERNAL", error_message=f"OCO Failure: {err_msg}"
        )
        raise

async def async_cancel_oco_order(client: AsyncClient, symbol: str, orderListId: int, transaction_id: str):
    """Wrapper Asíncrono para client.cancel_oco_order (SPOT)."""
    log_extra = {'transaction_id': transaction_id, 'symbol': symbol}
    if client is None:
        client = get_async_client()
        if client is None:
            raise InitializationError("AsyncClient no está inicializado.")
    try:
        response = await client.cancel_oco_order(symbol=symbol, orderListId=orderListId)
        logger.info(f"Orden OCO {orderListId} cancelada.", extra=log_extra)
        return response
    except Exception as e:
        logger.error(f"Error cancelando OCO {orderListId}: {e}", extra=log_extra)
        raise

async def async_cancel_order(client: AsyncClient, symbol: str, orderId: int, transaction_id: str):
    """Wrapper Asíncrono para client.cancel_order (SPOT)."""
    log_extra = {'transaction_id': transaction_id, 'symbol': symbol}
    if client is None:
        client = get_async_client()
        if client is None:
            raise InitializationError("AsyncClient no está inicializado.")
    try:
        response = await client.cancel_order(symbol=symbol, orderId=orderId)
        logger.info(f"Orden SPOT {orderId} cancelada.", extra=log_extra)
        return response
    except Exception as e:
        logger.error(f"Error cancelando orden SPOT {orderId}: {e}", extra=log_extra)
        raise

async def async_futures_create_order(
    client: AsyncClient, 
    symbol: str, 
    side: str, 
    type_: str, # Renombrado de 'type' a 'type_'
    transaction_id: str, 
    **kwargs
):
    """
    Wrapper Asíncrono para client.futures_create_order (FUTURES).
    """
    log_extra = {'transaction_id': transaction_id, 'symbol': symbol}
    if client is None:
        client = get_async_client()
        if client is None:
            raise InitializationError("AsyncClient no está inicializado.")
    
    params = {
        'symbol': symbol,
        'side': side,
        'type': type_,
        **kwargs
    }
    try:
        order = await client.futures_create_order(**params)
        csv_writer.log_execution(
            transaction_id=transaction_id, symbol=symbol, side=side,
            quantity=params.get('quantity', "N/A"), 
            price=params.get('price') or params.get('stopPrice', "N/A"),
            status=order.get('status', 'NEW'), error_message=None
        )
        return order
    except (BinanceAPIException, BinanceOrderException) as e:
        err_msg = f"Code: {e.code} - {e.message}"
        logger.error(f"Error API/Order (async_futures_create_order) para {symbol}: {err_msg}", extra=log_extra)
        csv_writer.log_execution(
            transaction_id=transaction_id, symbol=symbol, side=side,
            quantity=params.get('quantity', "N/A"), 
            price=params.get('price') or params.get('stopPrice', "N/A"),
            status="REJECTED_API", error_message=err_msg
        )
        raise
    except Exception as e:
        err_msg = f"Error inesperado (async_futures_create_order) para {symbol}: {e}"
        logger.error(err_msg, extra=log_extra, exc_info=True)
        csv_writer.log_execution(
            transaction_id=transaction_id, symbol=symbol, side=side,
            quantity=params.get('quantity', "N/A"), 
            price=params.get('price') or params.get('stopPrice', "N/A"),
            status="FAILED_INTERNAL", error_message=err_msg
        )
        raise

async def async_futures_cancel_order(client: AsyncClient, symbol: str, orderId: int, transaction_id: str):
    """Wrapper Asíncrono para client.futures_cancel_order (FUTURES)."""
    log_extra = {'transaction_id': transaction_id, 'symbol': symbol}
    if client is None:
        client = get_async_client()
        if client is None:
            raise InitializationError("AsyncClient no está inicializado.")
    try:
        response = await client.futures_cancel_order(symbol=symbol, orderId=orderId)
        logger.info(f"Orden FUTURES {orderId} cancelada.", extra=log_extra)
        return response
    except Exception as e:
        logger.error(f"Error cancelando orden FUTURES {orderId}: {e}", extra=log_extra)
        raise

async def async_stream_agg_trades(symbol: str, timeout: int = 300):
    """
    Se conecta al stream de aggTrades usando aiohttp (independiente de python-binance)
    para el emergency watcher.
    """
    url = f"wss://stream.binance.com:9443/ws/{symbol.lower()}@aggTrade"
    log_extra = {'symbol': symbol, 'transaction_id': 'AGG_TRADE_WATCHER'}
    
    try:
        async with aiohttp.ClientSession() as session:
            async with session.ws_connect(url, timeout=timeout, heartbeat=60) as ws:
                logger.info(f"Watcher de Emergencia (aggTrade stream) conectado para {symbol}", extra=log_extra)
                async for msg in ws:
                    if msg.type == aiohttp.WSMsgType.TEXT:
                        try:
                            data = json.loads(msg.data)
                            # Formato esperado: {'e': 'aggTrade', 'E': ..., 's': 'BTCUSDT', 'a': ..., 'p': 'price', 'q': 'qty', 'f': ..., 'l': ..., 'T': ..., 'm': bool}
                            trade = {
                                'price': data.get('p'),
                                'quantity': data.get('q'),
                                'is_buyer_maker': data.get('m'),
                                'timestamp': data.get('T')
                            }
                            yield trade
                        except json.JSONDecodeError:
                            logger.warning("Error decodificando aggTrade JSON", extra=log_extra)
                    elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                        logger.warning(f"aggTrade stream desconectado.", extra=log_extra)
                        break
    except asyncio.TimeoutError:
        logger.warning(f"aggTrade stream timeout.", extra=log_extra)
        raise
    except Exception as e:
        logger.error(f"Error en aggTrade stream: {e}", extra=log_extra, exc_info=True)
        raise

# --- FIN DE MODIFICACIÓN ---