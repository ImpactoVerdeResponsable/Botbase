# main.py
import time
import datetime
import traceback
import uuid
import sys
import pandas as pd
import collections  # Necesario para collections.deque (rate limiting)
import asyncio
from typing import Optional  # <--- AÑADIDO: Requerido para Optional[AsyncClient]

from unified_logger import get_logger
from io_worker import io_worker  # Importar el worker de E/S

import config
import utils
import binance_interaction as bi
import position_management as pm
import async_stream_manager
from microstructure_manager import microstructure_manager
import market_analyzer
# --- INICIO DE MODIFICACIÓN: (FIX ModuleNotFoundError) ---
from strategies import strategy_hub  # Corregido (antes 'import strategy_hub')
# --- FIN DE MODIFICACIÓN ---
import dynamic_params
import technical_analysis as ta
import market_scanner
import filters_manager
# --- INICIO DE MODIFICACIÓN: (Implementación OCO) ---
import protection_manager
from binance import AsyncClient # <--- MODIFICACIÓN: Importar AsyncClient
# --- FIN DE MODIFICACIÓN ---

from binance.enums import SIDE_BUY, ORDER_STATUS_FILLED
from exceptions import ConfigError, CriticalBinanceError, InitializationError, OrderPlacementError, InsufficientDataError, FiltersNotFoundError, ConnectionError

logger = get_logger(__name__)

# --- Límites de Ejecución y Rate Limiting (Global) ---
MAX_ITERATIONS_PER_MINUTE = 60
MAX_API_CALLS_PER_MINUTE = 1200
api_call_timestamps = collections.deque(maxlen=MAX_API_CALLS_PER_MINUTE)


def check_rate_limits(transaction_id):
    """Implementa rate limiting SÍNCRONO (bloqueante) para el modo POLLING."""
    current_time = time.time()
    one_minute_ago = current_time - 60

    while api_call_timestamps and api_call_timestamps[0] < one_minute_ago:
        api_call_timestamps.popleft()

    if len(api_call_timestamps) >= MAX_API_CALLS_PER_MINUTE:
        wait_time = 60 - (current_time - api_call_timestamps[0])
        logger.warning(f"🚨 API Rate Limit ALCANZADO ({len(api_call_timestamps)} llamadas/min). Esperando {wait_time:.2f}s.", extra={'symbol': 'RATE_LIMIT', 'transaction_id': transaction_id})
        if wait_time > 0:
            time.sleep(wait_time)  # <-- BLOQUEANTE
            return False

    return True


def record_api_call():
    """Registra una llamada API para el rate limiting (no bloqueante)."""
    api_call_timestamps.append(time.time())


async def async_check_rate_limits(transaction_id):
    """Implementa rate limiting ASÍNCRONO (no bloqueante) para el modo EVENT."""
    current_time = time.time()
    one_minute_ago = current_time - 60

    while api_call_timestamps and api_call_timestamps[0] < one_minute_ago:
        api_call_timestamps.popleft()

    if len(api_call_timestamps) >= MAX_API_CALLS_PER_MINUTE:
        wait_time = 60 - (current_time - api_call_timestamps[0])
        logger.warning(f"🚨 API Rate Limit ALCANZADO ({len(api_call_timestamps)} llamadas/min). Esperando {wait_time:.2f}s.", extra={'symbol': 'RATE_LIMIT_ASYNC', 'transaction_id': transaction_id})
        if wait_time > 0:
            await asyncio.sleep(wait_time)  # <-- NO BLOQUEANTE
            return False

    return True


def main_bot_loop():
    """Bucle principal de ejecución síncrona (Polling)."""
    bot_session_id = str(uuid.uuid4())[:8]
    log_extra_session = {'transaction_id': bot_session_id, 'symbol': 'BOT_SESSION'}

    try:
        logger.info(f"🚀 Iniciando {config.get_param('GLOBAL_SETTINGS.BOT_NAME')} (Sesión: {bot_session_id})...", extra=log_extra_session)
        current_config_cache = config.load_config(force_reload=True)
        utils.validate_config_keys(current_config_cache, transaction_id=f"{bot_session_id}_ValConfRun")

        if not check_rate_limits(f"{bot_session_id}_Init"):
            sys.exit(1)

        bi.initialize_binance_client(transaction_id=f"{bot_session_id}_InitBinance")
        pm.load_app_state(transaction_id=f"{bot_session_id}_LoadAppState")

        logger.info("Ejecutando escaneo de mercado inicial...", extra=log_extra_session)
        top_symbols = market_scanner.get_top_symbols(transaction_id=f"{bot_session_id}_InitialScan")

        active_positions_symbols = list(pm.get_all_active_positions().keys())
        symbols_to_filter = list(set(top_symbols) | set(active_positions_symbols))

        if not symbols_to_filter:
            logger.critical("🚨 Escaneo inicial falló y no hay posiciones activas. No se pueden determinar símbolos para operar. Saliendo.", extra=log_extra_session)
            return

        logger.info(f"Escaneo inicial completado. Símbolos seleccionados: {top_symbols}", extra=log_extra_session)
        logger.info(f"Cargando filtros para {len(symbols_to_filter)} símbolos (Escaneados + Activos)...", extra=log_extra_session)

        if not filters_manager.filters_manager.ensure_filters_loaded(list(symbols_to_filter), f"{bot_session_id}_LoadFilters"):
            logger.critical("🚨 Error fatal al cargar filtros iniciales. Saliendo.", extra=log_extra_session)
            return

        strategies_config = config.get_param("STRATEGIES_CONFIG", return_dict_for_complex=True)
        interval = config.get_param("GLOBAL_SETTINGS.INTERVAL")
        max_concurrent_positions = int(config.get_param("GLOBAL_SETTINGS.MAX_CONCURRENT_POSITIONS", 3))

        if strategies_config.get("MicrostructureSweep", {}).get("enabled", False):
            logger.warning(
                "La estrategia 'MicrostructureSweep' está HABILITADA, pero el bot está en modo 'POLLING'. "
                "Esta estrategia requiere streams de microestructura y SÓLO funcionará en modo 'EVENT'.",
                extra=log_extra_session
            )

        def should_trade_current_time():
            current_hour = datetime.datetime.utcnow().hour
            if 8 <= current_hour <= 16:
                return 1.0
            elif 2 <= current_hour <= 4:
                return 0.5
            else:
                return 0.7

        while True:
            loop_start_time = time.time()
            iter_id_base = utils.generate_unique_id()
            log_extra_iter = {'transaction_id': iter_id_base, 'symbol': 'LOOP_ITER'}

            logger.info(f"📢 {config.get_param('GLOBAL_SETTINGS.BOT_NAME')} {interval} --- ⏱️ Nueva Iteración (ID: {iter_id_base}) ---", extra=log_extra_iter)

            try:
                if not check_rate_limits(iter_id_base):
                    continue

                time_multiplier = should_trade_current_time()
                if time_multiplier < 0.7:
                    logger.info(f"Reducción horaria activa: {time_multiplier * 100}% allocation", extra=log_extra_iter)

                config.load_config(force_reload=False)
                utils.handle_daily_backup(transaction_id=f"{iter_id_base}_Backup")

                current_top_symbols = market_scanner.get_top_symbols(transaction_id=f"{iter_id_base}_ScanCycle")
                active_positions = pm.get_all_active_positions()
                symbols_to_process = set(current_top_symbols) | set(active_positions.keys())

                if not current_top_symbols:
                    active_pos_syms = list(active_positions.keys())
                    if active_pos_syms:
                        logger.warning(f"Escaneo cíclico falló, usando símbolos de posiciones activas: {active_pos_syms}", extra=log_extra_iter)
                    else:
                        logger.error("Escaneo cíclico falló y no hay posiciones activas. Saltando ciclo.", extra=log_extra_iter)
                        utils.wait_for_next_cycle(loop_start_time, iter_id_base)
                        continue

                if not filters_manager.filters_manager.ensure_filters_loaded(list(symbols_to_process), f"{iter_id_base}_EnsureFilters"):
                    logger.error("❌ No se pudieron cargar filtros necesarios. Saltando ciclo.", extra=log_extra_iter)
                    utils.wait_for_next_cycle(loop_start_time, iter_id_base)
                    continue

                active_positions = pm.get_all_active_positions()
                symbols_to_process = set(current_top_symbols) | set(active_positions.keys())

                logger.info(f"🔎 Buscando nuevas señales en {current_top_symbols}. Gestionando {len(active_positions)} pos. ({len(active_positions)}/{max_concurrent_positions} pos.)", extra=log_extra_iter)

                for symbol in symbols_to_process:
                    iter_id_sym = f"{iter_id_base}_{symbol[:4]}"
                    log_extra_sym = {'transaction_id': iter_id_sym, 'symbol': symbol}

                    try:
                        record_api_call()
                        df_klines = bi.get_klines_data(symbol, interval, limit=500, transaction_id=iter_id_sym)
                        if df_klines is None or df_klines.empty or len(df_klines) < 50:
                            logger.warning(f"Datos insuficientes o inválidos para {symbol}. Saltando.", extra=log_extra_sym)
                            continue
                    except InsufficientDataError as e:
                        logger.warning(f"No se pudieron obtener klines para {symbol}: {e}. Saltando.", extra=log_extra_sym)
                        continue
                    except CriticalBinanceError as e:
                        logger.error(f"Error crítico de Binance al obtener klines para {symbol}: {e}. Saltando símbolo.", extra=log_extra_sym)
                        continue
                    except ConnectionError as e:
                        logger.error(f"🔌 Connection error during klines fetch: {e}", extra=log_extra_sym)
                        continue

                    if symbol in active_positions:
                        try:
                            bi.get_symbol_filters(symbol, transaction_id=f"{iter_id_sym}_PreCheck")
                        except FiltersNotFoundError:
                            logger.critical(f"FATAL: No se puede gestionar o cerrar {symbol}. FiltersManager falló.", extra=log_extra_sym)
                            continue

                        pm.check_and_manage_existing_position(symbol, df_klines, transaction_id=iter_id_sym)
                        if pm.get_active_position(symbol) is not None:
                            logger.debug(f"Posición activa encontrada para {symbol}, no buscar nueva señal.", extra=log_extra_sym)
                            continue

                    current_active_count = len(pm.get_all_active_positions())
                    if current_active_count >= max_concurrent_positions:
                        if symbol not in active_positions:
                            logger.debug(f"Límite de posiciones ({max_concurrent_positions}) alcanzado. No buscar nueva señal para {symbol}.", extra=log_extra_sym)
                        continue

                    if pm.is_global_circuit_breaker_tripped():
                        logger.warning("Circuit Breaker GLOBAL activado. Saltando búsqueda de nuevas señales.", extra=log_extra_iter)
                        break

                    vol_ratio = dynamic_params.calculate_volatility_ratio(
                        df_klines,
                        config.get_param("MARKET_ANALYZER_CONFIG.ATR_SHORT_PERIOD"),
                        config.get_param("MARKET_ANALYZER_CONFIG.ATR_LONG_PERIOD"),
                        symbol,
                        iter_id_sym
                    )
                    try:
                        record_api_call()
                        df_macro = bi.get_klines_data(symbol, "4h", limit=201, transaction_id=f"{iter_id_sym}_Macro")
                        ema_macro = ta.get_ema(df_macro, length=200) if df_macro is not None else None
                        macro_trend_is_up = (ema_macro is not None and not ema_macro.empty and ema_macro.iloc[-1] is not None and df_klines['close'].iloc[-1] > ema_macro.iloc[-1])
                    except Exception as e_macro:
                        logger.warning(f"No se pudo determinar la macro tendencia para {symbol}: {e_macro}. Asumiendo 'arriba'.", extra=log_extra_sym)
                        macro_trend_is_up = True

                    market_state = market_analyzer.determine_market_state(df_klines, strategies_config, log_extra_sym)
                    logger.info(f"📊 Estado de Mercado para {symbol}: {market_state}", extra=log_extra_sym)

                    signal = strategy_hub.get_signal(market_state, df_klines, strategies_config, vol_ratio, macro_trend_is_up, iter_id_sym, **{})

                    if signal:
                        utils.log_trading_metrics(symbol, signal, market_state, vol_ratio, df_klines, iter_id_sym)

                    if signal:
                        if pm.is_strategy_on_cooldown(symbol, signal.get("strategy_name", "Desconocida")):
                            logger.info(f"❄️ Estrategia '{signal.get('strategy_name', 'Desconocida')}' en cooldown para {symbol}. Señal ignorada.", extra=log_extra_sym)
                            continue

                        if signal.get("strategy_name") == "MicrostructureSweep":
                            logger.warning(f"Ignorando señal de MicrostructureSweep en modo POLLING para {symbol}.", extra=log_extra_sym)
                            continue

                        logger.info(f"🔥 Señal {signal.get('type', 'LONG')} encontrada para {symbol} por {signal.get('strategy_name', 'Desconocida')}", extra=log_extra_sym)

                        trade_amount, sl_price, tp_price = utils.calculate_position_details(symbol, df_klines, signal, vol_ratio, transaction_id=iter_id_sym)

                        if trade_amount and config.get_param("SHADOW_OPTUNA.ENABLED", default_value=False):
                            from shadow_logger import log_signal
                            signal['quantity'] = str(trade_amount)
                            log_signal(signal, df_klines, iter_id_sym)

                        if trade_amount and sl_price and tp_price:
                            try:
                                if len(pm.get_all_active_positions()) >= max_concurrent_positions:
                                    logger.warning(f"Límite de posiciones ({max_concurrent_positions}) alcanzado JUSTO ANTES de abrir {symbol}. Abortando.", extra=log_extra_sym)
                                    continue

                                record_api_call()
                                order_result = bi.place_market_order(symbol, SIDE_BUY, trade_amount, transaction_id=iter_id_sym)
                                if order_result and order_result.get('status') == ORDER_STATUS_FILLED:
                                    sl_atr_mult = dynamic_params.get_dynamic_risk_multiplier(vol_ratio, symbol)
                                    # --- MODIFICACIÓN OCO: Usar la función síncrona ---
                                    pm.open_new_position(symbol, order_result, sl_price, tp_price, signal, sl_atr_mult, iter_id_sym)
                            except OrderPlacementError as e:
                                logger.error(f"Error de colocación de orden para {symbol}: {e}", exc_info=True, extra=log_extra_sym)
                            except CriticalBinanceError as e:
                                logger.error(f"Error crítico de Binance al colocar orden para {symbol}: {e}. Intentando continuar.", exc_info=True, extra=log_extra_sym)

            except ConfigError as e:
                logger.critical(f"🚨 Error de Configuración Crítico: {e}. Deteniendo el bot.", exc_info=True, extra=log_extra_iter)
                break
            except CriticalBinanceError as e:
                logger.error(f"🚨 Error Crítico de Binance en el bucle principal (ID: {iter_id_base}): {e}. Intentando continuar...", exc_info=True, extra=log_extra_iter)
            except MemoryError:
                logger.critical("🚨 MEMORY ERROR - Reiniciando bot debido a falta de memoria.", extra=log_extra_iter)
                sys.exit(1)
            except ConnectionError as e:
                logger.error(f"🔌 Connection error: {e}. Espera prolongada.", extra=log_extra_iter)
                time.sleep(60)
            except Exception as e_loop_general:
                logger.error(f"Error GENERAL INESPERADO en bucle (ID: {iter_id_base}): {e_loop_general}", exc_info=True, extra=log_extra_iter)

            utils.wait_for_next_cycle(loop_start_time, iter_id_base)

    except KeyboardInterrupt:
        logger.info("🛑 Interrupción por teclado.", extra={'symbol': 'GLOBAL_EXIT', 'transaction_id': 'EXIT'})
    except InitializationError as e:
        logger.critical(f"🚨 Error de Inicialización Crítico: {e}. El bot no puede iniciar.", exc_info=True, extra=log_extra_session)
    except ConfigError as e:
        logger.critical(f"🚨 Error de Configuración Crítico al iniciar: {e}. El bot no puede iniciar.", exc_info=True, extra=log_extra_session)
    except Exception as e_global:
        logger.critical(f"💥 Error GLOBAL NO CAPTURADO: {e_global}", exc_info=True, extra={'symbol': 'GLOBAL_CRASH', 'transaction_id': 'CRASH'})

    finally:
        logger.info(f"🏁 Bot (Sesión: {bot_session_id}) finalizando... Encolando guardado de estado final.", extra=log_extra_session)
        pm.save_app_state(transaction_id=f"{bot_session_id}_FinalSave")


async def main_event_driven_loop():
    """
    Bucle principal de ejecución asíncrona (Event-Driven).
    """
    bot_session_id = str(uuid.uuid4())[:8]
    log_extra_session = {'transaction_id': bot_session_id, 'symbol': 'BOT_SESSION_ASYNC'}
    stop_event = asyncio.Event()
    
    # --- INICIO MODIFICACIÓN OCO: Inicializar cliente asíncrono ---
    aclient: Optional[AsyncClient] = None
    # --- FIN MODIFICACIÓN ---

    try:
        logger.info(f"🚀 Iniciando {config.get_param('GLOBAL_SETTINGS.BOT_NAME')} (Sesión ASYNC: {bot_session_id})...", extra=log_extra_session)
        current_config_cache = config.load_config(force_reload=True)
        utils.validate_config_keys(current_config_cache, transaction_id=f"{bot_session_id}_ValConfRun")

        if not await async_check_rate_limits(f"{bot_session_id}_Init"):
            sys.exit(1)

        bi.initialize_binance_client(transaction_id=f"{bot_session_id}_InitBinance")
        # --- INICIO MODIFICACIÓN OCO: Obtener cliente asíncrono ---
        aclient = bi.get_async_client()
        if aclient is None:
            raise InitializationError("No se pudo inicializar el AsyncClient de Binance.")
        # --- FIN MODIFICACIÓN ---

        pm.load_app_state(transaction_id=f"{bot_session_id}_LoadAppState")
         
        # --- INICIO MODIFICACIÓN OCO: Reconciliación en arranque ---
        logger.info("Reconciliando protecciones de exchange al inicio...", extra=log_extra_session)
        await protection_manager.reconcile_protections_on_startup(aclient, transaction_id=f"{bot_session_id}_Reconcile")
        # --- FIN MODIFICACIÓN ---

        logger.info("Ejecutando escaneo de mercado inicial (Async)...", extra=log_extra_session)
        top_symbols = await asyncio.to_thread(market_scanner.get_top_symbols, transaction_id=f"{bot_session_id}_InitialScan")

        active_positions_symbols = list(pm.get_all_active_positions().keys())
        symbols_to_process = list(set(top_symbols) | set(active_positions_symbols))

        if not symbols_to_process:
            logger.critical("🚨 Escaneo inicial falló y no hay posiciones activas (Async). Saliendo.", extra=log_extra_session)
            return

        logger.info(f"Cargando filtros para {len(symbols_to_process)} símbolos (Async)...", extra=log_extra_session)
        await bi.async_load_exchange_filters(symbols_to_process, transaction_id=f"{bot_session_id}_LoadFilters")

        strategies_config = config.get_param("STRATEGIES_CONFIG", return_dict_for_complex=True)
        interval = config.get_param("GLOBAL_SETTINGS.INTERVAL")
        max_concurrent_positions = int(config.get_param("GLOBAL_SETTINGS.MAX_CONCURRENT_POSITIONS", 3))

        microstructure_symbols = []
        if strategies_config.get("MicrostructureSweep", {}).get("enabled", False):
            microstructure_symbols = symbols_to_process
            logger.info(f"MicrostructureSweep habilitada. Suscribiendo a streams OB para {len(microstructure_symbols)} símbolos.", extra=log_extra_session)
        else:
            logger.info("Estrategias de microestructura no habilitadas. Saltando suscripciones OB.", extra=log_extra_session)
        
        for symbol in microstructure_symbols:
            microstructure_manager.subscribe(symbol)

        async def process_candle_event(symbol: str, kline_data: dict):
            """
            Callback ejecutado por async_stream_manager CADA VEZ que una vela se cierra.
            """
            iter_id_sym = f"{utils.generate_unique_id()}_{symbol[:4]}"
            log_extra_sym = {'transaction_id': iter_id_sym, 'symbol': symbol}

            logger.debug(f"Evento de vela cerrada recibido para {symbol}", extra=log_extra_sym)

            try:
                if not await async_check_rate_limits(iter_id_sym):
                    logger.warning(f"Rate limit alcanzado, saltando evento para {symbol}", extra=log_extra_sym)
                    return

                config.load_config(force_reload=False)

                record_api_call()
                df_klines = await bi.async_get_klines_data(symbol, interval, limit=500, transaction_id=iter_id_sym)
                if df_klines is None or df_klines.empty or len(df_klines) < 50:
                    logger.warning(f"Datos insuficientes o inválidos para {symbol} (Async). Saltando.", extra=log_extra_sym)
                    return

                active_positions = pm.get_all_active_positions()
                if symbol in active_positions:
                    await pm.async_check_and_manage_existing_position(symbol, df_klines, transaction_id=iter_id_sym)
                    if pm.get_active_position(symbol) is not None:
                        logger.debug(f"Posición activa gestionada para {symbol}, no buscar nueva señal.", extra=log_extra_sym)
                        return

                current_active_count = len(pm.get_all_active_positions())
                if current_active_count >= max_concurrent_positions:
                    logger.debug(f"Límite de posiciones ({max_concurrent_positions}) alcanzado. No buscar nueva señal para {symbol}.", extra=log_extra_sym)
                    return

                if pm.is_global_circuit_breaker_tripped():
                    logger.warning(f"Circuit Breaker GLOBAL activado (Async). Saltando búsqueda de nuevas señales para {symbol}.", extra=log_extra_sym)
                    return

                vol_ratio = dynamic_params.calculate_volatility_ratio(
                    df_klines,
                    config.get_param("MARKET_ANALYZER_CONFIG.ATR_SHORT_PERIOD"),
                    config.get_param("MARKET_ANALYZER_CONFIG.ATR_LONG_PERIOD"),
                    symbol,
                    iter_id_sym
                )

                record_api_call()
                df_macro = await bi.async_get_klines_data(symbol, "4h", limit=201, transaction_id=f"{iter_id_sym}_Macro")
                ema_macro = ta.get_ema(df_macro, length=200) if df_macro is not None else None
                macro_trend_is_up = (ema_macro is not None and not ema_macro.empty and ema_macro.iloc[-1] is not None and df_klines['close'].iloc[-1] > ema_macro.iloc[-1])

                market_state = market_analyzer.determine_market_state(df_klines, strategies_config, log_extra_sym)
                logger.info(f"📊 Estado de Mercado para {symbol} (Async): {market_state}", extra=log_extra_sym)

                microstructure_snapshot = microstructure_manager.get_snapshot(symbol)
                kwargs_for_signal = {}
                
                if microstructure_snapshot:
                    try:
                        bids = microstructure_snapshot.get('bids', [])
                        asks = microstructure_snapshot.get('asks', [])
                        if bids and asks and bids[0][0] > 0 and asks[0][0] > 0:
                            best_bid = bids[0][0]
                            best_ask = asks[0][0]
                            spread_pct = ((best_ask - best_bid) / best_ask) * 100
                            kwargs_for_signal['spread_bp'] = float(spread_pct * 100)
                        
                        kwargs_for_signal['quote_cancel_ratio'] = 7.0 # Dummy
                        kwargs_for_signal['cvd_tick'] = 1.0 # Dummy (positivo)
                        kwargs_for_signal['a1_stuffing_detected'] = (symbol == 'ZENUSDT') 
                    except Exception as e_ms:
                        logger.warning(f"Error calculando stub microstructure metrics para {symbol}: {e_ms}", extra=log_extra_sym)
                    
                    kwargs_for_signal['microstructure_data'] = kwargs_for_signal

                signal = strategy_hub.get_signal(
                    market_state, 
                    df_klines, 
                    strategies_config, 
                    vol_ratio, 
                    macro_trend_is_up, 
                    iter_id_sym,
                    **kwargs_for_signal
                )

                if signal:
                    utils.log_trading_metrics(symbol, signal, market_state, vol_ratio, df_klines, iter_id_sym)

                if signal:
                    if pm.is_strategy_on_cooldown(symbol, signal.get("strategy_name", "Desconocida")):
                        logger.info(f"❄️ Estrategia '{signal.get('strategy_name', 'Desconocida')}' en cooldown para {symbol} (Async). Señal ignorada.", extra=log_extra_sym)
                        return

                    logger.info(f"🔥 Señal {signal.get('type', 'LONG')} encontrada para {symbol} por {signal.get('strategy_name', 'Desconocida')} (Async)", extra=log_extra_sym)

                    trade_amount, sl_price, tp_price = utils.calculate_position_details(symbol, df_klines, signal, vol_ratio, transaction_id=iter_id_sym)

                    if trade_amount and config.get_param("SHADOW_OPTUNA.ENABLED", default_value=False):
                        from shadow_logger import log_signal
                        signal['quantity'] = str(trade_amount)
                        log_signal(signal, df_klines, iter_id_sym)

                    if trade_amount and sl_price and tp_price:
                        record_api_call()
                        
                        # --- INICIO MODIFICACIÓN OCO: Llamar a async_place_market_order ---
                        # (La lógica de OCO está ahora dentro de 'async_open_new_position')
                        order_result = await bi.async_place_market_order(symbol, SIDE_BUY, trade_amount, transaction_id=iter_id_sym)
                        
                        if order_result and order_result.get('status') == ORDER_STATUS_FILLED:
                            sl_atr_mult = dynamic_params.get_dynamic_risk_multiplier(vol_ratio, symbol)
                            
                            # Llamar a la nueva función asíncrona que maneja OCO
                            await pm.async_open_new_position(
                                symbol, 
                                order_result, 
                                sl_price, 
                                tp_price, 
                                signal, 
                                sl_atr_mult, 
                                iter_id_sym,
                                client=aclient,
                                market_type="SPOT" # Asumir SPOT
                            )
                        # --- FIN MODIFICACIÓN OCO ---

            except Exception as e_candle:
                logger.error(f"Error GENERAL INESPERADO en process_candle_event para {symbol}: {e_candle}", exc_info=True, extra=log_extra_sym)
        
        logger.info(f"Iniciando Async Stream Manager para {len(symbols_to_process)} símbolos...", extra=log_extra_session)
        await async_stream_manager.start_kline_streams(
            symbols_list=symbols_to_process,
            candle_processor_callback=process_candle_event,
            stop_event=stop_event
        )

    except KeyboardInterrupt:
        logger.info("🛑 Interrupción por teclado (Async). Estableciendo stop_event...", extra=log_extra_session)
        stop_event.set()
    except InitializationError as e:
        logger.critical(f"🚨 Error de Inicialización Crítico (Async): {e}. El bot no puede iniciar.", exc_info=True, extra=log_extra_session)
    except ConfigError as e:
        logger.critical(f"🚨 Error de Configuración Crítico al iniciar (Async): {e}. El bot no puede iniciar.", exc_info=True, extra=log_extra_session)
    except Exception as e_global:
        logger.critical(f"💥 Error GLOBAL NO CAPTURADO (Async): {e_global}", exc_info=True, extra={'symbol': 'GLOBAL_CRASH_ASYNC', 'transaction_id': 'CRASH'})
    
    finally:
        if stop_event:
            stop_event.set()
        
        logger.info("Deteniendo Microstructure Manager...", extra=log_extra_session)
        await microstructure_manager.stop_all()
        
        # --- INICIO MODIFICACIÓN OCO: Cerrar AsyncClient ---
        if aclient:
            await aclient.close_connection()
            logger.info("AsyncClient cerrado.", extra=log_extra_session)
        # --- FIN MODIFICACIÓN ---
            
        logger.info(f"🏁 Bot (Sesión Async: {bot_session_id}) finalizando... Encolando guardado de estado final.", extra=log_extra_session)
        pm.save_app_state(transaction_id=f"{bot_session_id}_FinalSave")


if __name__ == "__main__":
    utils.setup_logging()
    io_worker.start()

    try:
        execution_mode = config.get_param("GLOBAL_SETTINGS.EXECUTION_MODE", "POLLING")

        if execution_mode.upper() == "POLLING":
            logger.info("Iniciando en modo POLLING (Síncrono).")
            main_bot_loop()
        elif execution_mode.upper() == "EVENT":
            logger.info("Iniciando en modo EVENT (Asíncrono).")
            asyncio.run(main_event_driven_loop())
        else:
            logger.critical(f"EXECUTION_MODE '{execution_mode}' inválido. Use 'POLLING' o 'EVENT'. Saliendo.")

    except Exception as e_main:
        logger.critical(f"Error fatal en el arranque principal: {e_main}", exc_info=True, extra={'symbol': 'MAIN_INIT_FAIL'})

    finally:
        logger.info("Deteniendo I/O Worker...")
        io_worker.stop()
        logger.info("Proceso del bot finalizado.")