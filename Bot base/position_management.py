# position_management.py
from unified_logger import get_logger
import json
import os
import datetime
import pytz
from decimal import Decimal, InvalidOperation
from filelock import FileLock, Timeout
import copy
import queue
import asyncio
import time  # <--- AÑADIDO: Requerido por el parche de protection_manager
from typing import Optional, Dict, Any  # <--- AÑADIDO: Requerido por el parche
import binance_interaction as bi
import config
# import utils # (Fase 1.1) Eliminado para romper ciclo
# import excel_logger # (Fase 1.1) Eliminado para romper ciclo
import path_manager
from io_worker import io_worker, TASK_SAVE_JSON
# --- INICIO DE MODIFICACIÓN: (Circular Import Fix) ---
# import protection_manager  # <--- AÑADIDO: Requerido por el parche (MOVIDO A LOCAL)
# --- FIN DE MODIFICACIÓN ---

logger = get_logger(__name__)


class AppState:
    def __init__(self, path_key: str):
        self.path_key = path_key  # e.g., "active_positions"
        self.state = self._initialize_default_state()

    def _initialize_default_state(self):
        return {
            "active_positions": {},
            "risk_management": {},
            "bot_state": {
                "last_backup_date": (datetime.date.today() - datetime.timedelta(days=1)).isoformat(),
                "circuit_breaker": {
                    "global_consecutive_losses": 0,
                    "strategy_losses": {}
                },
                # --- INICIO DE MODIFICACIÓN: (v2.9.1) ---
                "data_quality_failures": {}
                # --- FIN DE MODIFICACIÓN ---
            },
            "cooldown_until": {}
        }

    def load(self, transaction_id="LOAD_STATE"):
        """
        Carga el estado sincrónicamente.
        Esto DEBE ser bloqueante para asegurar que el bot inicie
        con el estado correcto antes de operar.
        """
        log_extra = {'transaction_id': transaction_id, 'symbol': 'APP_STATE'}
        try:
            filepath = path_manager.get_path(self.path_key)
        except (KeyError, IOError) as e:
            logger.critical(f"Error fatal al obtener la ruta de estado '{self.path_key}': {e}. Usando estado por defecto.", extra=log_extra)
            self.state = self._initialize_default_state()
            self.save(f"{transaction_id}_InitSave") 
            return
        lock_path = filepath + ".lock"
        if not os.path.exists(filepath):
            self.state = self._initialize_default_state()
            self.save(f"{transaction_id}_InitSave")  # Encolar guardado
            return
        try:
            with FileLock(lock_path, timeout=5):
                with open(filepath, 'r', encoding='utf-8') as f:
                    loaded_data = json.load(f)
            self.state['active_positions'] = loaded_data.get('active_positions', {})
            self.state['bot_state'] = loaded_data.get('bot_state', self._initialize_default_state()['bot_state'])
            # --- Asegurar claves (v2.9.1) ---
            if 'circuit_breaker' not in self.state['bot_state']:
                self.state['bot_state']['circuit_breaker'] = self._initialize_default_state()['bot_state']['circuit_breaker']
            if 'data_quality_failures' not in self.state['bot_state']:
                self.state['bot_state']['data_quality_failures'] = self._initialize_default_state()['bot_state']['data_quality_failures']
            # --- Fin Asegurar claves ---
            self.state['cooldown_until'] = loaded_data.get('cooldown_until', {})
            logger.info(f"Estado cargado desde {filepath}. Se encontraron {len(self.state['active_positions'])} posiciones.", extra=log_extra)
        except Timeout:
            logger.error(f"Timeout al intentar bloquear el archivo de estado '{filepath}' durante la carga.", extra=log_extra)
            self.state = self._initialize_default_state()
        except json.JSONDecodeError as e:
            logger.critical(f"Archivo de estado '{filepath}' corrupto (JSON inválido): {e}. Usando estado por defecto.", extra=log_extra)
            self.state = self._initialize_default_state()
        except Exception as e:
            logger.error(f"Error inesperado al cargar estado: {e}. Usando estado por defecto.", exc_info=True, extra=log_extra)
            self.state = self._initialize_default_state()

    def save(self, transaction_id="SAVE_STATE"):
        """
        Encola el guardado del estado en el IOWorker (No bloqueante).
        """
        log_extra = {'transaction_id': transaction_id, 'symbol': 'APP_STATE'}
        try:
            state_copy = copy.deepcopy(self.state)
            payload = {
                "path_key": self.path_key,
                "data": state_copy
            }
            io_worker.add_task(TASK_SAVE_JSON, payload, transaction_id)
            logger.debug(f"Estado encolado para guardado (ID: {transaction_id})", extra=log_extra)
        except queue.Full:
            logger.error(f"Fallo CRÍTICO al encolar guardado de estado (ID: {transaction_id}): Cola de E/S llena.", extra=log_extra)
        except Exception as e:
            logger.error(f"Error inesperado al encolar guardado de estado: {e}", exc_info=True, extra=log_extra)

    def get_all_active_positions(self):
        return self.state.get('active_positions', {})

    def get_active_position(self, symbol):
        return self.state.get('active_positions', {}).get(symbol)

    def open_new_position(self, symbol, position_data):
        self.state['active_positions'][symbol] = position_data

    def close_position(self, symbol):
        if symbol in self.state.get('active_positions', {}):
            del self.state['active_positions'][symbol]

    def get_bot_state_value(self, key):
        return self.state.get('bot_state', {}).get(key)

    def set_bot_state_value(self, key, value):
        self.state.get('bot_state', {})[key] = value

    def get_circuit_breaker_state(self):
        if 'circuit_breaker' not in self.state.get('bot_state', {}):
            self.state.setdefault('bot_state', {})['circuit_breaker'] = self._initialize_default_state()['bot_state']['circuit_breaker']
        return self.state['bot_state']['circuit_breaker']

    def increment_losses(self, symbol, strategy_name):
        cb_state = self.get_circuit_breaker_state()
        cb_state['global_consecutive_losses'] = cb_state.get('global_consecutive_losses', 0) + 1
        key = f"{symbol}_{strategy_name}"
        if 'strategy_losses' not in cb_state:
            cb_state['strategy_losses'] = {}
        cb_state['strategy_losses'][key] = cb_state['strategy_losses'].get(key, 0) + 1
        cb_config = config.get_param("CIRCUIT_BREAKER_CONFIG", return_dict_for_complex=True)
        if cb_config and cb_config.get("ENABLED", False):
            max_global_losses = int(cb_config.get("GLOBAL_MAX_CONSECUTIVE_LOSSES", 5))
            current_global_losses = cb_state.get('global_consecutive_losses', 0)
            if current_global_losses >= max_global_losses:
                duration = int(cb_config.get("GLOBAL_COOLDOWN_MINUTES", 120))
                is_active, _ = self.get_cooldown_status("GLOBAL_CIRCUIT_BREAKER")
                if not is_active:
                    self.set_cooldown("GLOBAL_CIRCUIT_BREAKER", duration)
                    logger.critical(f"🚨 CIRCUIT BREAKER GLOBAL ACTIVADO: {current_global_losses} pérdidas. Cooldown de {duration} min iniciado.", extra={'symbol': 'GLOBAL_CB'})

    def reset_losses(self, symbol, strategy_name):
        cb_state = self.get_circuit_breaker_state()
        cb_state['global_consecutive_losses'] = 0
        key = f"{symbol}_{strategy_name}"
        if 'strategy_losses' in cb_state and key in cb_state['strategy_losses']:
            cb_state['strategy_losses'][key] = 0
        self.clear_cooldown("GLOBAL_CIRCUIT_BREAKER")

    def get_cooldown_status(self, key):
        cooldown_end_time_str = self.state.get('cooldown_until', {}).get(key)
        if not cooldown_end_time_str:
            return False, None
        try:
            cooldown_end_time = datetime.datetime.fromisoformat(cooldown_end_time_str)
            if cooldown_end_time.tzinfo is None:
                cooldown_end_time = pytz.utc.localize(cooldown_end_time)
            now_utc = datetime.datetime.now(pytz.utc)
            if now_utc < cooldown_end_time:
                return True, cooldown_end_time
            if key in self.state.get('cooldown_until', {}):
                del self.state['cooldown_until'][key]
            return False, None
        except ValueError:
            logger.error(f"Timestamp de cooldown inválido para '{key}': {cooldown_end_time_str}. Eliminando entrada.", extra={'symbol': key.split('_')[0] if '_' in key else 'GLOBAL'})
            if key in self.state.get('cooldown_until', {}):
                del self.state['cooldown_until'][key]
            return False, None

    def set_cooldown(self, key, duration_minutes):
        cooldown_end = datetime.datetime.now(pytz.utc) + datetime.timedelta(minutes=duration_minutes)
        self.state.setdefault('cooldown_until', {})[key] = cooldown_end.isoformat()
        logger.warning(f"❄️ COOLDOWN ACTIVADO para '{key}' por {duration_minutes} minutos.", extra={'symbol': key.split('_')[0] if '_' in key else 'GLOBAL'})

    def clear_cooldown(self, key):
        log_extra = {'transaction_id': 'RESET_COOLDOWN', 'symbol': key}
        if key in self.state.get('cooldown_until', {}):
            try:
                del self.state['cooldown_until'][key]
                logger.info(f"Cooldown key '{key}' ha sido reseteada (probablemente por un TP).", extra=log_extra)
            except Exception as e:
                logger.warning(f"No se pudo eliminar la clave de cooldown '{key}': {e}", extra=log_extra)

    # --- INICIO DE MODIFICACIÓN: (v2.9.1) ---
    def get_data_quality_failures_state(self):
        """Asegura que la clave 'data_quality_failures' exista."""
        if 'data_quality_failures' not in self.state.get('bot_state', {}):
            self.state.setdefault('bot_state', {})['data_quality_failures'] = self._initialize_default_state()['bot_state']['data_quality_failures']
        return self.state['bot_state']['data_quality_failures']

    def increment_data_quality_failure(self, symbol: str):
        """Incrementa el contador de fallos de calidad para un símbolo."""
        log_extra = {'symbol': symbol, 'transaction_id': 'DATA_QA_FAIL'}
        dq_state = self.get_data_quality_failures_state()
        dq_state[symbol] = dq_state.get(symbol, 0) + 1
        current_failures = dq_state[symbol]
        try:
            threshold = int(config.get_param("GLOBAL_SETTINGS.DATA_QUALITY_FAILURE_THRESHOLD", 5))
        except Exception:
            threshold = 5
        logger.warning(f"Fallo de calidad de datos para {symbol}. Contador: {current_failures}/{threshold}", extra=log_extra)
        if current_failures >= threshold:
            logger.critical(f"Umbral de fallos de calidad de datos ({threshold}) alcanzado para {symbol}. Intentando auto-blacklist.", extra=log_extra)
            try:
                self.auto_blacklist_symbol(symbol)
                dq_state[symbol] = 0  # Resetear contador tras blacklisting
            except Exception as e_bl:
                logger.error(f"Fallo al intentar auto-blacklist de {symbol}: {e_bl}", exc_info=True, extra=log_extra)
        # Guardar el estado (no bloqueante) para persistir el incremento
        self.save(transaction_id="SaveDataQualityFailure")

    def auto_blacklist_symbol(self, symbol: str):
        """
        Añade un símbolo al BLACKLIST_SYMBOLS en bot_settings.json.
        Esta operación es BLOQUEANTE (usa FileLock) para evitar
        race conditions con Optuna.
        """
        log_extra = {'symbol': symbol, 'transaction_id': 'AUTO_BLACKLIST'}
        try:
            config_filepath = path_manager.get_path("bot_settings")
            lock_path = config_filepath + ".lock"
            logger.info(f"Adquiriendo FileLock para {lock_path}...", extra=log_extra)
            with FileLock(lock_path, timeout=10):
                logger.info("FileLock adquirido. Ejecutando R-M-W.", extra=log_extra)
                # 1. Leer (Read)
                with open(config_filepath, 'r', encoding='utf-8') as f:
                    config_data = json.load(f)
                # 2. Modificar (Modify)
                if "MARKET_SCANNER_CONFIG" not in config_data:
                    config_data["MARKET_SCANNER_CONFIG"] = {}
                if "BLACKLIST_SYMBOLS" not in config_data["MARKET_SCANNER_CONFIG"]:
                    config_data["MARKET_SCANNER_CONFIG"]["BLACKLIST_SYMBOLS"] = []
                if symbol not in config_data["MARKET_SCANNER_CONFIG"]["BLACKLIST_SYMBOLS"]:
                    config_data["MARKET_SCANNER_CONFIG"]["BLACKLIST_SYMBOLS"].append(symbol)
                    logger.info(f"Símbolo {symbol} añadido a BLACKLIST_SYMBOLS.", extra=log_extra)
                else:
                    logger.warning(f"Símbolo {symbol} ya estaba en BLACKLIST_SYMBOLS.", extra=log_extra)
                # 3. Escribir (Write)
                temp_filepath = config_filepath + ".tmp"
                with open(temp_filepath, 'w', encoding='utf-8') as f:
                    json.dump(config_data, f, indent=4, default=str)
                os.replace(temp_filepath, config_filepath)
            logger.info(f"Auto-blacklist completado. {symbol} añadido a {config_filepath}.", extra=log_extra)
        except Timeout:
            logger.error(f"Timeout al intentar bloquear {lock_path} para auto-blacklist. {symbol} no fue añadido.", extra=log_extra)
        except Exception as e:
            logger.error(f"Error crítico durante auto-blacklist de {symbol}: {e}", exc_info=True, extra=log_extra)
            raise
    # --- FIN DE MODIFICACIÓN: (v2.9.1) ---


app_state_manager = AppState(path_key="active_positions")


# --- INICIO DE MODIFICACIÓN: Getter para Protection Manager ---
def get_app_state_manager() -> AppState:
    """Retorna la instancia global del AppState manager."""
    return app_state_manager
# --- FIN DE MODIFICACIÓN ---


def load_app_state(transaction_id="LOAD_STATE_WRAPPER"):
    app_state_manager.load(transaction_id)


def save_app_state(transaction_id="SAVE_STATE_WRAPPER"):
    app_state_manager.save(transaction_id)


def get_all_active_positions():
    return app_state_manager.get_all_active_positions()


def get_active_position(symbol):
    return app_state_manager.get_active_position(symbol)


def get_bot_state_value(key):
    return app_state_manager.get_bot_state_value(key)


def set_bot_state_value(key, value):
    app_state_manager.set_bot_state_value(key, value)


def clear_cooldown_key(key):
    return app_state_manager.clear_cooldown(key)


# --- INICIO DE MODIFICACIÓN: (v2.9.1) ---
def increment_data_quality_failure(symbol: str): 
    """Wrapper para la función de incremento de fallos."""
    app_state_manager.increment_data_quality_failure(symbol)
# --- FIN DE MODIFICACIÓN ---


def is_strategy_on_cooldown(symbol, strategy_name):
    global_key = f"{symbol}_GLOBAL"
    is_on_global_cooldown, _ = app_state_manager.get_cooldown_status(global_key)
    if is_on_global_cooldown:
        logger.debug(f"Símbolo {symbol} en cooldown global.", extra={'symbol': symbol})
        return True
    strategy_key = f"{symbol}_{strategy_name}"
    is_on_strategy_cooldown, _ = app_state_manager.get_cooldown_status(strategy_key)
    if is_on_strategy_cooldown:
        logger.debug(f"Estrategia {strategy_name} para {symbol} en cooldown.", extra={'symbol': symbol, 'strategy': strategy_name})
        return True
    return False


def is_global_circuit_breaker_tripped():
    cb_config = config.get_param("CIRCUIT_BREAKER_CONFIG", return_dict_for_complex=True)
    if not cb_config or not cb_config.get("ENABLED", False):
        return False
    is_on_cooldown, end_time = app_state_manager.get_cooldown_status("GLOBAL_CIRCUIT_BREAKER")
    cb_state = app_state_manager.get_circuit_breaker_state()
    current_losses = cb_state.get("global_consecutive_losses", 0)
    if is_on_cooldown:
        logger.warning(f"🚨 CIRCUIT BREAKER GLOBAL ACTIVO ({current_losses} pérdidas)", extra={'symbol': 'GLOBAL_CB'})
        return True
    if current_losses > 0 and not is_on_cooldown:
        logger.info(f"🔄 Reactivando bot - Reset de {current_losses} pérdidas globales", extra={'symbol': 'GLOBAL_CB_RESET'})
        cb_state['global_consecutive_losses'] = 0
        app_state_manager.save(transaction_id="GCB_AutoReset")
    return False


def open_new_position(symbol, order_result, sl_price, tp_price, signal_info, sl_atr_mult, transaction_id):
    """
    Función SÍNCRONA para el modo POLLING.
    (La nueva lógica OCO se aplica en 'async_open_new_position')
    """
    log_extra = {'transaction_id': transaction_id, 'symbol': symbol}
    try:
        net_quantity_received = order_result.get('netQuantityReceived', order_result['executedQty'])
        open_ts_utc = datetime.datetime.fromtimestamp(order_result['transactTime'] / 1000, tz=pytz.utc)
        position = {
            "TradeID": order_result['orderId'],
            "TimestampAperturaUTC": open_ts_utc.isoformat(),
            "Symbol": symbol,
            "PrecioEntradaPromedio": order_result['avgPrice'],
            "CantidadNetaEntrada": net_quantity_received,
            "MontoCompraBrutoUSDT": order_result.get('quoteValue', '0'),
            "MontoComisionCompraUSDT": order_result.get('total_commission_usdt_equivalent', '0'),
            "StopLoss": str(sl_price),
            "TakeProfit": str(tp_price),
            "StrategyName_En_Compra": signal_info.get("strategy_name", "Desconocida"),
            "SL_ATR_Multiplier": str(sl_atr_mult),
            "market_type": "SPOT",  # Asumir SPOT para modo POLLING
            "entry_side": signal_info.get('type', 'LONG')
        }
        app_state_manager.open_new_position(symbol, position)
        save_app_state(f"{transaction_id}_SaveOpen")
        logger.info(f"✅📈 NUEVA POSICIÓN ABIERTA para {symbol}: ID {position.get('TradeID')}", extra=log_extra)
    except Exception as e:
        logger.error(f"Error crítico al intentar guardar la nueva posición para {symbol}: {e}", exc_info=True, extra=log_extra)


def close_position_logic(symbol, position_data, reason, exit_price, transaction_id):
    import utils  # Importación local mantenida para evitar ciclos
    import excel_logger  # Importación local mantenida
    # --- INICIO DE MODIFICACIÓN: (Circular Import Fix) ---
    import protection_manager
    # --- FIN DE MODIFICACIÓN ---
    log_extra = {'transaction_id': transaction_id, 'symbol': symbol}
    try:
        quantity_to_sell_str = position_data.get('CantidadNetaEntrada', '0')
        quantity_to_sell = Decimal(quantity_to_sell_str)
        min_qty, _ = bi.get_min_notional_and_qty(symbol, transaction_id=f"{transaction_id}_GetMinQty")
        if min_qty is None:
            logger.critical(f"No se pudieron obtener los filtros para {symbol}. Se aborta el cierre para evitar desincronización.", extra=log_extra)
            return
        if quantity_to_sell < min_qty:
            logger.warning(f"La cantidad registrada ({quantity_to_sell_str}) es 'polvo'. Eliminando posición sin vender.", extra=log_extra)
            app_state_manager.close_position(symbol)
            save_app_state(f"{transaction_id}_SaveDustClose")
            return
        base_asset, _ = utils.split_symbol_pair(symbol)
        real_balance = bi.get_account_balance_for_asset(base_asset, transaction_id=f"{transaction_id}_PreSellBal")
        if quantity_to_sell > real_balance:
            logger.warning(f"DESINCRONIZACIÓN para {symbol}: Se intenta vender {quantity_to_sell} pero solo hay {real_balance}. Ajustando cantidad a vender.", extra=log_extra)
            quantity_to_sell = real_balance
            if quantity_to_sell < min_qty:
                logger.error(f"Balance real ajustado ({real_balance}) es 'polvo'. Eliminando posición sin vender.", extra=log_extra)
                app_state_manager.close_position(symbol)
                save_app_state(f"{transaction_id}_SaveDustCloseAdjusted")
                return
        order_result = None
        logger.info(f"Intentando cierre con orden a MERCADO para {symbol} (Qty: {quantity_to_sell}) por razón: {reason}", extra=log_extra)
        # --- MODIFICACIÓN: Asumir que el cierre es 'SELL' (el bot solo es LONG) ---
        order_result = bi.place_market_order(symbol, "SELL", quantity_to_sell, transaction_id)
        if order_result and order_result.get('status') == 'FILLED':
            # --- MODIFICACIÓN: Cancelar protección en exchange ANTES de loggear ---
            protection_meta = position_data.get('protection_meta')
            if protection_meta and protection_meta.get('status') == 'ACTIVE':
                try:
                    aclient = bi.get_async_client()
                    if aclient:
                        # No podemos esperar (await) en una función síncrona,
                        # creamos una tarea para que se ejecute en el loop de asyncio
                        # (Si el loop no está corriendo, esto podría fallar, pero es la única forma en modo POLLING)
                        asyncio.create_task(
                            protection_manager.cancel_protection_orders(
                                aclient, protection_meta, f"{transaction_id}_CancelProt"
                            )
                        )
                        logger.info(f"Solicitud de cancelación de protección para {symbol} enviada.", extra=log_extra)
                    else:
                        logger.error(f"No se pudo obtener AsyncClient para cancelar protección de {symbol}.", extra=log_extra)
                except Exception as e_cancel:
                    logger.error(f"Error al solicitar cancelación de protección para {symbol}: {e_cancel}", extra=log_extra)
            # --- FIN MODIFICACIÓN ---
            open_ts = None
            open_timestamp_str = position_data.get('TimestampAperturaUTC')
            try:
                if open_timestamp_str:
                    open_ts = datetime.datetime.fromisoformat(open_timestamp_str)
                if open_ts.tzinfo is None:
                    open_ts = pytz.utc.localize(open_ts)
                else:
                    logger.critical(f"TimestampAperturaUTC faltante en position_data para {symbol}. Datos corruptos.", extra=log_extra)
            except (ValueError, TypeError) as e:
                logger.critical(f"Error al parsear TimestampAperturaUTC ('{open_timestamp_str}') para {symbol}. Datos corruptos: {e}", extra=log_extra)
            if open_ts is None:
                logger.critical(f"IMPOSIBLE REGISTRAR TRADE {symbol}: Timestamp de apertura inválido o faltante. La posición NO se eliminará del estado para revisión manual.", extra=log_extra)
                return
            close_ts = None
            try:
                transact_time = order_result.get('transactTime')
                if transact_time:
                    close_ts = datetime.datetime.fromtimestamp(transact_time / 1000, tz=pytz.utc)
                else:
                    logger.error(f"transactTime faltante en order_result para cierre de {symbol}. Usando hora actual UTC.", extra=log_extra)
                    close_ts = datetime.datetime.now(pytz.utc)
            except Exception as e_ts_close:
                logger.error(f"Error parseando transactTime para cierre de {symbol}: {e_ts_close}. Usando hora actual UTC.", extra=log_extra)
                close_ts = datetime.datetime.now(pytz.utc)
            monto_compra_bruto = Decimal(position_data.get('MontoCompraBrutoUSDT', '0'))
            monto_comision_compra = Decimal(position_data.get('MontoComisionCompraUSDT', '0'))
            costo_total_compra = monto_compra_bruto + monto_comision_compra
            monto_venta_bruto = Decimal(order_result.get('quoteValue', '0'))
            monto_comision_venta = Decimal(order_result.get('total_commission_usdt_equivalent', '0'))
            ingreso_neto_venta = monto_venta_bruto - monto_comision_venta
            net_profit = ingreso_neto_venta - costo_total_compra
            entry_price = Decimal(position_data['PrecioEntradaPromedio'])
            position_size_sold = Decimal(order_result.get('executedQty', '0'))
            exit_price_actual = Decimal(order_result.get('avgPrice', '0'))
            gross_profit = monto_venta_bruto - monto_compra_bruto
            total_commission_paid = monto_comision_compra + monto_comision_venta
            duration_sec = int((close_ts - open_ts).total_seconds()) if close_ts and open_ts else -1
            _, quote_asset = utils.split_symbol_pair(symbol)
            closed_trade_data = {
                "trade_id": position_data.get('TradeID', 'N/A'),
                "bot_name": config.get_param("GLOBAL_SETTINGS.BOT_NAME"),
                "symbol": symbol,
                "open_timestamp": open_ts,
                "close_timestamp": close_ts,
                "trade_duration_sec": duration_sec,
                "trade_type": "LONG",
                "entry_price": float(entry_price),
                "exit_price": float(exit_price_actual),
                "position_size_asset": float(position_size_sold),
                "position_size_quote": float(monto_compra_bruto),
                "stop_loss_price": float(position_data.get('StopLoss', 0)),
                "take_profit_price": float(position_data.get('TakeProfit', 0)),
                "commission": float(total_commission_paid),
                "swap": 0.0,
                "gross_profit_quote": float(gross_profit),
                "net_profit_quote": float(net_profit),
                "close_reason": reason,
                "comment": position_data.get("StrategyName_En_Compra", "Desconocida")
            }
            try:
                excel_logger.log_trade(closed_trade_data)
                logger.info(f"✔️💸 POSICIÓN CERRADA para {symbol}. Razón: {reason}. PnL Neto: {net_profit:+.4f} {quote_asset}", extra=log_extra)
                strategy_name = position_data.get("StrategyName_En_Compra", "Desconocida")
                if reason in ["STOP_LOSS_HIT", "LATERAL_CLOSE", "TIMEOUT_CLOSE", "EARLY_TIMEOUT_LOSS"]:
                    app_state_manager.increment_losses(symbol, strategy_name)
                    cb_config = config.get_param("CIRCUIT_BREAKER_CONFIG", return_dict_for_complex=True)
                    if cb_config and cb_config.get("ENABLED", False):
                        key_strategy = f"{symbol}_{strategy_name}"
                        max_losses_strategy = int(cb_config.get("MAX_CONSECUTIVE_LOSSES_PER_STRATEGY", 2))
                        current_losses_strategy = app_state_manager.get_circuit_breaker_state().get("strategy_losses", {}).get(key_strategy, 0)
                        if current_losses_strategy >= max_losses_strategy:
                            duration_strategy = int(cb_config.get("STRATEGY_COOLDOWN_MINUTES", 60))
                            app_state_manager.set_cooldown(key_strategy, duration_strategy)
                        app_state_manager.set_cooldown(f"{symbol}_GLOBAL", duration_strategy)
                else:
                    app_state_manager.reset_losses(symbol, strategy_name)
                app_state_manager.close_position(symbol)
                save_app_state(f"{transaction_id}_SaveClose")
            except Exception as e_log_save:
                logger.critical(f"Error al ENCOLAR trade {symbol} en Excel O al actualizar estado: {e_log_save}. La posición puede seguir en active_positions.json.", exc_info=True, extra=log_extra)
        elif order_result and order_result.get('status') == 'NEW':
            logger.critical(f"ORDEN LÍMITE ABIERTA PARA {symbol} PERO NO HAY LÓGICA DE SEGUIMIENTO. ¡DESINCRONIZACIÓN!", extra=log_extra)
        else:
            logger.error(f"La orden de cierre para {symbol} no se completó (status: {order_result.get('status', 'DESCONOCIDO')}). No se registrará ni se cerrará la posición en el estado.", extra=log_extra)
    except Exception as e:
        logger.critical(f"Error CRÍTICO INESPERADO en close_position_logic para {symbol}: {e}. La posición puede seguir activa o desincronizada.", exc_info=True, extra=log_extra)


def check_and_manage_existing_position(symbol, df, transaction_id):
    position = get_active_position(symbol)
    if not position:
        return
    log_extra = {'transaction_id': transaction_id, 'symbol': symbol}
    # --- INICIO MODIFICACIÓN: Lógica de SL/TP virtual desactivada si hay protección en exchange ---
    protection_meta = position.get('protection_meta')
    if protection_meta and protection_meta.get('status') == 'ACTIVE':
        logger.debug(f"Gestión de {symbol} delegada al Exchange (OCO/Protección activa).", extra=log_extra)
        # NOTA: Aún podríamos querer gestionar TIMEOUTS aquí.
        # Por ahora, se salta el chequeo de SL/TP virtual.
        # (La lógica de TIMEOUT/LATERALIDAD más abajo SÍ se ejecutará)
    elif protection_meta and protection_meta.get('status') == 'FAILED':
        logger.warning(f"Gestionando {symbol} con SL virtual (Fallo de protección en Exchange). Watcher de emergencia debería estar activo.", extra=log_extra)
    else:
        # Sin protección, gestionar SL/TP virtual
        logger.debug(f"Gestionando {symbol} con SL/TP virtual (Sin protección en Exchange).", extra=log_extra)
    # --- FIN MODIFICACIÓN ---
    try:
        sl_price = Decimal(position['StopLoss'])
        tp_price = Decimal(position['TakeProfit'])
        entry_price = Decimal(position['PrecioEntradaPromedio'])
        net_entry_qty_str = position.get('CantidadNetaEntrada')
        if net_entry_qty_str is None:
            raise ValueError("CantidadNetaEntrada es None")
        net_entry_qty = Decimal(net_entry_qty_str)
        if net_entry_qty <= 0:
            raise ValueError(f"CantidadNetaEntrada inválida: {net_entry_qty}")
    except (InvalidOperation, TypeError, KeyError, ValueError) as e:
        logger.critical(f"DATOS DE POSICIÓN CORRUPTOS o inválidos para {symbol}: {e}. No se puede gestionar. Posición: {position}", extra=log_extra)
        return
    if df is None or df.empty or 'close' not in df.columns or 'low' not in df.columns or 'high' not in df.columns:
        logger.warning(f"DataFrame inválido para gestionar posición {symbol}. Saltando gestión.", extra=log_extra)
        return
    try:
        current_price = df['close'].iloc[-1]
        current_low = df['low'].iloc[-1]
        current_high = df['high'].iloc[-1]
        if any(v is None for v in [current_price, current_low, current_high]):
            raise ValueError("Precio actual (close/low/high) es None")
    except (IndexError, ValueError) as e:
        logger.warning(f"No se pudieron obtener precios actuales válidos para gestionar {symbol}: {e}. Saltando gestión.", extra=log_extra)
        return
    try:
        pnl_quote = (current_price - entry_price) * net_entry_qty
        logger.info(f"🔎 Gestionando posición BUY en {symbol}. Precio actual: {current_price:.4f} | SL: {sl_price}, TP: {tp_price} | PnL actual: {pnl_quote:+.2f} USDT", extra=log_extra)
    except Exception:
        logger.warning(f"No se pudo calcular PnL actual para logging en {symbol}.", extra=log_extra)
    # --- INICIO MODIFICACIÓN: SL/TP Virtual solo si no hay protección ---
    if not (protection_meta and protection_meta.get('status') == 'ACTIVE'):
        if current_low <= sl_price:
            logger.warning(f"🚨 SL VIRTUAL ALCANZADO para {symbol}. Vela LOW: {current_low} <= SL: {sl_price}", extra=log_extra)
            close_position_logic(symbol, position, "STOP_LOSS_HIT", sl_price, transaction_id=f"{transaction_id}_CloseSL")
            return
        elif current_high >= tp_price:
            logger.info(f"📤 TP VIRTUAL ALCANZADO para {symbol}. Vela HIGH: {current_high} >= TP: {tp_price}", extra=log_extra)
            close_position_logic(symbol, position, "TAKE_PROFIT_HIT", tp_price, transaction_id=f"{transaction_id}_CloseTP")
            return
    # --- FIN MODIFICACIÓN ---
    exit_config = config.get_param("POSITION_EXIT_CONFIG", return_dict_for_complex=True)
    if not exit_config:
        return
    open_timestamp_str = position.get("TimestampAperturaUTC")
    open_time = None
    if open_timestamp_str:
        try:
            open_time = datetime.datetime.fromisoformat(open_timestamp_str)
            if open_time.tzinfo is None:
                open_time = pytz.utc.localize(open_time)
        except (ValueError, TypeError):
            logger.error(f"TimestampAperturaUTC inválido ('{open_timestamp_str}') al verificar tiempo/lateralidad para {symbol}", extra=log_extra)
            open_time = None
    if open_time:
        now_utc = datetime.datetime.now(pytz.utc)
        duration_seconds = (now_utc - open_time).total_seconds()
        if exit_config.get("ENABLE_EARLY_EXIT_ON_LOSS", False):
            max_duration_minutes_early = int(exit_config.get("MAX_TRADE_DURATION_MINUTES", 15))
            early_exit_pct = Decimal(str(exit_config.get("EARLY_EXIT_TIME_PERCENTAGE", "0.75")))
            time_threshold_seconds = max_duration_minutes_early * 60 * early_exit_pct
            if duration_seconds > time_threshold_seconds:
                if current_price < entry_price:
                    logger.warning(f"⏳ SALIDA ANTICIPADA para {symbol}. En pérdida tras superar el {early_exit_pct * 100}% del tiempo máximo.", extra=log_extra)
                    close_position_logic(symbol, position, "EARLY_TIMEOUT_LOSS", current_price, transaction_id=f"{transaction_id}_CloseEarly")
                    return
        max_duration_minutes = int(exit_config.get("MAX_TRADE_DURATION_MINUTES", 15))
        if duration_seconds > max_duration_minutes * 60:
            logger.warning(f"🕰️ TIMEOUT ALCANZADO para {symbol}. Duración: {datetime.timedelta(seconds=int(duration_seconds))}", extra=log_extra)
            close_position_logic(symbol, position, "TIMEOUT_CLOSE", current_price, transaction_id=f"{transaction_id}_CloseTimeout")
            return
        candle_count = int(exit_config.get("LATERAL_CANDLE_COUNT", 10))
        if len(df) >= candle_count:
            try:
                lateral_range_pct = Decimal(str(exit_config.get("LATERAL_RANGE_PERCENTAGE", "0.002")))
                recent_candles = df.tail(candle_count)
                max_price = recent_candles['high'].max()
                min_price = recent_candles['low'].min()
                if entry_price > 0 and max_price is not None and min_price is not None and isinstance(max_price, Decimal) and isinstance(min_price, Decimal):
                    price_range = max_price - min_price
                    if (price_range / entry_price) < lateral_range_pct:
                        logger.warning(f"📉 LATERALIDAD DETECTADA para {symbol}. Rango ({price_range:.4f}) < {lateral_range_pct * 100:.2f}% del precio entrada.", extra=log_extra)
                        close_position_logic(symbol, position, "LATERAL_CLOSE", current_price, transaction_id=f"{transaction_id}_CloseLateral")
                        return
                elif not (entry_price > 0):
                    logger.debug(f"Precio de entrada no válido ({entry_price}) para check de lateralidad en {symbol}.", extra=log_extra)
            except Exception as e_lat:
                logger.warning(f"Error calculando lateralidad para {symbol}: {e_lat}", extra=log_extra)


# --- Wrappers Asíncronos (Fase 2.2) ---
# --- INICIO DE MODIFICACIÓN: Lógica de OCO aplicada a async_open_new_position ---
async def async_open_new_position(
    symbol: str, 
    order_result: Dict[str, Any], 
    sl_price: Decimal, 
    tp_price: Decimal, 
    signal_info: Dict[str, Any], 
    sl_atr_mult: Decimal, 
    transaction_id: str,
    client: Optional[Any] = None,  # Cliente Asíncrono
    market_type: str = "SPOT"
):
    """
    Wrapper Asíncrono para abrir posición (MODO EVENT).
    Esta función ahora contiene la lógica de OCO/Protección.
    """
    # --- INICIO DE MODIFICACIÓN: (Circular Import Fix) ---
    import protection_manager
    # --- FIN DE MODIFICACIÓN ---
    log_extra = {'transaction_id': transaction_id, 'symbol': symbol}
    app_state = get_app_state_manager()
    if client is None:
        client = bi.get_async_client()
        if client is None:
            logger.error("AsyncClient no disponible para async_open_new_position", extra=log_extra)
            return
    try:
        # 1. Obtener cantidad ejecutada
        executed_qty_str = order_result.get('executedQty') or order_result.get('qty')  # Futures 'qty'
        executed_qty = Decimal(str(executed_qty_str)) if executed_qty_str else Decimal('0')
        if executed_qty <= 0:
            fills = order_result.get('fills', [])
            if fills and isinstance(fills, list):
                executed_qty = sum(Decimal(str(f.get('qty', '0'))) for f in fills)
        if executed_qty <= 0:
            logger.error("async_open_new_position llamada con executedQty 0", extra=log_extra)
            return
        # 2. Obtener avgPrice
        avg_price_str = order_result.get('avgPrice')
        if not avg_price_str and order_result.get('cummulativeQuoteQty') and executed_qty > 0:
            avg_price_str = str(Decimal(order_result['cummulativeQuoteQty']) / executed_qty)
        elif not avg_price_str and order_result.get('fills'):
            avg_price_str = order_result['fills'][0].get('price')
        avg_price = Decimal(str(avg_price_str))
        # 3. Persistir estado base
        open_ts_utc = datetime.datetime.fromtimestamp(order_result.get('transactTime', int(time.time() * 1000)) / 1000, tz=pytz.utc)
        position = {
            "TradeID": order_result.get('orderId') or order_result.get('clientOrderId'),
            "TimestampAperturaUTC": open_ts_utc.isoformat(),
            "Symbol": symbol,
            "PrecioEntradaPromedio": str(avg_price),
            "CantidadNetaEntrada": str(executed_qty),
            "MontoCompraBrutoUSDT": order_result.get('cummulativeQuoteQty') or '0',
            "MontoComisionCompraUSDT": order_result.get('total_commission_usdt_equivalent', '0'),
            "StopLoss": str(sl_price),
            "TakeProfit": str(tp_price),
            "StrategyName_En_Compra": signal_info.get("strategy_name", "Desconocida"),
            "SL_ATR_Multiplier": str(sl_atr_mult),
            "market_type": market_type,
            "entry_side": signal_info.get('type', 'LONG')  # Almacenar 'LONG' o 'SHORT'
        }
        app_state.open_new_position(symbol, position)
        app_state.save(transaction_id=f"{transaction_id}_SaveOpenMinimal")
        # 4. Intentar colocar protección
        try:
            protection_meta = await protection_manager.place_entry_with_exchange_protection(
                client=client,
                symbol=symbol,
                side='BUY' if signal_info.get('type', 'LONG').upper() in ['BUY', 'LONG'] else 'SELL',
                requested_quantity=Decimal(executed_qty),  # Usar la cantidad exacta llenada
                sl_price=Decimal(sl_price),
                tp_price=Decimal(tp_price),
                transaction_id=transaction_id + "_PROT",
                market_type=market_type
            )
            # Enriquecer posición con metadata de protección
            position['protection_meta'] = protection_meta
            app_state.open_new_position(symbol, position)
            app_state.save(transaction_id=f"{transaction_id}_SaveWithProtection")
            logger.info(f"✅📈 NUEVA POSICIÓN ABIERTA con protección (exchange) para {symbol}: ID {position.get('TradeID')}", extra=log_extra)
        except protection_manager.ProtectionPlacementError as e_prot:
            # Si la colocación de protección falló, la posición se mantiene
            # y el 'protection_manager' ya armó el watcher de emergencia.
            logger.error(f"Colocación de protección falló para {symbol}: {e_prot}. Posición guardada. Watcher de emergencia armado.", extra=log_extra, exc_info=False)
            # El estado 'position' (sin 'protection_meta') ya está guardado.
    except Exception as e:
        logger.critical(f"Error crítico al intentar guardar la nueva posición (async): {e}", exc_info=True, extra=log_extra)
# --- FIN DE MODIFICACIÓN ---


async def async_check_and_manage_existing_position(symbol, df, transaction_id):
    """
    Wrapper Asíncrono para check_and_manage_existing_position.
    Ejecuta la lógica de gestión (incluyendo I/O de cierre) en un hilo separado.
    """
    return await asyncio.to_thread(
        check_and_manage_existing_position,
        symbol, df, transaction_id
    )