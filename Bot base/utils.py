from unified_logger import get_logger
import logging
import os
import shutil
import datetime
import json
import sys
import time
from decimal import Decimal, ROUND_DOWN, InvalidOperation, getcontext
import pytz
from typing import Tuple, Optional, Dict, Any
import pandas as pd # <--- CORRECCIÓN: Importar pandas

import path_manager
import unified_logger
import config
import json_writer
import binance_interaction as bi
import dynamic_params
import technical_analysis as ta_module
# --- INICIO DE MODIFICACIÓN: (v2.9.1) Tarea 2 ---
import advanced_risk_management
# --- FIN DE MODIFICACIÓN ---
from exceptions import ConfigError, CriticalBinanceError

# aumentar precisión Decimal si es necesario
getcontext().prec = 28

logger = get_logger(__name__)


# ---------------------------------------------------------------------------
# ConsoleFormatter
# ---------------------------------------------------------------------------
class ConsoleFormatter(logging.Formatter):
    def __init__(self):
        super().__init__()
        # formato breve para logs del bot (estrategias y componentes core)
        self.bot_format = '%(asctime)s,%(msecs)03d - %(levelname)s - [%(transaction_id)s|%(symbol)s] - %(message)s'
        # formato por defecto para utilidades / scripts
        self.default_format = '%(asctime)s,%(msecs)03d - %(levelname)s - [%(name)s] - [%(transaction_id)s|%(symbol)s] - %(message)s'
        self.datefmt = '%Y-%m-%d %H:%M:%S'

    def format(self, record: logging.LogRecord) -> str:
        # asegurar campos extra
        if not hasattr(record, 'transaction_id'):
            record.transaction_id = 'N/A'
        if not hasattr(record, 'symbol'):
            record.symbol = 'N/A'

        # módulos que preferimos con formato bot
        bot_module_names = [
            '__main__', 'strategies', 'config', 'utils', 'binance_interaction', 'position_management', 'market_analyzer',
            'strategy_hub', 'dynamic_params', 'technical_analysis', 'market_scanner', 'excel_logger', 'filters_manager',
            'redis_manager', 'unified_logger', 'path_manager', 'io_worker', 'json_writer', 'csv_writer',
            'advanced_risk_management'
        ]

        # si el logger pertenece a uno de los módulos listados o comienza con 'strategies', usamos bot_format
        if record.name == '__main__' or record.name.startswith('strategies') or record.name in bot_module_names:
            formatter = logging.Formatter(self.bot_format, datefmt=self.datefmt)
        else:
            formatter = logging.Formatter(self.default_format, datefmt=self.datefmt)
        return formatter.format(record)


# ---------------------------------------------------------------------------
# Logging setup wrapper
# ---------------------------------------------------------------------------
def setup_logging():
    """
    Inicializa logging usando unified_logger. Si falla, configura fallback básico.
    """
    try:
        unified_logger.setup_logging_from_config(config)
    except Exception as e:
        logging.basicConfig(level=logging.INFO)
        logger.error(
            f"Error fatal al configurar logging: {e}",
            exc_info=True,
            extra={'symbol': 'LOG_SETUP_FAIL'}
        )


# ---------------------------------------------------------------------------
# Config validation
# ---------------------------------------------------------------------------
def validate_config_keys(config_dict: dict, transaction_id: str) -> None:
    """
    Valida que las secciones mínimas existan en el dict de configuración.
    Lanza ConfigError con lista de errores si algo crítico falta.
    """
    log_extra = {'transaction_id': transaction_id, 'symbol': 'CONFIG_VALIDATION'}
    required_sections = [
        "GLOBAL_SETTINGS", "CAPITAL_ALLOCATION_CONFIG", "RISK_MANAGEMENT_CONFIG",
        "PROFITABILITY_CONFIG", "POSITION_EXIT_CONFIG", "MARKET_QUALITY_FILTERS",
        "MARKET_SCANNER_CONFIG", "MARKET_ANALYZER_CONFIG", "CIRCUIT_BREAKER_CONFIG",
        "STRATEGIES_CONFIG", "DYNAMIC_PARAMS_CONFIG"
    ]
    # PORTFOLIO_ALLOCATION es tratado como opcional pero lo listamos para advertencia
    required_sections.extend(["PORTFOLIO_ALLOCATION"])

    errors = []
    for section in required_sections:
        if section not in config_dict:
            if section == "PORTFOLIO_ALLOCATION":
                logger.warning(
                    f"Sección opcional '{section}' no encontrada. Se usará allocation_weight = 1.0",
                    extra=log_extra
                )
            else:
                errors.append(f"Sección requerida '{section}' no encontrada.")

    # Validar intervalo
    if "GLOBAL_SETTINGS" in config_dict:
        try:
            interval = config_dict["GLOBAL_SETTINGS"].get("INTERVAL")
            if not interval or interval not in config.VALID_BINANCE_INTERVALS:
                errors.append(
                    f"GLOBAL_SETTINGS.INTERVAL inválido o faltante. Valores válidos: {config.VALID_BINANCE_INTERVALS}"
                )
        except Exception:
            errors.append("GLOBAL_SETTINGS.INTERVAL inválido o faltante.")

    if errors:
        error_message = f"Errores de configuración detectados: {'; '.join(errors)}"
        logger.critical(error_message, extra=log_extra)
        raise ConfigError(error_message, errors_list=errors)
    else:
        logger.info("Validación básica de configuración completada.", extra=log_extra)


# ---------------------------------------------------------------------------
# Symbol pair helpers
# ---------------------------------------------------------------------------
def split_symbol_pair(symbol: str) -> Tuple[Optional[str], Optional[str]]:
    """
    Dado un ticker como 'BTCUSDT', retorna ('BTC', 'USDT'). Soporta quotes comunes.
    """
    quote_assets = ["USDT", "BUSD", "USDC", "TUSD", "FDUSD", "DAI", "EUR", "GBP", "TRY"]
    if not symbol or not isinstance(symbol, str):
        return None, None
    for quote in quote_assets:
        if symbol.endswith(quote):
            base = symbol[:-len(quote)]
            return base, quote
    # fallback: intentar 4 o 3 chars
    if len(symbol) > 4:
        potential_quote = symbol[-4:]
        if potential_quote in quote_assets:
            return symbol[:-4], potential_quote
        potential_quote = symbol[-3:]
        if potential_quote in quote_assets:
            return symbol[:-3], potential_quote
    logger.warning(f"No se pudo determinar el par base/cotizado para {symbol}", extra={'symbol': symbol})
    return None, None


# ---------------------------------------------------------------------------
# Safe conversions / helpers
# ---------------------------------------------------------------------------
def safe_decimal(value: Any, default: Decimal = Decimal('0')) -> Decimal:
    """
    Convierte a Decimal de forma segura, devolviendo default en caso de error.
    """
    try:
        if isinstance(value, Decimal):
            return value
        if value is None:
            return default
        return Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError):
        return default


def safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except Exception:
        return default


def now_utc_iso() -> str:
    return datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc).isoformat()


def localize_timestamp(ts: datetime.datetime, tz_name: str = 'America/Argentina/Buenos_Aires') -> datetime.datetime:
    try:
        tz = pytz.timezone(tz_name)
        if ts.tzinfo is None:
            return tz.localize(ts)
        return ts.astimezone(tz)
    except Exception:
        return ts


# ---------------------------------------------------------------------------
# Stop loss / take profit calculations
# ---------------------------------------------------------------------------
def _calculate_stop_loss_take_profit(symbol: str, df_klines: pd.DataFrame, signal_params: dict,
                                     vol_ratio: Decimal, transaction_id: str) -> Tuple[Optional[Decimal], Optional[Decimal]]:
    """
    Calcula stop loss y take profit basados en ATR y parámetros dinámicos.
    Retorna (stop_loss_price, take_profit_price) o (None, None) si no es válido.
    """
    log_extra = {'transaction_id': transaction_id, 'symbol': symbol}

    try:
        current_price_raw = df_klines['close'].iloc[-1]
        current_price = safe_decimal(current_price_raw)
        if current_price is None or current_price <= 0:
            logger.warning("Precio actual inválido, no se puede calcular SL/TP.", extra=log_extra)
            return None, None

        atr_period_short = int(config.get_param("MARKET_ANALYZER_CONFIG.ATR_SHORT_PERIOD", 7))
        _, atr_value_raw = ta_module.get_atr(df_klines, length=atr_period_short)
        atr_value = safe_decimal(atr_value_raw)

        if atr_value is None or atr_value <= 0:
            logger.warning("ATR inválido, no se puede calcular SL.", extra=log_extra)
            return None, None

        # multiplicador dinámico desde dynamic_params
        sl_atr_multiplier = safe_decimal(dynamic_params.get_dynamic_risk_multiplier(vol_ratio, symbol), Decimal('1.0'))

        stop_loss_calculated = current_price - (atr_value * sl_atr_multiplier)
        stop_loss_price = safe_decimal(bi.format_price(symbol, stop_loss_calculated))
        if stop_loss_price is None:
            logger.warning("Formato SL inválido tras formateo.", extra=log_extra)
            return None, None

        if stop_loss_price >= current_price:
            logger.warning(
                f"SL calculado ({stop_loss_price}) >= Precio actual ({current_price}). SL inválido.",
                extra=log_extra
            )
            return None, None

        distancia_stop_loss = current_price - stop_loss_price
        if distancia_stop_loss <= 0:
            logger.warning(
                f"Distancia a SL ({distancia_stop_loss}) es cero o negativa después de formatear. SL inválido.",
                extra=log_extra
            )
            return None, None

        risk_reward_ratio = safe_decimal(config.get_param("RISK_MANAGEMENT_CONFIG.RISK_REWARD_RATIO", "1.5"), Decimal('1.5'))
        distancia_take_profit = distancia_stop_loss * risk_reward_ratio
        take_profit_price_calculated = current_price + distancia_take_profit
        take_profit_price = safe_decimal(bi.format_price(symbol, take_profit_price_calculated))

        if take_profit_price is None or take_profit_price <= current_price:
            logger.warning(
                f"TP calculado ({take_profit_price}) <= Precio actual ({current_price}). TP inválido.",
                extra=log_extra
            )
            return None, None

        return stop_loss_price, take_profit_price
    except Exception as e:
        logger.error(f"Error calculando SL/TP: {e}", exc_info=True, extra=log_extra)
        return None, None


# ---------------------------------------------------------------------------
# Base size calculator
# ---------------------------------------------------------------------------
def _calculate_base_position_size(symbol: str, vol_ratio: Decimal, transaction_id: str) -> Decimal:
    log_extra = {'transaction_id': transaction_id, 'symbol': symbol}
    try:
        risk_config = config.get_param("RISK_MANAGEMENT_CONFIG", symbol=symbol, return_dict_for_complex=True) or {}
        base_size = safe_decimal(risk_config.get("BASE_POSITION_SIZE", "1000.0"), Decimal('1000.0'))

        vol_high_thresh = safe_decimal(config.get_param("DYNAMIC_PARAMS_CONFIG.VOL_RATIO_HIGH_THRESHOLD", "1.3"), Decimal('1.3'))
        vol_low_thresh = safe_decimal(config.get_param("DYNAMIC_PARAMS_CONFIG.VOL_RATIO_LOW_THRESHOLD", "0.8"), Decimal('0.8'))

        if vol_ratio > vol_high_thresh:
            base_size = (base_size * Decimal('0.7')).quantize(Decimal('0.00000001'))
        elif vol_ratio < vol_low_thresh:
            base_size = (base_size * Decimal('1.2')).quantize(Decimal('0.00000001'))

        return base_size
    except Exception as e:
        logger.error(f"Error en _calculate_base_position_size: {e}", exc_info=True, extra=log_extra)
        return Decimal('1000.0')


# ---------------------------------------------------------------------------
# Main function: calculate_position_details
# ---------------------------------------------------------------------------
def calculate_position_details(symbol: str, df_klines: pd.DataFrame, signal_params: dict,
                               vol_ratio: Decimal, transaction_id: str) -> Tuple[Optional[Decimal], Optional[Decimal], Optional[Decimal]]:
    """
    Calcula tamaño de posición (en activo), SL y TP tomando en cuenta:
    - allocation weight
    - dynamic allocation (advanced_risk_management)
    - formato de cantidad y validaciones (min_qty / min_notional)
    - rentabilidad mínima vs comisiones
    Retorna (trade_amount_asset, sl_price, tp_price) o (None, None, None) si falla.
    """
    log_extra = {'transaction_id': transaction_id, 'symbol': symbol}
    allocation_multiplier = Decimal('1.0')  # por defecto

    try:
        current_price_raw = df_klines['close'].iloc[-1]
        current_price = safe_decimal(current_price_raw)
        if current_price is None or current_price <= 0:
            logger.warning("Precio actual inválido, no se puede calcular posición.", extra=log_extra)
            return None, None, None

        # 1. Estrategia y nombre
        strategy_name = signal_params.get("strategy_name", "Desconocida") if isinstance(signal_params, dict) else "Desconocida"

        # 2. Allocation weight (estático)
        allocation_weight = Decimal('1.0')
        try:
            portfolio_alloc = config.get_param("PORTFOLIO_ALLOCATION", return_dict_for_complex=True) or {}
            if isinstance(portfolio_alloc, dict) and strategy_name in portfolio_alloc:
                allocation_weight = safe_decimal(portfolio_alloc[strategy_name], Decimal('1.0'))
                logger.debug(f"Allocation weight (estático) para {strategy_name}: {allocation_weight}", extra=log_extra)
        except Exception:
            allocation_weight = Decimal('1.0')

        # 3. Base size USDT
        base_size_usdt = _calculate_base_position_size(symbol, vol_ratio, transaction_id)

        # 4. Aplicar allocation weight (estático)
        weighted_size_usdt = (base_size_usdt * allocation_weight).quantize(Decimal('0.00000001'))

        # 5. Calcular SL/TP: si la estrategia ya los provee, usar los provistos
        sl_price = None
        tp_price = None
        if isinstance(signal_params, dict) and "stop_loss_price" in signal_params and "take_profit_price" in signal_params:
            logger.debug(f"Usando SL/TP dinámico provisto por la estrategia {strategy_name}.", extra=log_extra)
            sl_price = safe_decimal(signal_params.get("stop_loss_price"))
            tp_price = safe_decimal(signal_params.get("take_profit_price"))

            if sl_price is None or tp_price is None or sl_price <= 0 or tp_price <= 0:
                logger.warning(f"Estrategia {strategy_name} proveyó SL/TP nulo o inválido. Abortando.", extra=log_extra)
                return None, None, None
        else:
            logger.debug(f"Estrategia {strategy_name} no proveyó SL/TP. Usando cálculo estándar.", extra=log_extra)
            sl_price, tp_price = _calculate_stop_loss_take_profit(symbol, df_klines, signal_params, vol_ratio, transaction_id)

        if sl_price is None or tp_price is None:
            logger.warning("SL o TP inválidos, abortando cálculo.", extra=log_extra)
            return None, None, None

        # 6. Lógica de conversión a cantidad de activo
        # trade_amount_asset_calculated = weighted_size_usdt / current_price
        trade_amount_asset_calculated = (weighted_size_usdt / current_price).quantize(Decimal('0.00000001'))

        # 7. Compensación por comisión (si se desea que el notional cubra comisión)
        commission_rate = safe_decimal(config.get_param("PROFITABILITY_CONFIG.COMMISSION_PERCENT", "0.001"), Decimal('0.001'))
        try:
            compensation_factor = (Decimal("1.0") / (Decimal("1.0") - commission_rate)).quantize(Decimal('0.00000001'))
        except (InvalidOperation, ZeroDivisionError):
            compensation_factor = Decimal('1.0')

        trade_amount_asset_calculated = (trade_amount_asset_calculated * compensation_factor).quantize(Decimal('0.00000001'))

        # 8. Formatear cantidad con la función del exchange
        trade_amount_asset = bi.format_quantity(symbol, trade_amount_asset_calculated)

        # validar que sea > 0
        if trade_amount_asset is None or safe_decimal(trade_amount_asset) <= 0:
            logger.error(
                f"Cantidad de activo calculada (pre-dinámica) inválida ({trade_amount_asset_calculated} -> {trade_amount_asset})",
                extra=log_extra
            )
            return None, None, None

        # --- INICIO DE MODIFICACIÓN: (v2.9.1) Tarea 2: Aplicar multiplicador dinámico ---
        vol_high_thresh = safe_decimal(config.get_param("DYNAMIC_PARAMS_CONFIG.VOL_RATIO_HIGH_THRESHOLD", "1.3"), Decimal('1.3'))
        volatility_regime = "HIGH" if vol_ratio > vol_high_thresh else "NORMAL"

        try:
            allocation_multiplier = advanced_risk_management.get_dynamic_allocation_multiplier(
                symbol, strategy_name, volatility_regime, transaction_id
            ) or Decimal('1.0')
            allocation_multiplier = safe_decimal(allocation_multiplier, Decimal('1.0'))
        except Exception:
            allocation_multiplier = Decimal('1.0')

        if allocation_multiplier != Decimal('1.0'):
            logger.debug(
                f"Aplicando Allocation dinámico: {allocation_multiplier}x para {symbol} [{strategy_name}]",
                extra=log_extra
            )
            try:
                trade_amount_asset = bi.format_quantity(symbol, (safe_decimal(trade_amount_asset) * allocation_multiplier).quantize(Decimal('0.00000001')))
            except Exception:
                # si falló el reformat, revertir a valor anterior y loggear
                logger.warning("Falló re-formateo post allocation_multiplier, manteniendo cantidad anterior.", extra=log_extra)
        # --- FIN DE MODIFICACIÓN ---

        # 9. Validar mínimo notional y qty
        min_qty, min_notional = bi.get_min_notional_and_qty(symbol, transaction_id=f"{transaction_id}_GetMinNot")
        if min_notional is None:
            logger.error("No se pudo validar el notional mínimo.", extra=log_extra)
            return None, None, None

        current_notional = (safe_decimal(trade_amount_asset) * current_price).quantize(Decimal('0.00000001'))

        if current_notional < min_notional:
            logger.warning(
                f"Notional (post-dinámico) ({current_notional:.2f}) < Mínimo ({min_notional}). Ajustando a mínimo notional.",
                extra=log_extra
            )
            trade_amount_asset_calculated = ((min_notional * Decimal("1.01")) / current_price).quantize(Decimal('0.00000001'))
            trade_amount_asset = bi.format_quantity(symbol, trade_amount_asset_calculated)

        # Si min_qty existe, forzar ajuste
        if min_qty and safe_decimal(trade_amount_asset) < min_qty:
            logger.warning(
                f"Position size (post-dinámico) ({trade_amount_asset}) < min_qty ({min_qty}). Ajustando a min_qty.",
                extra=log_extra
            )
            trade_amount_asset = min_qty

        # revalidar notional
        current_notional = (safe_decimal(trade_amount_asset) * current_price).quantize(Decimal('0.00000001'))
        if current_notional < min_notional:
            logger.error(
                f"Ajuste final (post-dinámico) falló. Notional ({current_notional:.2f}) < Mínimo ({min_notional}). Abortando.",
                extra=log_extra
            )
            return None, None, None

        # 10. Filtro de rentabilidad mínima vs comisión
        if config.get_param("RISK_MANAGEMENT_CONFIG.ENABLE_PROFITABILITY_FILTER", True):
            estimated_commission_cost = (current_notional * commission_rate).quantize(Decimal('0.00000001'))
            min_factor = safe_decimal(config.get_param("RISK_MANAGEMENT_CONFIG.MIN_PROFIT_VS_COMMISSION_FACTOR", "2.0"), Decimal('2.0'))
            min_profit_needed = (estimated_commission_cost * min_factor).quantize(Decimal('0.00000001'))
            potential_profit = ((tp_price - current_price) * safe_decimal(trade_amount_asset)).quantize(Decimal('0.00000001'))

            if potential_profit < min_profit_needed:
                logger.info(
                    f"Filtro de Rentabilidad Mínima RECHAZADO (Post-Dinámico). Ganancia Potencial ({potential_profit:.4f}) < Mínimo Requerido ({min_profit_needed:.4f})",
                    extra=log_extra
                )
                return None, None, None
            logger.info("✅ Filtro de Rentabilidad Mínima OK.", extra=log_extra)

        logger.info(
            f"Position size calculado (Final): {trade_amount_asset} (Base USDT: {base_size_usdt:.2f} × Weight: {allocation_weight} × Dyn: {allocation_multiplier})",
            extra=log_extra
        )

        # retornar (cantidad activo, sl, tp)
        return safe_decimal(trade_amount_asset), sl_price, tp_price

    except InvalidOperation as e:
        logger.error(
            f"Error de operación Decimal al calcular detalles: {e}. Verificar configuración.",
            exc_info=True,
            extra=log_extra
        )
        return None, None, None
    except KeyError as e:
        logger.error(f"Falta una clave esperada en los datos de klines: {e}", exc_info=True, extra=log_extra)
        return None, None, None
    except IndexError as e:
        logger.error("Índice fuera de rango, probablemente datos insuficientes en klines: %s", str(e), exc_info=True, extra=log_extra)
        return None, None, None
    except Exception as e:
        logger.error(f"Error inesperado calculando detalles de posición: {e}", exc_info=True, extra=log_extra)
        return None, None, None


# ---------------------------------------------------------------------------
# Backup diario
# ---------------------------------------------------------------------------
def handle_daily_backup(transaction_id: str) -> None:
    """
    Realiza backup diario del archivo de trades cerrados (Excel) configurado en GLOBAL_SETTINGS.
    Actualiza el state en position_management para no repetir en el mismo día.
    """
    import position_management as pm  # import local para evitar dependencia circular
    log_extra = {'transaction_id': transaction_id, 'symbol': 'BACKUP'}
    today_iso = datetime.date.today().isoformat()
    last_backup_date = pm.get_bot_state_value("last_backup_date")

    if last_backup_date == today_iso:
        logger.debug("Backup diario ya realizado hoy. Saltando.", extra=log_extra)
        return

    logger.info(f"Realizando backup diario (Último: {last_backup_date}, Hoy: {today_iso})...", extra=log_extra)

    excel_filename_config = config.get_param("GLOBAL_SETTINGS.CLOSED_TRADES_EXCEL_FILE", default=None)

    if not excel_filename_config:
        logger.warning("Ruta de archivo Excel no configurada (CLOSED_TRADES_EXCEL_FILE). Saltando backup.", extra=log_extra)
        pm.set_bot_state_value("last_backup_date", today_iso)
        pm.save_app_state(f"{transaction_id}_SaveBackupDateOnly")
        return

    try:
        excel_filepath = path_manager.get_path("trades_excel")
        backup_dir = path_manager.get_path("backup_destination_path")
    except (KeyError, IOError) as e:
        logger.error(f"Error obteniendo rutas de backup desde path_manager: {e}. Saltando backup.", extra=log_extra)
        pm.set_bot_state_value("last_backup_date", today_iso)
        pm.save_app_state(f"{transaction_id}_SaveBackupDateOnly")
        return

    if not os.path.exists(excel_filepath):
        logger.warning(f"Archivo Excel '{excel_filepath}' no encontrado. Saltando backup.", extra=log_extra)
        pm.set_bot_state_value("last_backup_date", today_iso)
        pm.save_app_state(f"{transaction_id}_SaveBackupDateNotFound")
        return

    backup_filename = f"{os.path.splitext(excel_filename_config)[0]}_{today_iso}.xlsx"
    backup_filepath = os.path.join(backup_dir, backup_filename)

    try:
        shutil.copy2(excel_filepath, backup_filepath)
        logger.info(f"Backup creado exitosamente en: {backup_filepath}", extra=log_extra)
        pm.set_bot_state_value("last_backup_date", today_iso)
        pm.save_app_state(f"{transaction_id}_SaveBackupSuccess")
    except Exception as e:
        logger.error(f"Error al crear backup: {e}", exc_info=True, extra=log_extra)


# ---------------------------------------------------------------------------
# Utilities: unique id, scan saving
# ---------------------------------------------------------------------------
def generate_unique_id(length: int = 4) -> str:
    import uuid
    return str(uuid.uuid4())[:length]


def save_scan_results_to_file(scan_data: dict, transaction_id: str) -> None:
    """
    Guarda resultados de un scan en un archivo JSON con timestamps; convierte Decimal y datetimes.
    """
    log_extra = {'transaction_id': transaction_id, 'symbol': 'SCAN_SAVE'}
    try:
        try:
            results_dir_abs = path_manager.get_path("scan_results_directory")
        except (KeyError, IOError) as e:
            logger.error(f"No se pudo obtener/crear 'scan_results_directory': {e}. No se guardarán los resultados.", extra=log_extra)
            return

        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"scan_results_{timestamp}.json"
        filepath = os.path.join(results_dir_abs, filename)

        def decimal_default(obj):
            if isinstance(obj, Decimal):
                return str(obj)
            if isinstance(obj, datetime.datetime) or isinstance(obj, datetime.date):
                return obj.isoformat()
            raise TypeError(f"Object of type {obj.__class__.__name__} is not JSON serializable")

        # intentar crear directorio si no existe
        try:
            os.makedirs(results_dir_abs, exist_ok=True)
        except Exception:
            pass

        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(scan_data, f, indent=4, default=decimal_default)

        logger.debug(f"Resultados del escaneo guardados en: {filepath}", extra=log_extra)
    except Exception as e:
        logger.error(f"Error al guardar los resultados del escaneo: {e}", exc_info=True, extra=log_extra)


# ---------------------------------------------------------------------------
# Trading metrics logging
# ---------------------------------------------------------------------------
def log_trading_metrics(symbol: str, signal: Optional[dict], market_state: str, vol_ratio: Decimal,
                        df_klines: Optional[pd.DataFrame], transaction_id: str) -> None:
    """
    Loggea métricas en system.log y escribe decisión en decision_logs vía json_writer.
    """
    log_extra = {'transaction_id': transaction_id, 'symbol': symbol}
    try:
        price = None
        if df_klines is not None and not df_klines.empty:
            price = df_klines['close'].iloc[-1]

        signal_type = signal.get('type') if signal else None
        strategy = signal.get('strategy_name') if signal else None

        metrics = {
            'timestamp': datetime.datetime.utcnow().isoformat(),
            'symbol': symbol,
            'signal_type': signal_type,
            'strategy': strategy,
            'market_state': market_state,
            'volatility_ratio': float(vol_ratio) if vol_ratio is not None else 0.0,
            'transaction_id': transaction_id,
            'price': float(price) if price is not None else 0.0
        }
        logger.info(f"TRADING_METRICS: {json.dumps(metrics)}", extra=log_extra)

        # Enviar a json_writer para persistencia (IOWorker u otro mecanismo)
        json_writer.log_decision(
            symbol=symbol,
            strategy=strategy,
            signal_type=signal_type,
            vol_ratio=float(vol_ratio) if vol_ratio is not None else 0.0,
            market_state=market_state,
            price=price,
            transaction_id=transaction_id
        )
    except Exception as e:
        logger.warning(f"Error al loggear métricas de trading: {e}", extra=log_extra)


# ---------------------------------------------------------------------------
# Wait for next cycle (main loop helper)
# ---------------------------------------------------------------------------
def wait_for_next_cycle(loop_start_time: float, iter_id_base: str) -> None:
    """
    Calcula tiempo a dormir para cumplir BOT_SLEEP_INTERVAL_SECONDS y duerme
    mientras loggea duración de iteración y posiciones activas.
    """
    import position_management as pm
    log_extra_iter = {'transaction_id': iter_id_base, 'symbol': 'LOOP_ITER'}
    try:
        elapsed_time = time.time() - loop_start_time
        sleep_interval = int(config.get_param("GLOBAL_SETTINGS.BOT_SLEEP_INTERVAL_SECONDS", 15))
        wait_time = max(0, sleep_interval - elapsed_time)

        active_positions = []
        try:
            active_positions = pm.get_all_active_positions()
        except Exception:
            active_positions = []

        logger.info(
            f"✨Iteración {iter_id_base} completada en {elapsed_time:.2f}s. ➡️Posiciones Activas: {len(active_positions)}. Esperando {wait_time:.2f}s...",
            extra=log_extra_iter
        )

        if wait_time > 0:
            time.sleep(wait_time)
    except Exception as e:
        logger.error(f"Error en wait_for_next_cycle: {e}", exc_info=True, extra=log_extra_iter)


# ---------------------------------------------------------------------------
# Misc helpers: filesystem cleanup, rotate, ensure dir
# ---------------------------------------------------------------------------
def safe_remove(path: str) -> None:
    try:
        if os.path.exists(path):
            os.remove(path)
    except Exception as e:
        logger.debug(f"safe_remove fallo para {path}: {e}", extra={'symbol': 'FILESYSTEM'})


def ensure_dir(path: str) -> None:
    try:
        os.makedirs(path, exist_ok=True)
    except Exception as e:
        logger.debug(f"No se pudo crear directorio {path}: {e}", extra={'symbol': 'FILESYSTEM'})


def rotate_file_if_exists(src: str, keep_timestamp: bool = True) -> Optional[str]:
    """
    Si src existe, mueve a un archivo .old_<timestamp> dentro del mismo directorio.
    Retorna el nuevo path o None si no existía.
    """
    try:
        if not os.path.exists(src):
            return None
        dirpath = os.path.dirname(src)
        basename = os.path.basename(src)
        ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S") if keep_timestamp else "old"
        dst = os.path.join(dirpath, f"{basename}.old_{ts}")
        shutil.move(src, dst)
        return dst
    except Exception as e:
        logger.debug(f"rotate_file_if_exists fallo para {src}: {e}", extra={'symbol': 'FILESYSTEM'})
        return None


# ---------------------------------------------------------------------------
# JSON helpers with Decimal support
# ---------------------------------------------------------------------------
def json_dumps_with_decimal(obj: Any, **kwargs) -> str:
    def default(o):
        if isinstance(o, Decimal):
            return str(o)
        if isinstance(o, (datetime.date, datetime.datetime)):
            return o.isoformat()
        raise TypeError(f"Type {type(o)} not serializable")

    return json.dumps(obj, default=default, **kwargs)


def json_loads_with_decimal(s: str) -> Any:
    """
    Carga JSON y convierte números a Decimal cuando sea apropiado.
    Nota: para simplificar, retorna floats en arrays/objects; se puede adaptar si se desea Decimal estricto.
    """
    return json.loads(s)


# ---------------------------------------------------------------------------
# Small utilities used by other modules
# ---------------------------------------------------------------------------
def human_readable_bytes(num: int, suffix='B') -> str:
    for unit in ['','K','M','G','T','P']:
        if abs(num) < 1024.0:
            return f"{num:3.1f}{unit}{suffix}"
        num /= 1024.0
    return f"{num:.1f}P{suffix}"


def seconds_to_hms(s: int) -> str:
    h = s // 3600
    m = (s % 3600) // 60
    sec = s % 60
    if h:
        return f"{h}h{m}m{sec}s"
    if m:
        return f"{m}m{sec}s"
    return f"{sec}s"


# ---------------------------------------------------------------------------
# File-backed simple locks (for scripts that run in parallel)
# ---------------------------------------------------------------------------
def acquire_file_lock(lockfile_path: str, ttl_seconds: int = 60) -> bool:
    try:
        if os.path.exists(lockfile_path):
            # comprobar edad
            age = time.time() - os.path.getmtime(lockfile_path)
            if age > ttl_seconds:
                try:
                    os.remove(lockfile_path)
                except Exception:
                    pass
            else:
                return False
        with open(lockfile_path, 'w') as f:
            f.write(str(os.getpid()))
        return True
    except Exception:
        return False


def release_file_lock(lockfile_path: str) -> None:
    try:
        if os.path.exists(lockfile_path):
            os.remove(lockfile_path)
    except Exception:
        pass


# ---------------------------------------------------------------------------
# Utilities for configuration / env printing (debug helpers)
# ---------------------------------------------------------------------------
def dump_effective_config(out_path: str) -> None:
    """
    Serializa la configuración en un archivo local (útil para debugging).
    """
    try:
        cfg = {}
        # attempt to shallow-copy config attributes if possible
        try:
            cfg = dict(config.__dict__)
        except Exception:
            try:
                cfg = config.get_all() if hasattr(config, 'get_all') else {}
            except Exception:
                cfg = {}

        with open(out_path, 'w', encoding='utf-8') as f:
            f.write(json_dumps_with_decimal(cfg, indent=2))
        logger.debug(f"Configuración volcada a: {out_path}", extra={'symbol': 'CONFIG_DUMP'})
    except Exception as e:
        logger.debug(f"dump_effective_config fallo: {e}", extra={'symbol': 'CONFIG_DUMP'})


# ---------------------------------------------------------------------------
# Helper: pretty print for strategies to use when debugging
# ---------------------------------------------------------------------------
def pretty_df_head(df: pd.DataFrame, n: int = 5) -> str:
    try:
        return df.head(n).to_string()
    except Exception:
        return "<no-data>"


# ---------------------------------------------------------------------------
# Simple CSV writer guard (atomic)
# ---------------------------------------------------------------------------
def atomic_write_text_file(path: str, content: str, mode: str = 'w', encoding: str = 'utf-8') -> None:
    tmp = f"{path}.tmp"
    try:
        with open(tmp, 'w', encoding=encoding) as f:
            f.write(content)
        os.replace(tmp, path)
    except Exception as e:
        logger.debug(f"atomic_write_text_file fallo: {e}", extra={'symbol': 'FILESYSTEM'})
        try:
            if os.path.exists(tmp):
                os.remove(tmp)
        except Exception:
            pass


# ---------------------------------------------------------------------------
# End of file marker
# ---------------------------------------------------------------------------
# utils.py - End