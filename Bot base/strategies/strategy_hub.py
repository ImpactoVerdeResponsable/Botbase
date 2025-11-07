# strategies/strategy_hub.py
from unified_logger import get_logger
from decimal import Decimal
import config
import binance_interaction as bi
import dynamic_params

# --- IMPORTS DE ESTRATEGIAS ACTIVAS ---
from strategies import volatility_strategy
from strategies import volatility_breakout_strategy
from strategies import micro_scalping_rsi
from strategies import microstructure_sweep
# --- INICIO DE MODIFICACIÓN: (Fase 2) ---
from strategies import micro_scalping_enhanced
import shadow_logger
# --- FIN DE MODIFICACIÓN ---

logger = get_logger(__name__)


def get_signal(market_state, df, configs, vol_ratio, macro_trend_is_up, transaction_id, **kwargs):
    """
    Módulo Hub Central de Estrategias de Trading (v2.9.2)

    Flujo:
    1. Determina estado del mercado
    2. Selecciona estrategia según mapeo
    3. Ejecuta estrategia (si está habilitada)
    4. Aplica filtro de spread (v2.9.1)

    Args:
        market_state: Estado del mercado identificado
        df: DataFrame con datos de precio
        configs: Configuraciones de estrategias (dict-like)
        vol_ratio: Ratio de volatilidad calculado
        macro_trend_is_up: Tendencia macro alcista/bajista (bool)
        transaction_id: ID de transacción para logging
        **kwargs: Datos adicionales (ej. datos de microestructura)

    Returns:
        dict or None: Señal de trading o None si no hay señal
    """
    log_extra = {
        'transaction_id': transaction_id,
        'symbol': df.attrs.get('symbol', 'N/A') if hasattr(df, 'attrs') else 'N/A'
    }

    # --- INICIO DE MODIFICACIÓN: (FIX v2.9.2 - Eliminar Filtro Redundante) ---
    # El filtro de spread global obsoleto (MARKET_QUALITY_FILTERS) se elimina.
    # Ahora confiamos exclusivamente en el filtro de spread por estrategia
    # (apply_spread_filter) que se ejecuta DESPUÉS de obtener la señal.
    # --- FIN DE MODIFICACIÓN ---

    # --- MAPA DE ESTRATEGIAS ACTIVAS (OPTIMIZADO) ---
    strategy_map = {
        "FUERTE_ALCISTA": (micro_scalping_rsi.get_signal, configs.get("MicroScalpingRSI")),
        "INICIO_ALCISTA": (volatility_strategy.get_signal, configs.get("ImpulsoDeVolatilidad")),
        "LATERAL_CON_SESGO_ALCISTA": (micro_scalping_rsi.get_signal, configs.get("MicroScalpingRSI")),

        # Mapeo de MicrostructureSweep (según bot_settings.json v2.9.1)
        "LATERAL_CONSOLIDACION": (microstructure_sweep.get_signal, configs.get("MicrostructureSweep")),
        "COMPRESION_VOLATIL": (microstructure_sweep.get_signal, configs.get("MicrostructureSweep")),

        # Estados de No Operación (mantener)
        "FUERTE_BAJISTA": (None, None),
        "INICIO_BAJISTA": (None, None),
        "LATERAL_CON_SESGO_BAJISTA": (None, None),
        "INCIERTA": (None, None),
        "CHOPPY": (None, None),
    }

    strategy_func, strategy_params = strategy_map.get(market_state, (None, None))

    # --- Fallback para Squeeze ---
    if market_state == "COMPRESION_VOLATIL" and (not strategy_func or not strategy_params or not strategy_params.get("enabled", False)):
        logger.debug(
            "MicrostructureSweep desactivada para COMPRESION_VOLATIL, intentando fallback a RupturaDeCompresion.",
            extra=log_extra
        )
        strategy_func, strategy_params = (volatility_breakout_strategy.get_signal, configs.get("RupturaDeCompresion"))

    # --- INICIO DE MODIFICACIÓN: (FIX v2.9.2 - UnboundLocalError) ---
    # 1. Definir strategy_name y strategy_enabled ANTES del bloque 'if'
    strategy_name = None
    strategy_enabled = False

    if strategy_func and strategy_params:
        try:
            # Intentar resolver un nombre legible para la estrategia buscando la entrada en configs
            if isinstance(configs, dict):
                # Buscar la clave cuyo valor coincida con strategy_params (esto depende de cómo se estructura configs)
                strategy_name = next(
                    (name for name, cfg in configs.items() if cfg is strategy_params or cfg == strategy_params),
                    None
                )
            if not strategy_name:
                # Fallback: usar el módulo de la función como identificador
                strategy_name = getattr(strategy_func, "__module__", None) or strategy_func.__name__
        except Exception:
            strategy_name = getattr(strategy_func, "__module__", None) or strategy_func.__name__

        # Asignar el estado 'enabled'
        try:
            strategy_enabled = bool(strategy_params.get("enabled", False))
        except Exception:
            strategy_enabled = False
    # --- FIN DE MODIFICACIÓN ---

    # --- INICIO DE MODIFICACIÓN: (Fase 2 - Ejecución paralela Shadow) ---
    # Esta lógica se ejecuta independientemente de la estrategia principal
    try:
        if config.get_param("SHADOW_OPTUNA.ENABLED", default_value=False):
            shadow_params = configs.get("MicroScalpingRSI_Enhanced") if isinstance(configs, dict) else None
            if shadow_params and shadow_params.get("enabled", False):
                try:
                    shadow_params = dict(shadow_params)  # evitar mutar el dict original
                    shadow_params['strategy_name'] = "MicroScalpingRSI_Enhanced"
                    # Ejecutar estrategia shadow (no debe afectar el flujo principal)
                    shadow_signal = micro_scalping_enhanced.get_signal(
                        df, shadow_params, vol_ratio, macro_trend_is_up, transaction_id, **kwargs
                    )
                    if shadow_signal:
                        # Loggear señal shadow (no se retorna, solo se loggea)
                        try:
                            shadow_logger.log_signal(shadow_signal, df, transaction_id)
                        except Exception:
                            # Si falla el logger shadow, no interrumpimos el flujo principal
                            logger.debug("Fallo al loggear señal shadow, continuar.", extra=log_extra)
                        logger.debug(f"[SHADOW] Señal 'MicroScalpingRSI_Enhanced' loggeada.", extra=log_extra)
                except Exception as e_shadow:
                    logger.error(
                        f"Error ejecutando shadow strategy 'MicroScalpingRSI_Enhanced': {e_shadow}",
                        exc_info=True,
                        extra=log_extra
                    )
    except Exception:
        # Si config.get_param falla o configs no es el formato esperado, continuar sin shadow
        logger.debug("No se pudo evaluar SHADOW_OPTUNA, se omite ejecución shadow.", extra=log_extra)
    # --- FIN DE MODIFICACIÓN ---

    # --- Lógica de Estrategia Principal (Real) ---
    if strategy_func and strategy_params and strategy_enabled:

        # VERIFICAR si la estrategia está permitida en este market_state
        allowed_states = strategy_params.get("ALLOWED_MARKET_STATES") if isinstance(strategy_params, dict) else None
        if allowed_states and market_state not in allowed_states:
            logger.debug(f"Estrategia {strategy_name} no permitida en {market_state}", extra=log_extra)
            return None

        try:
            # Inyectar nombre de estrategia en parámetros (trabajar sobre copia para no mutar configs global)
            if isinstance(strategy_params, dict):
                strategy_params = dict(strategy_params)
                strategy_params['strategy_name'] = strategy_name
            else:
                # si no es dict, intentar asignar atributo si es objeto
                try:
                    setattr(strategy_params, 'strategy_name', strategy_name)
                except Exception:
                    pass

            logger.info(f"🧠 Aplicando estrategia '{strategy_name}' para estado '{market_state}'", extra=log_extra)

            # Ejecutar estrategia pasando los kwargs
            signal = strategy_func(df, strategy_params, vol_ratio, macro_trend_is_up, transaction_id, **kwargs)

            if signal:
                # APLICAR FILTRO DE SPREAD AUTOMÁTICO (v2.9.1)
                # Se añade el nombre de la estrategia a la señal para que apply_spread_filter lo use
                try:
                    signal["strategy_name"] = strategy_name
                except Exception:
                    # si signal no es dict-like, intentar acceder por atributos
                    try:
                        setattr(signal, "strategy_name", strategy_name)
                    except Exception:
                        pass

                try:
                    filtered_signal = dynamic_params.apply_spread_filter(
                        df.attrs.get('symbol', 'N/A') if hasattr(df, 'attrs') else 'N/A',
                        signal.get("strategy_name", "Unknown") if isinstance(signal, dict) else strategy_name,
                        signal,
                        transaction_id
                    )
                except Exception as e_filter:
                    # Si falla el filtro por cualquier razón, registrar y considerar que la señal fue descartada
                    logger.error(f"Error aplicando filtro de spread para {strategy_name}: {e_filter}", exc_info=True, extra=log_extra)
                    return None

                if filtered_signal is None:
                    # El log ya se maneja dentro de apply_spread_filter
                    return None

                # Reemplazar señal por la filtrada
                signal = filtered_signal

                logger.debug(f"✅ Señal generada por {strategy_name}", extra=log_extra)

            return signal

        except Exception as e:
            logger.error(f"❌ Error ejecutando {strategy_name}: {e}", exc_info=True, extra=log_extra)
            return None
    else:
        # --- INICIO DE MODIFICACIÓN: (FIX v2.9.2 - UnboundLocalError) ---
        # Ahora 'strategy_name' y 'strategy_enabled' están definidos o son None/False
        if market_state not in ["FUERTE_BAJISTA", "INICIO_BAJISTA", "LATERAL_CON_SESGO_BAJISTA", "INCIERTA", "CHOPPY"]:

            # Esta lógica suprime el log "No hay estrategia" si la estrategia deshabilitada es
            # la que se esperaba (MicroScalpingRSI en estado FUERTE_ALCISTA).
            is_disabled_scalping = (
                market_state == "FUERTE_ALCISTA"
                and (strategy_name is None or strategy_name == "MicroScalpingRSI" or strategy_name == "MicroScalpingRSI_Enhanced")
                and not strategy_enabled
            )

            if not is_disabled_scalping:
                logger.debug(f"⏭️  No hay estrategia habilitada para estado '{market_state}'", extra=log_extra)
        # --- FIN DE MODIFICACIÓN ---
        return None
