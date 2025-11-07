# strategies/microstructure_sweep.py
from unified_logger import get_logger
from decimal import Decimal
import pandas as pd
import technical_analysis as ta_module
import config  # Para leer la configuración específica del símbolo

logger = get_logger(__name__)


def get_signal(df, params, vol_ratio, macro_trend_is_up, transaction_id, **kwargs):
    """
    Estrategia de Microestructura (Sweep)

    Trigger:
    (price > resistance_level) &
    (quote_cancel_ratio > 6) &
    (cvd_tick >= CVD_THRESHOLD[0]) &
    (spread_bp < MAX_SPREAD_BP[4])

    NOTA: Esta estrategia requiere data streams (CVD, Orderbook)
    que no están en la firma estándar de get_signal.
    Se asume que los datos vendrán en 'kwargs'.
    """
    symbol = df.attrs.get('symbol', 'N/A')
    log_extra = {'transaction_id': transaction_id, 'symbol': symbol}

    try:
        # --- 1. Cargar Parámetros de Estrategia ---
        strat_params = params.get("parameters", {})
        cancel_ratio_thresh = Decimal(str(strat_params.get("QUOTE_CANCEL_RATIO_THRESHOLD", 6)))
        cvd_ticks = int(str(strat_params.get("CVD_CONFIRMATION_TICKS", 1)))

        # --- INICIO DE MODIFICACIÓN (Fase 2 - Ajustes) ---
        # Leer thresholds relajados (CVD >= 0, Spread < 4)
        max_spread_bp = Decimal(str(strat_params.get("MAX_SPREAD_BP", 4)))
        cvd_thresh = Decimal(str(strat_params.get("CVD_THRESHOLD", 0)))
        # --- FIN DE MODIFICACIÓN ---

        # --- 2. Cargar Parámetros Específicos del Símbolo ---
        # (Esto es necesario para niveles de Resistencia/Soporte)
        symbol_settings = config.get_param(f"SYMBOL_SETTINGS.{symbol}.MicrostructureSweep", return_dict_for_complex=True)
        if not symbol_settings:
            logger.debug(f"MicrostructureSweep: No hay SYMBOL_SETTINGS para {symbol}. Saltando.", extra=log_extra)
            return None

        resistance_levels = [Decimal(str(lvl)) for lvl in symbol_settings.get("RESISTANCE_LEVELS", [])]
        if not resistance_levels:
            logger.debug(f"MicrostructureSweep: No hay RESISTANCE_LEVELS configurados para {symbol}.", extra=log_extra)
            return None

        # (Se asume el primer nivel de resistencia como el gatillo principal)
        resistance_level = resistance_levels[0]

        # --- 3. Obtener Datos de Microestructura (Placeholders) ---
        # Estos datos DEBEN ser inyectados en el 'strategy_hub'
        microstructure_data = kwargs.get('microstructure_data', {})

        price = Decimal(str(df['close'].iloc[-1]))
        quote_cancel_ratio = Decimal(str(microstructure_data.get('quote_cancel_ratio', 0)))
        cvd_tick = Decimal(str(microstructure_data.get('cvd_tick', 0)))
        spread_bp = Decimal(str(microstructure_data.get('spread_bp', 100)))

        # --- 4. Evaluar Condiciones de Trigger ---

        # Condición 1: Precio rompe la resistencia
        cond_1_price_break = (price > resistance_level)

        # Condición 2: Alto ratio de cancelación (spoofing)
        cond_2_cancel_ratio = (quote_cancel_ratio > cancel_ratio_thresh)

        # --- INICIO DE MODIFICACIÓN (Fase 2 - Ajustes) ---
        # Condición 3: CVD (Delta) positivo (CVD >= Threshold)
        cond_3_cvd_positive = (cvd_tick >= cvd_thresh)
        # --- FIN DE MODIFICACIÓN ---

        # Condición 4: Spread bajo
        cond_4_spread_low = (spread_bp < max_spread_bp)

        # Trigger exacto
        conditions = (
            cond_1_price_break and
            cond_2_cancel_ratio and
            cond_3_cvd_positive and
            cond_4_spread_low
        )

        if conditions:
            logger.info(f"🔥 MicrostructureSweep señal LONG detectada para {symbol}.", extra=log_extra)

            # (Lógica para SL/TP basado en config)
            support_levels = [Decimal(str(lvl)) for lvl in symbol_settings.get("SUPPORT_LEVELS", [])]
            air_pocket_targets = [Decimal(str(lvl)) for lvl in symbol_settings.get("AIR_POCKET_TARGETS", [])]

            signal = {
                "type": "LONG",
                "strategy_name": "MicrostructureSweep",

                # Instrucción: "position_size = 0.7× normal"
                # Usamos el 'ALLOCATION_WEIGHT' para esto, que debe estar en PORTFOLIO_ALLOCATION
                "position_size_multiplier": Decimal(str(strat_params.get("POSITION_SIZE_MULTIPLIER", 0.7))),

                # Instrucción: "Stop-loss: below micro-support + 1 tick"
                "stop_loss_price": (support_levels[0] - Decimal('0.01')) if support_levels else None,

                # Instrucción: "Take-profit: air-pocket mid"
                "take_profit_price": air_pocket_targets[0] if air_pocket_targets else None,

                # Instrucción: "max_holding = 4min"
                "max_holding_minutes": int(strat_params.get("MAX_HOLDING_MINUTES", 4))
            }

            return signal

        else:
            return None

    except Exception as e:
        logger.error(f"Error en MicrostructureSweep para {symbol}: {e}", exc_info=True, extra=log_extra)
        return None
