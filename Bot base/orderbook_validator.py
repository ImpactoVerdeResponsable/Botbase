from unified_logger import get_logger
from decimal import Decimal, InvalidOperation
from typing import Dict, Any

logger = get_logger(__name__)

class OrderbookValidator:
    """Validador robusto para datos de orderbook de Binance"""
    
    @staticmethod
    def validate_orderbook_structure(orderbook_data: dict, symbol: str) -> bool:
        """
        Valida estructura básica del orderbook
        """
        required_keys = {'bids', 'asks', 'lastUpdateId'}
        log_extra = {'symbol': symbol, 'transaction_id': 'OB_VALIDATE'}
        
        if not orderbook_data or not isinstance(orderbook_data, dict):
            logger.warning(f"Orderbook vacío o inválido para {symbol}", extra=log_extra)
            return False
            
        if not all(key in orderbook_data for key in required_keys):
            logger.warning(f"Orderbook incompleto para {symbol}: {orderbook_data.keys()}", extra=log_extra)
            return False
            
        # Validar estructura de bids/asks
        if not OrderbookValidator._validate_price_levels(orderbook_data['bids'], 'bids', symbol):
            return False
            
        if not OrderbookValidator._validate_price_levels(orderbook_data['asks'], 'asks', symbol):
            return False
            
        return True
    
    @staticmethod
    def _validate_price_levels(levels: list, side: str, symbol: str) -> bool:
        """Valida niveles de precio individuales"""
        log_extra = {'symbol': symbol, 'transaction_id': 'OB_VALIDATE_LEVELS'}

        if not isinstance(levels, list):
            logger.warning(f"Levels no es lista para {symbol} {side}: {type(levels)}", extra=log_extra)
            return False
            
        if len(levels) == 0:
            logger.debug(f"Orderbook vacío para {symbol} {side}", extra=log_extra)
            return True  # Orderbook vacío es válido pero puede indicar problemas
            
        for i, level in enumerate(levels):
            if not isinstance(level, list) or len(level) < 2:
                logger.warning(f"Nivel {i} inválido en {symbol} {side}: {level}", extra=log_extra)
                return False
                
            try:
                price = Decimal(level[0])
                quantity = Decimal(level[1])
                
                if price <= 0 or quantity <= 0:
                    logger.warning(f"Precio o cantidad inválidos en {symbol} {side}[{i}]: {price}, {quantity}", extra=log_extra)
                    return False
                    
            except (InvalidOperation, TypeError, ValueError) as e:
                logger.warning(f"Error parseando nivel {i} en {symbol} {side}: {e}", extra=log_extra)
                return False
                
        return True