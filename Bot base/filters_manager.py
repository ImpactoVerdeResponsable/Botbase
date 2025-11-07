# --- INICIO DE MODIFICACIÓN: (Paso 3) ---
from unified_logger import get_logger
# --- FIN DE MODIFICACIÓN ---
from typing import List, Set, Dict
import binance_interaction as bi
from exceptions import FiltersNotFoundError

# --- INICIO DE MODIFICACIÓN: (Paso 3) ---
logger = get_logger(__name__)
# --- FIN DE MODIFICACIÓN ---

class FiltersManager:
    def __init__(self):
        self.loaded_symbols: Set[str] = set()
    
    def ensure_filters_loaded(self, symbols: List[str], transaction_id: str) -> bool:
        """
        Garantiza que los filtros estén cargados para los símbolos especificados
        
        Args:
            symbols: Lista de símbolos a verificar
            transaction_id: ID para logging
            
        Returns:
            bool: True si todos los filtros están cargados correctamente
        """
        log_extra = {'transaction_id': transaction_id, 'symbol': 'FILTERS_MGR'}
        
        symbols_to_load = []
        
        for symbol in symbols:
            if symbol not in self.loaded_symbols:
                try:
                    # Verificar si el filtro existe en caché
                    bi.get_symbol_filters(symbol, transaction_id=f"{transaction_id}_Check")
                    self.loaded_symbols.add(symbol)
                except FiltersNotFoundError:
                    symbols_to_load.append(symbol)
        
        if symbols_to_load:
            logger.info(f"📥 Cargando filtros para {len(symbols_to_load)} símbolos: {symbols_to_load}", extra=log_extra)
            try:
                bi.load_exchange_filters(symbols_to_load, transaction_id=f"{transaction_id}_Load")
                self.loaded_symbols.update(symbols_to_load)
                logger.info(f"✅ Filtros cargados exitosamente", extra=log_extra)
            except Exception as e:
                logger.error(f"❌ Error cargando filtros: {e}", extra=log_extra)
                return False
        
        return True
    
    def get_loaded_count(self) -> int:
        """Retorna cantidad de símbolos con filtros cargados"""
        return len(self.loaded_symbols)
    
    def clear_cache(self):
        """Limpia la caché de símbolos cargados"""
        self.loaded_symbols.clear()

# Instancia global
filters_manager = FiltersManager()