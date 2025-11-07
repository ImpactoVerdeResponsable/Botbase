# exceptions.py

class InitializationError(Exception):
    def __init__(self, message, symbol=None):
        super().__init__(message)
        self.message = message
        self.symbol = symbol

class InsufficientDataError(Exception):
    def __init__(self, message, symbol=None):
        super().__init__(message)
        self.message = message
        self.symbol = symbol

class FiltersNotFoundError(Exception):
    def __init__(self, message, symbol=None):
        super().__init__(message)
        self.message = message
        self.symbol = symbol

class OrderPlacementError(Exception):
    def __init__(self, message, symbol=None, quantity=None, formatted_quantity=None, params=None):
        super().__init__(message)
        self.message = message
        self.symbol = symbol
        self.quantity = quantity
        self.formatted_quantity = formatted_quantity
        self.params = params

class ConfigError(Exception):
    def __init__(self, message, symbol=None, errors_list=None):
        super().__init__(message)
        self.message = message
        self.symbol = symbol
        self.errors_list = errors_list if errors_list is not None else []

class CriticalBinanceError(Exception):
    def __init__(self, message, symbol=None):
        super().__init__(message)
        self.message = message
        self.symbol = symbol
        
# --- INICIO DE MODIFICACIÓN: (Corrección de ImportError) ---
class ConnectionError(Exception):
    def __init__(self, message="Fallo de conexión de red general o con la API.", symbol=None):
        super().__init__(message)
        self.message = message
        self.symbol = symbol
# --- FIN DE MODIFICACIÓN ---