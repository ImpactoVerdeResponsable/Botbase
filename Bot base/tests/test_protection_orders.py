# tests/test_protection_orders.py
import asyncio
import pytest
from decimal import Decimal
import protection_manager
import binance_interaction as bi
from unified_logger import get_logger
from unittest.mock import MagicMock, AsyncMock # Usar Mocks asíncronos

logger = get_logger(__name__)

# --- INICIO MODIFICACIÓN: Crear Mocks compatibles con Async ---
# (El DummyClient provisto no es 'awaitable' correctamente por monkeypatch)

class MockAsyncClient:
    """Mock del AsyncClient para simular respuestas awaitable."""
    
    async def create_order(self, *args, **kwargs):
        logger.info("MockAsyncClient.create_order (ENTRY) llamado")
        return {'executedQty': '0.01', 'orderId': 'entry123', 'transactTime': 1600000000000, 'avgPrice': '100.0', 'cummulativeQuoteQty': '1.0', 'status': 'FILLED'}

    async def order_oco(self, *args, **kwargs):
        logger.info("MockAsyncClient.order_oco (SPOT OCO) llamado")
        # Simular la respuesta de la librería python-binance
        return {
            'orderListId': 9999, 
            'orders': [
                {'symbol': 'BTCUSDT', 'orderId': 'oco1_tp', 'clientOrderId': '...'},
                {'symbol': 'BTCUSDT', 'orderId': 'oco2_sl', 'clientOrderId': '...'}
            ],
            'orderReports': [
                {'symbol': 'BTCUSDT', 'orderId': 'oco1_tp', 'type': 'LIMIT_MAKER', 'status': 'NEW', 'price': '110.0', 'origQty': '0.01'},
                {'symbol': 'BTCUSDT', 'orderId': 'oco2_sl', 'type': 'STOP_LOSS_LIMIT', 'status': 'NEW', 'stopPrice': '99.0', 'origQty': '0.01'}
            ]
        }

    async def futures_create_order(self, *args, **kwargs):
        type_ = kwargs.get('type', 'UNKNOWN')
        logger.info(f"MockAsyncClient.futures_create_order ({type_}) llamado")
        if type_ == 'TAKE_PROFIT_MARKET':
            return {'orderId': 'fut_tp_1', 'status': 'NEW'}
        elif type_ == 'STOP_MARKET':
            return {'orderId': 'fut_sl_1', 'status': 'NEW'}
        # Fallback para la orden de entrada de futuros
        return {'executedQty': '0.01', 'orderId': 'entry_fut_123', 'transactTime': 1600000000000, 'avgPrice': '100.0', 'qty': '0.01', 'status': 'FILLED'}

# Mock para app_state_manager (requerido por protection_manager)
@pytest.fixture(autouse=True)
def mock_app_state(monkeypatch):
    """Mockea el app_state_manager global."""
    mock_state = {
        "active_positions": {},
        "protections": {},
        "protections_by_symbol": {}
    }

    class MockAppState:
        def __init__(self):
            self.state = mock_state
        
        def get_active_position(self, symbol):
            return self.state["active_positions"].get(symbol)
        
        def open_new_position(self, symbol, data):
            self.state["active_positions"][symbol] = data
            
        def save(self, transaction_id):
            logger.debug(f"MockAppSate.save() llamado (tx: {transaction_id})")
            pass # No hacer nada

    mock_manager_instance = MockAppState()
    
    # Mockear el getter que usa protection_manager
    monkeypatch.setattr("position_management.get_app_state_manager", lambda: mock_manager_instance)
    return mock_manager_instance

# Mock para binance_interaction (formateo)
@pytest.fixture(autouse=True)
def mock_bi_formatters(monkeypatch):
    monkeypatch.setattr(bi, "format_quantity", lambda symbol, qty: qty)
    monkeypatch.setattr(bi, "format_price", lambda symbol, price: price)
# --- FIN MODIFICACIÓN ---


@pytest.mark.asyncio
async def test_place_entry_with_exchange_protection_spot(monkeypatch, mock_app_state):
    """
    Test High (0-3h) (Spot OCO):
    Verifica que una orden de entrada SPOT llame a la creación de una orden OCO
    y guarde el estado correctamente.
    """
    client = MockAsyncClient() # Usar el Mock awaitable
    
    # Mockear las funciones de bi que llama protection_manager
    # Usar AsyncMock para funciones que deben ser 'await'
    monkeypatch.setattr(bi, "async_create_order", AsyncMock(side_effect=client.create_order))
    monkeypatch.setattr(bi, "async_create_oco_order", AsyncMock(side_effect=client.order_oco))

    # --- Ejecutar ---
    prot_meta = await protection_manager.place_entry_with_exchange_protection(
        client=client, 
        symbol='BTCUSDT', 
        side='BUY', 
        requested_quantity=Decimal('0.01'), 
        sl_price=Decimal('99.0'), 
        tp_price=Decimal('110.0'), 
        transaction_id='T1_SPOT', 
        market_type='SPOT'
    )
    
    # --- Verificar ---
    assert prot_meta is not None
    assert prot_meta.get('status') == 'ACTIVE'
    assert prot_meta.get('market_type') == 'SPOT'
    assert prot_meta.get('protection_group_id') == 9999 # OCO ID
    assert len(prot_meta.get('protection_orders', [])) == 2 # Dos IDs de orden (TP y SL)
    
    # Verificar que el estado se guardó
    entry_id = prot_meta.get('entry_order_id')
    assert entry_id == 'entry123'
    assert mock_app_state.state['protections'].get(entry_id) is not None
    assert mock_app_state.state['protections'][entry_id]['status'] == 'ACTIVE'


@pytest.mark.asyncio
async def test_place_entry_with_exchange_protection_futures(monkeypatch, mock_app_state):
    """
    Test High (0-3h) (Futures TP/SL):
    Verifica que una orden de entrada FUTURES llame a la creación de 
    dos órdenes separadas (TAKE_PROFIT_MARKET y STOP_MARKET).
    """
    client = MockAsyncClient() # Usar el Mock awaitable

    # Mockear las funciones de bi que llama protection_manager
    monkeypatch.setattr(bi, "async_futures_create_order", AsyncMock(side_effect=client.futures_create_order))

    # --- Ejecutar ---
    prot_meta = await protection_manager.place_entry_with_exchange_protection(
        client=client, 
        symbol='BTCUSDT', 
        side='BUY', 
        requested_quantity=Decimal('0.01'), 
        sl_price=Decimal('99.0'), 
        tp_price=Decimal('110.0'), 
        transaction_id='T2_FUTURES', 
        market_type='FUTURES'
    )

    # --- Verificar ---
    assert prot_meta is not None
    assert prot_meta.get('status') == 'ACTIVE'
    assert prot_meta.get('market_type') == 'FUTURES'
    assert prot_meta.get('protection_group_id') is None # No OCO ID
    assert len(prot_meta.get('protection_orders', [])) == 2
    assert 'fut_tp_1' in prot_meta['protection_orders']
    assert 'fut_sl_1' in prot_meta['protection_orders']
    
    # Verificar estado
    entry_id = prot_meta.get('entry_order_id')
    assert entry_id == 'entry_fut_123'
    assert mock_app_state.state['protections'].get(entry_id) is not None
    assert mock_app_state.state['protections'][entry_id]['status'] == 'ACTIVE'


@pytest.mark.asyncio
async def test_protection_placement_failure_arms_watcher(monkeypatch, mock_app_state, caplog):
    """
    Test Medium (2-6h) (Fallback Watcher):
    Verifica que si la colocación de OCO falla (tras reintentos), 
    se lance ProtectionPlacementError y se arme el watcher de emergencia.
    """
    client = MockAsyncClient()
    
    # Simular fallo en la orden de entrada
    monkeypatch.setattr(bi, "async_create_order", AsyncMock(side_effect=client.create_order))
    # Simular que OCO falla 3 veces
    monkeypatch.setattr(bi, "async_create_oco_order", AsyncMock(side_effect=Exception("API Error -2010 OCO")))
    
    # Mockear el watcher para que no se ejecute realmente, solo registrar que se llamó
    mock_watcher = AsyncMock()
    monkeypatch.setattr(protection_manager, "_emergency_watcher_for_symbol", mock_watcher)
    
    # --- Ejecutar (Esperar la excepción) ---
    with pytest.raises(protection_manager.ProtectionPlacementError, match="API Error -2010 OCO"):
        await protection_manager.place_entry_with_exchange_protection(
            client=client, 
            symbol='BTCUSDT', 
            side='BUY', 
            requested_quantity=Decimal('0.01'), 
            sl_price=Decimal('99.0'), 
            tp_price=Decimal('110.0'), 
            transaction_id='T3_FAIL', 
            market_type='SPOT'
        )

    # --- Verificar ---
    # 1. Verificar que el estado se guardó como "FAILED"
    entry_id = 'entry123' # La orden de entrada SÍ se completó
    assert mock_app_state.state['protections'].get(entry_id) is not None
    assert mock_app_state.state['protections'][entry_id]['status'] == 'FAILED'
    assert "API Error -2010 OCO" in mock_app_state.state['protections'][entry_id]['failed_reason']

    # 2. Verificar que el watcher de emergencia fue llamado (armado)
    mock_watcher.assert_called_once()
    # Verificar args del watcher: (symbol, sl_price, entry_side, qty, tx_id, market_type)
    watcher_args = mock_watcher.call_args[0]
    assert watcher_args[0] == 'BTCUSDT'
    assert watcher_args[1] == Decimal('99.0') # sl_price
    assert watcher_args[2] == 'BUY' # entry_side
    assert watcher_args[3] == Decimal('0.01') # qty
    assert "EMERGENCY" in watcher_args[4] # transaction_id
    assert watcher_args[5] == 'SPOT' # market_type

    # 3. Verificar logs
    assert "Colocación de protección falló" in caplog.text
    assert "Armado de Watcher de Emergencia" in caplog.text