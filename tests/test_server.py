import pytest
from unittest.mock import AsyncMock, Mock, patch
import asyncio
import json
from fastapi import WebSocket
from app.server import EnhancedConnectionManager, MessageBus, CommandTransmitter, Message, MessageType

# filepath: /d:/Project/cobalt-forward-1/app/test_server.py


@pytest.fixture
def mock_message_bus():
    bus = AsyncMock(spec=MessageBus)
    bus.subscribe = AsyncMock(return_value=asyncio.Queue())
    bus.unsubscribe = AsyncMock()
    bus.publish = AsyncMock()
    return bus


@pytest.fixture
def mock_command_transmitter():
    transmitter = AsyncMock(spec=CommandTransmitter)
    transmitter.send = AsyncMock(return_value=Mock(
        status=Mock(value="success"),
        result={"data": "test"},
        error=None
    ))
    return transmitter


@pytest.fixture
def mock_websocket():
    socket = AsyncMock(spec=WebSocket)
    socket.accept = AsyncMock()
    socket.send_json = AsyncMock()
    socket.receive_text = AsyncMock()
    return socket


@pytest.fixture
def connection_manager(mock_message_bus, mock_command_transmitter):
    return EnhancedConnectionManager(mock_message_bus, mock_command_transmitter)


@pytest.mark.asyncio
async def test_connection_manager_init(connection_manager):
    assert connection_manager.active_connections == set()
    assert isinstance(connection_manager._subscription_queues, dict)


@pytest.mark.asyncio
async def test_connect(connection_manager, mock_websocket):
    await connection_manager.connect(mock_websocket)

    assert mock_websocket.accept.called
    assert mock_websocket in connection_manager.active_connections
    assert mock_websocket in connection_manager._subscription_queues
    assert connection_manager._message_bus.subscribe.called


@pytest.mark.asyncio
async def test_disconnect(connection_manager, mock_websocket):
    await connection_manager.connect(mock_websocket)
    await connection_manager.disconnect(mock_websocket)

    assert mock_websocket not in connection_manager.active_connections
    assert mock_websocket not in connection_manager._subscription_queues
    assert connection_manager._message_bus.unsubscribe.called


@pytest.mark.asyncio
async def test_forward_messages(connection_manager, mock_websocket):
    test_message = Message(type=MessageType.EVENT, data={"test": "data"})
    queue = asyncio.Queue()
    await queue.put(test_message)

    with patch.object(connection_manager, '_subscription_queues', {mock_websocket: queue}):
        # Start forwarding task
        task = asyncio.create_task(
            connection_manager._forward_messages(mock_websocket, queue))

        # Give time for message processing
        await asyncio.sleep(0.1)

        # Cancel task
        task.cancel()

        try:
            await task
        except asyncio.CancelledError:
            pass

        mock_websocket.send_json.assert_called_once_with({
            "type": test_message.type.value,
            "data": test_message.data
        })


@pytest.mark.asyncio
async def test_handle_client_message_command(connection_manager, mock_websocket):
    command_message = json.dumps({
        "type": "command",
        "payload": {"action": "test"}
    })

    await connection_manager.handle_client_message(mock_websocket, command_message)

    assert connection_manager._command_transmitter.send.called
    mock_websocket.send_json.assert_called_once()


@pytest.mark.asyncio
async def test_handle_client_message_event(connection_manager, mock_websocket):
    event_message = json.dumps({
        "type": "event",
        "data": "test"
    })

    await connection_manager.handle_client_message(mock_websocket, event_message)

    assert connection_manager._message_bus.publish.called


@pytest.mark.asyncio
async def test_handle_client_message_error(connection_manager, mock_websocket):
    invalid_message = "invalid json"

    await connection_manager.handle_client_message(mock_websocket, invalid_message)

    mock_websocket.send_json.assert_called_once()
    assert mock_websocket.send_json.call_args[0][0]["type"] == "error"


@pytest.mark.asyncio
async def test_forward_messages_error_handling(connection_manager, mock_websocket):
    mock_websocket.send_json.side_effect = Exception("Test error")
    queue = asyncio.Queue()
    await queue.put(Message(type=MessageType.EVENT, data={"test": "data"}))

    with patch.object(connection_manager, '_subscription_queues', {mock_websocket: queue}):
        await connection_manager._forward_messages(mock_websocket, queue)

    assert mock_websocket not in connection_manager.active_connections


@pytest.mark.asyncio
async def test_multiple_connections(connection_manager):
    websocket1 = AsyncMock(spec=WebSocket)
    websocket2 = AsyncMock(spec=WebSocket)

    await connection_manager.connect(websocket1)
    await connection_manager.connect(websocket2)

    assert len(connection_manager.active_connections) == 2
    assert len(connection_manager._subscription_queues) == 2

    await connection_manager.disconnect(websocket1)

    assert len(connection_manager.active_connections) == 1
    assert websocket2 in connection_manager.active_connections
