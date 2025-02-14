import pytest
from unittest.mock import Mock, AsyncMock, patch, call
import asyncio
import logging
from app.tcp_client import TCPClient, ClientConfig, ClientState

# filepath: /D:/Project/cobalt-forward-1/app/test_tcp_client.py


@pytest.fixture
def config():
    return ClientConfig(
        host="localhost",
        port=8080,
        timeout=1.0,
        retry_attempts=2,
        retry_delay=0.1
    )


@pytest.fixture
def mock_stream_reader():
    return AsyncMock(spec=asyncio.StreamReader)


@pytest.fixture
def mock_stream_writer():
    writer = AsyncMock(spec=asyncio.StreamWriter)
    writer.wait_closed = AsyncMock()
    return writer


@pytest.fixture
async def tcp_client(config):
    client = TCPClient(config)
    yield client
    # Cleanup
    if client.state == ClientState.CONNECTED:
        await client.disconnect()


@pytest.mark.asyncio
async def test_initial_state(tcp_client):
    assert tcp_client.state == ClientState.DISCONNECTED
    assert tcp_client._reader is None
    assert tcp_client._writer is None


@pytest.mark.asyncio
async def test_successful_connection(tcp_client, mock_stream_reader, mock_stream_writer):
    with patch('asyncio.open_connection',
               AsyncMock(return_value=(mock_stream_reader, mock_stream_writer))):
        await tcp_client.connect()

        assert tcp_client.state == ClientState.CONNECTED
        assert tcp_client._reader == mock_stream_reader
        assert tcp_client._writer == mock_stream_writer


@pytest.mark.asyncio
async def test_connection_retry(tcp_client):
    connection_error = ConnectionError("Test connection error")

    with patch('asyncio.open_connection', AsyncMock(side_effect=[
        connection_error,
        (AsyncMock(spec=asyncio.StreamReader),
         AsyncMock(spec=asyncio.StreamWriter))
    ])):
        await tcp_client.connect()

        assert tcp_client.state == ClientState.CONNECTED


@pytest.mark.asyncio
async def test_connection_failure_after_retries(tcp_client):
    with patch('asyncio.open_connection',
               AsyncMock(side_effect=ConnectionError("Test connection error"))):
        with pytest.raises(ConnectionError):
            await tcp_client.connect()

        assert tcp_client.state == ClientState.ERROR


@pytest.mark.asyncio
async def test_disconnect(tcp_client, mock_stream_reader, mock_stream_writer):
    with patch('asyncio.open_connection',
               AsyncMock(return_value=(mock_stream_reader, mock_stream_writer))):
        await tcp_client.connect()
        await tcp_client.disconnect()

        assert tcp_client.state == ClientState.DISCONNECTED
        assert tcp_client._reader is None
        assert tcp_client._writer is None
        assert mock_stream_writer.close.called
        assert mock_stream_writer.wait_closed.called


@pytest.mark.asyncio
async def test_send_data(tcp_client, mock_stream_reader, mock_stream_writer):
    with patch('asyncio.open_connection',
               AsyncMock(return_value=(mock_stream_reader, mock_stream_writer))):
        await tcp_client.connect()

        test_data = b"test data"
        await tcp_client.send(test_data)

        mock_stream_writer.write.assert_called_once_with(test_data)
        assert mock_stream_writer.drain.called


@pytest.mark.asyncio
async def test_receive_data(tcp_client, mock_stream_reader, mock_stream_writer):
    with patch('asyncio.open_connection',
               AsyncMock(return_value=(mock_stream_reader, mock_stream_writer))):
        await tcp_client.connect()

        test_data = b"test data"
        mock_stream_reader.read.return_value = test_data

        received_data = await tcp_client.receive()
        assert received_data == test_data


@pytest.mark.asyncio
async def test_send_when_disconnected(tcp_client):
    with pytest.raises(ConnectionError):
        await tcp_client.send(b"test data")


@pytest.mark.asyncio
async def test_receive_when_disconnected(tcp_client):
    with pytest.raises(ConnectionError):
        await tcp_client.receive()


def test_callback_registration(tcp_client):
    callback = Mock()
    tcp_client.on('connected', callback)
    assert callback in tcp_client._callbacks['connected']


def test_invalid_callback_event(tcp_client):
    callback = Mock()
    tcp_client.on('invalid_event', callback)
    assert 'invalid_event' not in tcp_client._callbacks


@pytest.mark.asyncio
async def test_callback_triggering(tcp_client, mock_stream_reader, mock_stream_writer):
    callback = Mock()
    tcp_client.on('connected', callback)

    with patch('asyncio.open_connection',
               AsyncMock(return_value=(mock_stream_reader, mock_stream_writer))):
        await tcp_client.connect()
        assert callback.called


@pytest.mark.asyncio
async def test_error_callback(tcp_client):
    error_callback = Mock()
    tcp_client.on('error', error_callback)

    with patch('asyncio.open_connection',
               AsyncMock(side_effect=ConnectionError("Test error"))), \
            pytest.raises(ConnectionError):
        await tcp_client.connect()

        assert error_callback.called


@pytest.mark.asyncio
async def test_connection_timeout(tcp_client):
    with patch('asyncio.open_connection',
               AsyncMock(side_effect=asyncio.TimeoutError())), \
            pytest.raises(ConnectionError):
        await tcp_client.connect()

        assert tcp_client.state == ClientState.ERROR


@pytest.mark.asyncio
async def test_send_error_handling(tcp_client, mock_stream_reader, mock_stream_writer):
    mock_stream_writer.drain.side_effect = ConnectionError("Send error")

    with patch('asyncio.open_connection',
               AsyncMock(return_value=(mock_stream_reader, mock_stream_writer))):
        await tcp_client.connect()

        with pytest.raises(ConnectionError):
            await tcp_client.send(b"test data")

        assert tcp_client.state == ClientState.ERROR


@pytest.mark.asyncio
async def test_receive_error_handling(tcp_client, mock_stream_reader, mock_stream_writer):
    mock_stream_reader.read.side_effect = ConnectionError("Receive error")

    with patch('asyncio.open_connection',
               AsyncMock(return_value=(mock_stream_reader, mock_stream_writer))):
        await tcp_client.connect()

        with pytest.raises(ConnectionError):
            await tcp_client.receive()

        assert tcp_client.state == ClientState.ERROR


@pytest.mark.asyncio
async def test_callback_error_handling(tcp_client, caplog):
    def failing_callback(*args):
        raise Exception("Callback error")

    tcp_client.on('connected', failing_callback)

    with patch('asyncio.open_connection',
               AsyncMock(return_value=(AsyncMock(), AsyncMock()))):
        with caplog.at_level(logging.ERROR):
            await tcp_client.connect()

        assert "Callback error for event connected" in caplog.text
