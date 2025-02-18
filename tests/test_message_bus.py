import pytest
import asyncio
from unittest.mock import Mock, AsyncMock, patch
import json
from typing import Dict, Any

# filepath: /D:/Project/cobalt-forward-1/app/test_message_bus.py

from app.core.message_bus import (
    MessageBus,
    MessageType,
    Message,
    JSONSerializer,
    TCPClient
)


@pytest.fixture
def tcp_client():
    client = AsyncMock(spec=TCPClient)
    client.send = AsyncMock()
    client.receive = AsyncMock()
    client.on = Mock()
    return client


@pytest.fixture
def message_bus(tcp_client):
    bus = MessageBus(tcp_client)
    return bus


@pytest.fixture
def test_message():
    return Message(
        id="test-id",
        type=MessageType.EVENT,
        topic="test-topic",
        data={"test": "data"},
        correlation_id=None,
        metadata={}
    )


@pytest.mark.asyncio
async def test_message_bus_initialization(tcp_client):
    bus = MessageBus(tcp_client)
    assert bus._tcp_client == tcp_client
    assert isinstance(bus._serializer, JSONSerializer)
    assert not bus._running
    assert bus._processing_task is None


@pytest.mark.asyncio
async def test_start_stop(message_bus):
    await message_bus.start()
    assert message_bus._running
    assert message_bus._processing_task is not None

    await message_bus.stop()
    assert not message_bus._running
    assert message_bus._processing_task is None


@pytest.mark.asyncio
async def test_publish_message(message_bus, tcp_client):
    data = {"test": "data"}
    topic = "test-topic"

    await message_bus.publish(topic, data)

    assert tcp_client.send.called
    sent_data = tcp_client.send.call_args[0][0]
    decoded_message = json.loads(sent_data.decode('utf-8'))

    assert decoded_message['topic'] == topic
    assert decoded_message['data'] == data
    assert decoded_message['type'] == MessageType.EVENT.value
    assert message_bus._message_count == 1


@pytest.mark.asyncio
async def test_request_response(message_bus, tcp_client):
    response_data = {"response": "data"}
    topic = "test-request"

    async def mock_send_and_respond(data):
        message = json.loads(data.decode('utf-8'))
        response = Message(
            id="response-id",
            type=MessageType.RESPONSE,
            topic=topic,
            data=response_data,
            correlation_id=message['correlation_id']
        )
        message_bus._response_futures[message['correlation_id']].set_result(
            response_data)

    tcp_client.send.side_effect = mock_send_and_respond

    result = await message_bus.request(topic, {"request": "data"})
    assert result == response_data


@pytest.mark.asyncio
async def test_subscribe_unsubscribe(message_bus):
    topic = "test-topic"

    queue = await message_bus.subscribe(topic)
    assert len(message_bus._subscribers[topic]) == 1
    assert isinstance(queue, asyncio.Queue)

    await message_bus.unsubscribe(topic, queue)
    assert topic not in message_bus._subscribers


@pytest.mark.asyncio
async def test_message_processing(message_bus, tcp_client, test_message):
    topic = "test-topic"
    queue = await message_bus.subscribe(topic)

    serialized_message = message_bus._serializer.serialize(test_message)
    tcp_client.receive.return_value = serialized_message

    await message_bus.start()
    # Allow time for message processing
    await asyncio.sleep(0.1)

    received_message = await queue.get()
    assert received_message.topic == test_message.topic
    assert received_message.data == test_message.data


@pytest.mark.asyncio
async def test_request_timeout(message_bus, tcp_client):
    topic = "test-timeout"

    with pytest.raises(asyncio.TimeoutError):
        await message_bus.request(topic, {"test": "data"}, timeout=0.1)


@pytest.mark.asyncio
async def test_metrics(message_bus):
    topic = "test-metrics"
    await message_bus.publish(topic, {"test": "data"})

    metrics = message_bus.metrics
    assert metrics['message_count'] == 1
    assert 'average_processing_time' in metrics
    assert metrics['active_subscribers'] == 0
    assert metrics['pending_responses'] == 0


@pytest.mark.asyncio
async def test_error_handling(message_bus, tcp_client):
    tcp_client.receive.side_effect = Exception("Test error")

    await message_bus.start()
    await asyncio.sleep(0.1)  # Allow error to be processed

    # Message bus should continue running despite error
    assert message_bus._running


@pytest.mark.asyncio
async def test_multiple_subscribers(message_bus):
    topic = "test-topic"
    queue1 = await message_bus.subscribe(topic)
    queue2 = await message_bus.subscribe(topic)

    test_data = {"test": "data"}
    await message_bus.publish(topic, test_data)

    msg1 = await queue1.get()
    msg2 = await queue2.get()

    assert msg1.data == test_data
    assert msg2.data == test_data


@pytest.mark.asyncio
async def test_serializer_custom(tcp_client):
    class CustomSerializer(JSONSerializer):
        def serialize(self, message: Message) -> bytes:
            return json.dumps({"custom": "format"}).encode('utf-8')

    bus = MessageBus(tcp_client, serializer=CustomSerializer())
    await bus.publish("topic", {})

    sent_data = tcp_client.send.call_args[0][0]
    assert json.loads(sent_data.decode('utf-8')) == {"custom": "format"}
