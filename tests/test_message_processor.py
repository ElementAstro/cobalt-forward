import pytest
from unittest.mock import Mock
import json
from app.message_processor import MessageTransformer, MessageFilter, json_payload_transformer, compress_payload_transformer
from app.message_bus import Message, MessageType

# filepath: /d:/Project/cobalt-forward-1/app/test_message_processor.py


@pytest.fixture
def message_transformer():
    return MessageTransformer()


@pytest.fixture
def sample_message():
    return Message(
        type=MessageType.EVENT,
        topic="test.topic",
        data={"key": "value"},
        metadata={}
    )


def test_message_filter_init():
    filter_ = MessageFilter(topic_pattern=r"test\.*")
    assert filter_.topic_pattern == r"test\.*"
    assert filter_.condition is None


def test_message_filter_matches():
    filter_ = MessageFilter(topic_pattern=r"test\.*")
    message = Mock(topic="test.topic")
    assert filter_.matches(message) is True

    message.topic = "other.topic"
    assert filter_.matches(message) is False


def test_message_filter_with_condition():
    def condition(msg): return msg.data.get("key") == "value"
    filter_ = MessageFilter(topic_pattern=r"test\.*", condition=condition)
    message = Mock(topic="test.topic", data={"key": "value"})
    assert filter_.matches(message) is True

    message.data = {"key": "wrong"}
    assert filter_.matches(message) is False


def test_transformer_init(message_transformer):
    assert isinstance(message_transformer.filters, list)
    assert isinstance(message_transformer.transformers, list)
    assert len(message_transformer.filters) == 0
    assert len(message_transformer.transformers) == 0


def test_add_filter(message_transformer):
    filter_ = MessageFilter(topic_pattern=r"test\.*")
    message_transformer.add_filter(filter_)
    assert len(message_transformer.filters) == 1
    assert message_transformer.filters[0] == filter_


def test_add_transformer(message_transformer):
    def transformer(x): return x
    message_transformer.add_transformer(transformer)
    assert len(message_transformer.transformers) == 1
    assert message_transformer.transformers[0] == transformer


def test_process_message_no_filters(message_transformer, sample_message):
    result = message_transformer.process(sample_message)
    assert result == sample_message


def test_process_message_with_filter(message_transformer, sample_message):
    filter_ = MessageFilter(topic_pattern=r"test\.*")
    message_transformer.add_filter(filter_)
    result = message_transformer.process(sample_message)
    assert result == sample_message

    sample_message.topic = "other.topic"
    result = message_transformer.process(sample_message)
    assert result is None


def test_process_message_with_transformer(message_transformer, sample_message):
    def upper_case_transformer(msg):
        msg.data = {k.upper(): v.upper() if isinstance(v, str) else v
                    for k, v in msg.data.items()}
        return msg

    message_transformer.add_transformer(upper_case_transformer)
    result = message_transformer.process(sample_message)
    assert result.data == {"KEY": "value"}


def test_json_payload_transformer():
    message = Message(
        type=MessageType.EVENT,
        topic="test",
        data={"key": "value"},
        metadata={}
    )
    result = json_payload_transformer(message)
    assert isinstance(result.data, str)
    assert json.loads(result.data) == {"key": "value"}


def test_compress_payload_transformer():
    long_data = "a" * 1001
    message = Message(
        type=MessageType.EVENT,
        topic="test",
        data=long_data,
        metadata={}
    )
    result = compress_payload_transformer(message)
    assert result.metadata.get("compressed") is True


def test_multiple_filters(message_transformer, sample_message):
    filter1 = MessageFilter(topic_pattern=r"test\.*")
    filter2 = MessageFilter(topic_pattern=r".*\.topic")
    message_transformer.add_filter(filter1)
    message_transformer.add_filter(filter2)
    result = message_transformer.process(sample_message)
    assert result == sample_message

    sample_message.topic = "other.topic"
    result = message_transformer.process(sample_message)
    assert result is None


def test_multiple_transformers(message_transformer, sample_message):
    def transformer1(msg):
        msg.data["t1"] = True
        return msg

    def transformer2(msg):
        msg.data["t2"] = True
        return msg

    message_transformer.add_transformer(transformer1)
    message_transformer.add_transformer(transformer2)
    result = message_transformer.process(sample_message)
    assert result.data.get("t1") is True
    assert result.data.get("t2") is True


def test_process_message_with_invalid_filter():
    transformer = MessageTransformer()
    filter_ = MessageFilter(topic_pattern="(invalid")  # Invalid regex pattern
    transformer.add_filter(filter_)
    message = Mock(topic="test.topic")
    result = transformer.process(message)
    assert result is None


def test_process_message_with_failing_transformer(message_transformer, sample_message):
    def failing_transformer(msg):
        raise ValueError("Test error")

    message_transformer.add_transformer(failing_transformer)
    with pytest.raises(ValueError):
        message_transformer.process(sample_message)
