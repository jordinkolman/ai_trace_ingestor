import pytest
from unittest.mock import MagicMock, patch
from consumer import run_consumer, process_message

def test_process_message_success():
    mock_redis = MagicMock()
    mock_tracer = MagicMock()
    mock_span = MagicMock()
    mock_tracer.start_as_current_span.return_value.__enter__.return_value = mock_span

    fields = {
        "user_id": "user_123",
        "model_name": "gpt-4",
        "input_tokens": "100",
        "output_tokens": "50",
        "latency_ms": "250"
    }

    process_message(mock_redis, mock_tracer, "12345-0", fields)

    mock_tracer.start_as_current_span.assert_called_once_with("llm_trace")
    mock_span.set_attribute.assert_any_call("user_id", "user_123")
    mock_span.set_attribute.assert_any_call("model_name", "gpt-4")
    mock_span.set_attribute.assert_any_call("input_tokens", "100")

    mock_redis.publish.assert_called_once()


def test_run_consumer_success():
    mock_redis = MagicMock()
    mock_tracer = MagicMock()
    
    # Mock xread to return one batch of messages, then empty
    mock_redis.xread.side_effect = [
        [("incoming_llm_traces", [
            ("12345-0", {"user_id": "user_123", "model_name": "gpt-4"})
        ])],
        []
    ]

    # Run consumer with a limit of 1 processed message to prevent infinite loop
    run_consumer(mock_redis, mock_tracer, limit=1)

    assert mock_redis.xread.call_count >= 1
    mock_tracer.start_as_current_span.assert_called_once_with("llm_trace")


def test_run_consumer_redis_failure():
    mock_redis = MagicMock()
    mock_tracer = MagicMock()
    
    # Mock redis to raise an exception
    mock_redis.xread.side_effect = Exception("Redis connection lost")

    with pytest.raises(Exception, match="Redis connection lost"):
        run_consumer(mock_redis, mock_tracer, limit=1)
