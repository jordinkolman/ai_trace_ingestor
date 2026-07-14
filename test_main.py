from fastapi.testclient import TestClient
from unittest.mock import patch, MagicMock
from main import app

client = TestClient(app)

def test_ingest_trace_success():
    # Mock redis_client.set to return True (indicating a new event)
    # and mock redis_client.xadd to succeed.
    with patch('main.redis_client') as mock_redis:
        mock_redis.set.return_value = True
        mock_redis.xadd.return_value = "12345-0"

        payload = {
            "user_id": "user_123",
            "model_name": "gpt-4",
            "input_tokens": 100,
            "output_tokens": 50,
            "latency_ms": 250
        }

        response = client.post("/ingest/trace", json=payload)

        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "ingested"
        assert "idempotency_key" in data
        
        # Verify redis calls
        mock_redis.set.assert_called_once()
        mock_redis.xadd.assert_called_once_with(
            name="incoming_llm_traces",
            fields=payload,
            maxlen=10000,
            approximate=True
        )


def test_ingest_trace_duplicate():
    # Mock redis_client.set to return None/False (indicating a duplicate event)
    with patch('main.redis_client') as mock_redis:
        mock_redis.set.return_value = None

        payload = {
            "user_id": "user_123",
            "model_name": "gpt-4",
            "input_tokens": 100,
            "output_tokens": 50,
            "latency_ms": 250
        }

        response = client.post("/ingest/trace", json=payload)

        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "dropped"
        assert data["reason"] == "duplicate_event"
        
        # Verify redis set was called, but xadd was not called
        mock_redis.set.assert_called_once()
        mock_redis.xadd.assert_not_called()


def test_ingest_trace_redis_failure():
    # Mock redis_client.set to return True, but xadd to raise an exception
    with patch('main.redis_client') as mock_redis:
        mock_redis.set.return_value = True
        mock_redis.xadd.side_effect = Exception("Redis connection lost")

        payload = {
            "user_id": "user_123",
            "model_name": "gpt-4",
            "input_tokens": 100,
            "output_tokens": 50,
            "latency_ms": 250
        }

        response = client.post("/ingest/trace", json=payload)

        assert response.status_code == 500
        assert response.json()["detail"] == "Message broker insertion failed"
