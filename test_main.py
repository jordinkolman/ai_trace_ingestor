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


def test_get_traces_success():
    # Mock redis_client.xrange to return a list of stream messages
    with patch('main.redis_client') as mock_redis:
        mock_redis.xrange.return_value = [
            ("1526543278123-0", {
                "user_id": "user_123",
                "model_name": "gpt-4",
                "input_tokens": "100",
                "output_tokens": "50",
                "latency_ms": "250"
            }),
            ("1526543278124-0", {
                "user_id": "user_456",
                "model_name": "claude-3",
                "input_tokens": "80",
                "output_tokens": "40",
                "latency_ms": "180"
            }),
            ("1526543278125-0", {
                "user_id": "user_123",
                "model_name": "gpt-3.5-turbo",
                "input_tokens": "30",
                "output_tokens": "10",
                "latency_ms": "90"
            })
        ]

        response = client.get("/traces/user_123")

        assert response.status_code == 200
        data = response.json()
        assert len(data) == 2
        assert data[0]["model_name"] == "gpt-4"
        assert data[1]["model_name"] == "gpt-3.5-turbo"
        mock_redis.xrange.assert_called_once_with("incoming_llm_traces", min="-", max="+")


def test_get_traces_empty_or_not_found():
    # Mock redis_client.xrange to return an empty list
    with patch('main.redis_client') as mock_redis:
        mock_redis.xrange.return_value = []

        response = client.get("/traces/user_nonexistent")

        assert response.status_code == 200
        data = response.json()
        assert data == []
        mock_redis.xrange.assert_called_once_with("incoming_llm_traces", min="-", max="+")
