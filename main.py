from fastapi import FastAPI, Header, HTTPException, Request 
import hashlib
from models import LLMTrace
from dependencies import redis_client

app = FastAPI()


@app.post("/ingest/trace")
async def ingest_trace(trace: LLMTrace, x_idempotency_key: str | None = Header(default=None)):

    if not x_idempotency_key:
        payload_string = trace.model_dump_json()
        x_idempotency_key = hashlib.sha256(payload_string.encode()).hexdigest()

        lock_key = f"idempotency:trace:{x_idempotency_key}"

        is_new_event = redis_client.set(lock_key, "locked", nx=True, ex=86400)

        if not is_new_event:
                return {"status": "dropped", "reason": "duplicate_event"}

        stream_name = "incoming_llm_traces"

        trace_data = trace.model_dump()

        try:
            redis_client.xadd(
                name=stream_name,
                fields=trace_data,
                maxlen=10000,
                approximate=True 
            )
        except Exception as e:
            raise HTTPException(status_code=500, detail="Message broker insertion failed")
        
        return {"status": "ingested", "idempotency_key": x_idempotency_key}


@app.get("/traces/{user_id}")
async def get_user_traces(user_id: str):
    try:
        stream_data = redis_client.xrange("incoming_llm_traces", min="-", max="+")
    except Exception:
        stream_data = []

    if not stream_data:
        return []

    user_traces = []
    for _, fields in stream_data:
        if fields.get("user_id") == user_id:
            user_traces.append(fields)

    return user_traces
