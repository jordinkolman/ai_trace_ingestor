from fastapi import FastAPI, Header, HTTPException, Request 
from pydantic import BaseModel
import redis 
import json 
import hashlib

app = FastAPI()

redis_client = redis.Redis(host='127.0.0.1', port=6379, db=0, decode_responses=True)


class LLMTrace(BaseModel):
    user_id: str
    model_name: str 
    input_tokens: int 
    output_tokens: int 
    latency_ms: int


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
