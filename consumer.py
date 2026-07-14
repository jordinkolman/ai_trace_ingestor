import json
import os
import time
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from dependencies import redis_client

def setup_tracer():
    provider = TracerProvider()
    endpoint = os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://localhost:4317")
    processor = BatchSpanProcessor(OTLPSpanExporter(endpoint=endpoint, insecure=True))
    provider.add_span_processor(processor)
    trace.set_tracer_provider(provider)
    return trace.get_tracer("redis-stream-consumer")

def process_message(redis_client, tracer, message_id, fields):
    user_id = fields.get("user_id", "unknown")
    model_name = fields.get("model_name", "unknown")
    
    with tracer.start_as_current_span("llm_trace") as span:
        span.set_attribute("user_id", user_id)
        span.set_attribute("model_name", model_name)
        for key, val in fields.items():
            if key not in ["user_id", "model_name"]:
                if isinstance(val, str) and val.isdigit():
                    span.set_attribute(key, int(val))
                else:
                    span.set_attribute(key, val)

        live_payload = {"message_id": message_id, **fields}
        redis_client.publish("live_traces", json.dumps(live_payload))

def run_consumer(redis_client, tracer, stream_name="incoming_llm_traces", limit=None):
    last_id = "0-0"
    processed_count = 0
    
    while True:
        try:
            events = redis_client.xread({stream_name: last_id}, count=10, block=1000)
            if not events:
                if limit is not None and processed_count >= limit:
                    break
                continue

            for stream, messages in events:
                for message_id, fields in messages:
                    process_message(redis_client, tracer, message_id, fields)
                    last_id = message_id
                    processed_count += 1
                    
            if limit is not None and processed_count >= limit:
                break
        except Exception as e:
            print(f"Error processing stream: {e}")
            time.sleep(1)
            if limit is not None:
                raise e

if __name__ == "__main__":
    tracer = setup_tracer()
    run_consumer(redis_client, tracer)
