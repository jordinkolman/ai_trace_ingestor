import asyncio
from contextlib import asynccontextmanager
import os 
import json 
from fastapi import FastAPI, WebSocket
import redis 

@asynccontextmanager
async def lifespan(app: FastAPI):
    listener_task = asyncio.create_task(redis_pubsub_listener())
    print("WebSocket service started: Redis Pub/Sub listener active.")

    yield

    print("WebSocket service shutting down: Canceling listener task...")
    listener_task.cancel()
    try:
        await listener_task
    except asyncio.CancelledError:
        print("Listener task successfully canceled")

app = FastAPI(title="AI Telemetry Stream", lifespan=lifespan)

redis_host = os.getenv("REDIS_HOST", "127.0.0.1")
redis_client = redis.Redis(host=redis_host, port=6379, db=0, decode_responses=True)

class ConnectionManager:
    def __init__(self):
        self.active_connections: list[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)

    def disconnect(self, websocket: WebSocket):
        self.active_connections.remove(websocket)

    async def broadcast(self, message: str):
        for connection in self.active_connections:
            try:
                await connection.send_text(message)
            except Exception:
                self.disconnect(connection)

manager = ConnectionManager()

async def redis_pubsub_listener():
    pubsub = redis_client.pubsub()
    pubsub.subscribe("live_traces")

    while True:
        try:
            message = pubsub.get_message(ignore_subscribe_messages=True, timeout=1.0)
            if message and message["type"] == "message":
                await manager.broadcast(message["data"])
            await asyncio.sleep(0.01)
        except asyncio.CancelledError:
            pass
        except Exception as e:
            print(f"Pub/Sub listener error: {e}")

@app.websocket("/ws/live-traces")
async def websocket_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        while True:
            await websocket.receive_text()
    except Exception:
        manager.disconnect(websocket)
