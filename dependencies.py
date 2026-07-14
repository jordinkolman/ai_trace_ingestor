import os
import redis

redis_host = os.getenv("REDIS_HOST", "127.0.0.1")
redis_client = redis.Redis(host=redis_host, port=6379, db=0, decode_responses=True)
