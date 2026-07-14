from pydantic import BaseModel

class LLMTrace(BaseModel):
    user_id: str
    model_name: str 
    input_tokens: int 
    output_tokens: int 
    latency_ms: int
