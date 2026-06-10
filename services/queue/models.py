# worker/services/queue/models.py
from pydantic import BaseModel, Field
from typing import Optional, Literal
from datetime import datetime


class JobPayload(BaseModel):
    """
    Payload estándar para jobs de procesamiento RAG.
    
    Usado tanto en enqueue (API) como en dequeue (worker).
    Todos los campos son serializables a JSON para colas.
    """
    job_id: int = Field(..., description="ID del job en rag_jobs")
    s3_key: str = Field(..., description="Ruta del PDF en S3")
    filename: str = Field(..., description="Nombre original del archivo")
    user_id: Optional[int] = Field(default=None, description="Usuario que subió el archivo")
    priority: Literal["low", "normal", "high"] = Field(default="normal", description="Prioridad del job")
    created_at: datetime = Field(default_factory=datetime.utcnow, description="Timestamp de creación")
    
    # Campo interno para backends que lo requieran (ej: SQS receipt_handle)
    _internal_metadata: Optional[dict] = None
    
    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }
    
    def model_dump_json_safe(self) -> str:
        """Serializa el modelo excluyendo campos internos"""
        dump = self.model_dump(mode='json')
        dump.pop('_internal_metadata', None)
        import json
        return json.dumps(dump)
