# worker/services/queue/factory.py
import os
import logging
from typing import Type, TypeVar
from pydantic import BaseModel

from .base import QueueBackend
from .models import JobPayload

logger = logging.getLogger(__name__)
T = TypeVar('T', bound=BaseModel)


def get_queue_backend(payload_type: Type[T] = JobPayload) -> QueueBackend[T]:
    """
    Factory: retorna el backend de colas configurado en .env.
    
    Args:
        payload_type: Tipo Pydantic para serialización/deserialización
    
    Returns:
        Instancia de QueueBackend[payload_type]
    
    Raises:
        ValueError: Si QUEUE_PROVIDER no es soportado
    """
    provider = os.getenv("QUEUE_PROVIDER", "redis").lower()
    logger.info(f"🔌 Inicializando cola con proveedor: {provider}")
    
    if provider == "redis":
        from .redis_queue import RedisQueue
        return RedisQueue[T](
            host=os.getenv("REDIS_HOST", "redis"),
            port=int(os.getenv("REDIS_PORT", 6379)),
            db=int(os.getenv("REDIS_DB", 0)),
            stream_name=os.getenv("REDIS_STREAM", "rag-jobs"),
            group_name=os.getenv("REDIS_GROUP", "rag-workers"),
            payload_type=payload_type
        )
    
    elif provider == "sqs":
        from .sqs_queue import SQSQueue
        return SQSQueue[T](
            queue_url=os.getenv("COLA_SQS_RAG"),
            region_name=os.getenv("AWS_REGION", "us-west-2"),
            endpoint_url=os.getenv("AWS_SECRET_ACCESS_URL"),
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
            payload_type=payload_type
        )
    
    # Futuros proveedores (descomentar cuando implementes):
    # elif provider == "rabbitmq":
    #     from .rabbit_queue import RabbitMQQueue
    #     return RabbitMQQueue[T](...)
    # 
    # elif provider == "kafka":
    #     from .kafka_queue import KafkaQueue
    #     return KafkaQueue[T](...)
    
    raise ValueError(
        f"Queue provider '{provider}' not supported. "
        f"Use: redis, sqs, rabbitmq, kafka"
    )


def get_queue_backends(payload_type: Type[T] = JobPayload) -> list[QueueBackend[T]]:
    """
    Factory: retorna lista de backends configurados en QUEUE_PROVIDERS.
    
    Args:
        payload_type: Tipo Pydantic para serialización/deserialización
    
    Returns:
        Lista de instancias QueueBackend[payload_type]
    """
    # Soporte para QUEUE_PROVIDERS (múltiples) o QUEUE_PROVIDER (single, legacy)
    providers_str = os.getenv("QUEUE_PROVIDERS") or os.getenv("QUEUE_PROVIDER", "redis")
    providers = [p.strip().lower() for p in providers_str.split(",") if p.strip()]
    
    backends = []
    for provider in providers:
        logger.info(f"🔌 Inicializando cola con proveedor: {provider}")
        
        if provider == "redis":
            from .redis_queue import RedisQueue
            backends.append(RedisQueue[T](
                host=os.getenv("REDIS_HOST", "redis"),
                port=int(os.getenv("REDIS_PORT", 6379)),
                db=int(os.getenv("REDIS_DB", 0)),
                stream_name=os.getenv("REDIS_STREAM", "rag-jobs"),
                group_name=os.getenv("REDIS_GROUP", "rag-workers"),
                payload_type=payload_type
            ))
        
        elif provider == "sqs":
            from .sqs_queue import SQSQueue
            backends.append(SQSQueue[T](
                queue_url=os.getenv("COLA_SQS_RAG"),
                region_name=os.getenv("AWS_REGION", "us-west-2"),
                endpoint_url=os.getenv("AWS_SECRET_ACCESS_URL"),
                aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
                aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
                payload_type=payload_type
            ))
        else:
            logger.warning(f"⚠️ Proveedor de cola no soportado: {provider}")
    
    if not backends:
        raise ValueError("No se configuró ningún proveedor de cola válido")
    
    logger.info(f"✅ Worker escuchando {len(backends)} cola(s): {', '.join(providers)}")
    return backends