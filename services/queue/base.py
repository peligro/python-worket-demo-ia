# worker/services/queue/base.py
from abc import ABC, abstractmethod
from typing import Optional, Tuple, Type, TypeVar, Generic
from pydantic import BaseModel

# Tipo genérico para payloads tipados con Pydantic
T = TypeVar('T', bound=BaseModel)


class QueueBackend(ABC, Generic[T]):
    """
    Interfaz abstracta para backends de colas.
    
    Patrón Strategy: permite cambiar entre Redis/SQS/RabbitMQ/Kafka
    sin modificar la lógica de negocio.
    
    Uso con tipos:
        queue: QueueBackend[JobPayload] = get_queue_backend(JobPayload)
        queue.enqueue(JobPayload(job_id=123, ...))
    """
    
    @abstractmethod
    def enqueue(self, payload: T) -> str:
        """
        Agregar job a la cola.
        
        Args:
            payload: Objeto Pydantic con los datos del job
        
        Returns:
            ID del mensaje (para tracking interno)
        """
        pass
    
    @abstractmethod
    def dequeue(
        self, 
        block_seconds: int = 5
    ) -> Tuple[Optional[str], Optional[T]]:
        """
        Obtener job pendiente.
        
        Args:
            block_seconds: Tiempo máximo a esperar por un nuevo mensaje
        
        Returns:
            (message_id, payload) o (None, None) si timeout
        """
        pass
    
    @abstractmethod
    def acknowledge(self, message_id: str) -> bool:
        """
        Confirmar procesamiento exitoso.
        
        Args:
            message_id: ID del mensaje a confirmar
        
        Returns:
            True si se confirmó, False si ya no existía
        """
        pass
    
    @abstractmethod
    def nack(self, message_id: str, requeue: bool = True) -> bool:
        """
        Rechazar mensaje (para reintentos o dead-letter).
        
        Args:
            message_id: ID del mensaje
            requeue: Si True, vuelve a la cola; si False, va a DLQ
        
        Returns:
            True si se procesó correctamente
        """
        pass
    
    @abstractmethod
    def get_pending_count(self) -> int:
        """Obtener número de mensajes pendientes en la cola"""
        pass
    
    @abstractmethod
    def create_consumer_group(self) -> None:
        """
        Inicializar consumer group (solo aplica a Redis Streams).
        Para SQS/RabbitMQ/Kafka, este método puede ser no-op.
        """
        pass
