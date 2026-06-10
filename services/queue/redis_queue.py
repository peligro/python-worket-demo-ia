# worker/services/queue/redis_queue.py
import os
import json
import redis
import logging
from typing import Type, Optional, Tuple, TypeVar
from pydantic import BaseModel

from .base import QueueBackend

logger = logging.getLogger(__name__)
T = TypeVar('T', bound=BaseModel)


class RedisQueue(QueueBackend[T]):
    """
    Implementación de QueueBackend usando Redis Streams.
    
    Características:
    - Consumer groups para procesamiento paralelo
    - ACKs explícitos para garantía de entrega
    - Eliminación inmediata de mensajes procesados
    - Timeouts configurados para evitar bloqueos
    """
    
    def __init__(
        self,
        host: str,
        port: int,
        db: int,
        stream_name: str,
        group_name: str,
        payload_type: Type[T]
    ):
        self.redis = redis.Redis(
            host=host, 
            port=port, 
            db=db, 
            decode_responses=True,
            socket_connect_timeout=5,
            socket_timeout=30,
            retry_on_timeout=True,
            health_check_interval=30
        )
        self.stream = stream_name
        self.group = group_name
        self.consumer = f"worker-{os.getpid()}"
        self.payload_type = payload_type
        logger.info(f"🔌 RedisQueue conectado: {host}:{port}/{db} | stream={stream_name}")
    
    def enqueue(self, payload: T) -> str:
        """Encolar mensaje (usado por API u otros productores)"""
        try:
            message_id = self.redis.xadd(
                name=self.stream,
                fields={"payload": payload.model_dump_json()},
                maxlen=10000,
                approximate=True
            )
            logger.debug(f"📤 [Redis] Job encolado: {message_id} | job_id={payload.job_id}")
            return message_id
        except redis.exceptions.ConnectionError as e:
            logger.error(f"❌ [Redis] Error de conexión: {e}")
            raise
        except Exception as e:
            logger.error(f"❌ [Redis] Error encolando: {e}")
            raise
    
    def dequeue(
        self, 
        block_seconds: int = 5
    ) -> Tuple[Optional[str], Optional[T]]:
        """
        Obtener job pendiente (bloqueante hasta timeout).
        
        Args:
            block_seconds: Tiempo máximo a esperar por un nuevo mensaje
        
        Returns:
            (message_id, payload) o (None, None) si timeout
        """
        try:
            result = self.redis.xreadgroup(
                groupname=self.group,
                consumername=self.consumer,
                streams={self.stream: ">"},
                count=1,
                block=block_seconds * 1000
            )
            
            if result:
                _, messages = result[0]
                message_id, fields = messages[0]
                
                payload_data = json.loads(fields["payload"])
                payload = self.payload_type(**payload_data)
                
                logger.debug(f"📥 [Redis] Job recibido: {message_id} | job_id={payload.job_id}")
                return message_id, payload
            
            return None, None
            
        except redis.exceptions.ResponseError as e:
            if "NOGROUP" in str(e):
                logger.warning("⚠️ [Redis] Consumer group no encontrado, creando...")
                self.create_consumer_group()
                return self.dequeue(block_seconds)
            raise
        except redis.exceptions.TimeoutError as e:
            logger.warning(f"⚠️ [Redis] Timeout leyendo cola: {e}")
            return None, None
        except Exception as e:
            logger.error(f"❌ [Redis] Error obteniendo job: {e}")
            raise
    
    def acknowledge(self, message_id: str) -> bool:
        """Confirmar procesamiento exitoso (elimina mensaje de pending)"""
        try:
            count = self.redis.xack(self.stream, self.group, message_id)
            if count > 0:
                logger.debug(f"✅ [Redis] Job confirmado: {message_id}")
                return True
            logger.warning(f"⚠️ [Redis] Job ya confirmado o no encontrado: {message_id}")
            return False
        except Exception as e:
            logger.error(f"❌ [Redis] Error confirmando: {e}")
            raise
    
    def delete_message(self, message_id: str) -> bool:
        """
        Eliminar mensaje permanentemente del stream.
        Usar DESPUÉS de acknowledge() para limpieza inmediata.
        """
        try:
            deleted_count = self.redis.xdel(self.stream, message_id)
            if deleted_count > 0:
                logger.info(f"🗑️ [Redis] Mensaje eliminado del stream: {message_id}")
                return True
            logger.warning(f"⚠️ [Redis] Mensaje ya eliminado o no encontrado: {message_id}")
            return False
        except Exception as e:
            logger.error(f"❌ [Redis] Error eliminando mensaje: {e}")
            raise
    
    def nack(self, message_id: str, requeue: bool = True) -> bool:
        """
        Rechazar mensaje (para reintentos o dead-letter).
        """
        if not requeue:
            dlq_stream = f"{self.stream}:dlq"
            try:
                self.redis.xadd(
                    dlq_stream, 
                    {"original_id": message_id, "reason": "nack"}, 
                    maxlen=1000
                )
                logger.warning(f"⚠️ [Redis] Job enviado a DLQ: {message_id}")
            except Exception as e:
                logger.error(f"❌ [Redis] Error moviendo a DLQ: {e}")
        else:
            logger.debug(f"🔄 [Redis] Job pendiente para retry: {message_id}")
        return True
    
    def get_pending_count(self) -> int:
        """Obtener número de mensajes pendientes en el consumer group"""
        try:
            info = self.redis.xinfo_groups(self.stream)
            for group in info:
                if group["name"] == self.group:
                    return group["pending"]
            return 0
        except Exception:
            return 0
    
    def create_consumer_group(self) -> None:
        """Crear consumer group (ejecutar una vez al iniciar el worker)"""
        try:
            self.redis.xgroup_create(
                name=self.stream,
                id="$",
                groupname=self.group,
                mkstream=True
            )
            logger.info(f"✅ [Redis] Consumer group '{self.group}' creado en stream '{self.stream}'")
        except redis.exceptions.ResponseError as e:
            if "BUSYGROUP" in str(e):
                logger.debug(f"ℹ️ [Redis] Consumer group '{self.group}' ya existe")
            else:
                raise