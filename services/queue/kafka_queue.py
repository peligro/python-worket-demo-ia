import os
import json
import logging
from typing import Type, TypeVar, Optional, Tuple
from pydantic import BaseModel
from confluent_kafka import Consumer, KafkaError, KafkaException
from confluent_kafka.admin import AdminClient, NewTopic

from .base import QueueBackend

logger = logging.getLogger(__name__)
T = TypeVar('T', bound=BaseModel)


class KafkaQueue(QueueBackend[T]):
    """
    Implementación de QueueBackend usando Apache Kafka.
    
    Características:
    - Consumer groups nativos de Kafka
    - Auto-commit desactivado para control manual
    - Soporte para múltiples particiones
    """
    
    def __init__(
        self,
        bootstrap_servers: str,
        topic: str,
        group_id: str,
        payload_type: Type[T]
    ):
        self.topic = topic
        self.payload_type = payload_type
        
        # Configuración del consumer
        consumer_config = {
            'bootstrap.servers': bootstrap_servers,
            'group.id': group_id,
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False,  # Manual commit para garantía
            'session.timeout.ms': '30000',
            'max.poll.interval.ms': '300000',
        }
        
        self.consumer = Consumer(consumer_config)
        
        # Suscribirse al topic
        self.consumer.subscribe([topic])
        
        logger.info(f"🔌 KafkaQueue conectado: {bootstrap_servers} | topic={topic} | group={group_id}")
    
    def enqueue(self, payload: T) -> str:
        """
        Encolar mensaje en Kafka.
        Nota: Normalmente el producer está en la API, no en el worker.
        """
        raise NotImplementedError("KafkaQueue.enqueue() no debe usarse en el worker")
    
    def dequeue(
        self, 
        block_seconds: int = 5
    ) -> Tuple[Optional[str], Optional[T]]:
        """
        Obtener job pendiente de Kafka.
        
        Args:
            block_seconds: Tiempo máximo a esperar (no se usa directamente, 
                          Kafka usa poll timeout)
        
        Returns:
            (message_id, payload) o (None, None) si timeout
        """
        try:
            # Poll de Kafka (timeout en segundos)
            msg = self.consumer.poll(timeout=block_seconds)
            
            if msg is None:
                # Timeout: no hay mensajes
                return None, None
            
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition, esperar más
                    return None, None
                else:
                    logger.error(f"❌ [Kafka] Error: {msg.error()}")
                    raise KafkaException(msg.error())
            
            # Parsear mensaje
            payload_data = json.loads(msg.value().decode('utf-8'))
            payload = self.payload_type(**payload_data)
            
            # Usar offset como message_id (único por partición)
            message_id = f"{msg.topic()}-{msg.partition()}-{msg.offset()}"
            
            logger.debug(f"📥 [Kafka] Job recibido: {message_id} | job_id={payload.job_id}")
            
            return message_id, payload
            
        except KafkaException as e:
            logger.error(f"❌ [Kafka] Error obteniendo job: {e}")
            raise
        except json.JSONDecodeError as e:
            logger.error(f"❌ [Kafka] Error parseando payload: {e}")
            raise
    
    def acknowledge(self, message_id: str) -> bool:
        """
        Confirmar procesamiento exitoso (commit manual del offset).
        
        Args:
            message_id: No se usa en Kafka (el commit es del consumer actual)
        """
        try:
            # Commit del offset actual
            self.consumer.commit(asynchronous=False)
            logger.debug(f"✅ [Kafka] Job confirmado: {message_id}")
            return True
        except KafkaException as e:
            logger.error(f"❌ [Kafka] Error confirmando: {e}")
            return False
    
    def nack(self, message_id: str, requeue: bool = True) -> bool:
        """
        Rechazar mensaje.
        
        En Kafka:
        - Si requeue=True: no hacemos commit, el mensaje se reprocesará
        - Si requeue=False: hacemos commit de todas formas (se pierde)
        """
        if not requeue:
            # Commit de todas formas (mensaje se "pierde")
            try:
                self.consumer.commit(asynchronous=False)
                logger.warning(f"⚠️ [Kafka] Job descartado: {message_id}")
            except KafkaException as e:
                logger.error(f"❌ [Kafka] Error descartando job: {e}")
        else:
            # No hacer commit = el mensaje se reprocesará en el próximo poll
            logger.debug(f"🔄 [Kafka] Job se reprocesará: {message_id}")
        
        return True
    
    def get_pending_count(self) -> int:
        """
        Obtener número de mensajes pendientes.
        Nota: Kafka no expone esto fácilmente, retornamos -1 (desconocido).
        """
        # Kafka no tiene un "pending count" simple como Redis/SQS
        # Requeriría usar Kafka Admin API o herramientas externas
        return -1
    
    def create_consumer_group(self) -> None:
        """
        Crear topic si no existe (solo para desarrollo).
        En producción, los topics se crean con Kafka Admin o Terraform.
        """
        try:
            admin_client = AdminClient({
                'bootstrap.servers': self.consumer.config['bootstrap.servers']
            })
            
            # Verificar si el topic existe
            topics = admin_client.list_topics(timeout=5)
            
            if self.topic not in topics.topics:
                # Crear topic con 1 partición y replication factor 1 (dev)
                new_topic = NewTopic(
                    self.topic,
                    num_partitions=1,
                    replication_factor=1
                )
                futures = admin_client.create_topics([new_topic])
                
                # Esperar creación
                for topic, future in futures.items():
                    future.result()  # Raise exception if failed
                
                logger.info(f"✅ [Kafka] Topic '{self.topic}' creado")
            else:
                logger.debug(f"ℹ️ [Kafka] Topic '{self.topic}' ya existe")
                
        except Exception as e:
            logger.warning(f"⚠️ [Kafka] No se pudo crear topic: {e}")
            # No fallar si no se puede crear (el topic puede existir ya)
    
    def close(self) -> None:
        """Cerrar conexión del consumer"""
        try:
            self.consumer.close()
            logger.info("🔌 [Kafka] Consumer cerrado")
        except Exception as e:
            logger.error(f"❌ [Kafka] Error cerrando consumer: {e}")
