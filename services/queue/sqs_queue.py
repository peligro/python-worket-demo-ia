# worker/services/queue/sqs_queue.py
import os
import json
import boto3
import logging
from typing import Type, TypeVar, Optional, Tuple
from pydantic import BaseModel

from .base import QueueBackend

logger = logging.getLogger(__name__)
T = TypeVar('T', bound=BaseModel)


class SQSQueue(QueueBackend[T]):
    """
    Implementación de QueueBackend usando AWS SQS.
    
    Compatible con LocalStack para desarrollo local.
    """
    
    def __init__(
        self,
        queue_url: str,
        region_name: str,
        endpoint_url: Optional[str],
        aws_access_key_id: Optional[str],
        aws_secret_access_key: Optional[str],
        payload_type: Type[T]
    ):
        self.queue_url = queue_url
        self.payload_type = payload_type
        
        # Configurar cliente SQS (LocalStack o AWS real)
        sqs_kwargs = {"region_name": region_name}
        
        if endpoint_url:
            sqs_kwargs["endpoint_url"] = endpoint_url
            logger.info(f"🔌 SQS conectado a LocalStack: {endpoint_url}")
        
        if aws_access_key_id and aws_secret_access_key:
            sqs_kwargs["aws_access_key_id"] = aws_access_key_id
            sqs_kwargs["aws_secret_access_key"] = aws_secret_access_key
        
        self.sqs = boto3.client("sqs", **sqs_kwargs)
        logger.info(f"🔌 SQSQueue inicializado: {queue_url}")
    
    def enqueue(self, payload: T) -> str:
        try:
            response = self.sqs.send_message(
                QueueUrl=self.queue_url,
                MessageBody=payload.model_dump_json(),
                MessageAttributes={
                    "job_id": {
                        "DataType": "String", 
                        "StringValue": str(payload.job_id)
                    },
                    "priority": {
                        "DataType": "String", 
                        "StringValue": payload.priority
                    }
                }
            )
            message_id = response["MessageId"]
            logger.debug(f"📤 [SQS] Job encolado: {message_id} | job_id={payload.job_id}")
            return message_id
        except Exception as e:
            logger.error(f"❌ [SQS] Error encolando: {e}")
            raise
    
    def dequeue(
        self, 
        block_seconds: int = 5
    ) -> Tuple[Optional[str], Optional[T]]:
        try:
            response = self.sqs.receive_message(
                QueueUrl=self.queue_url,
                MaxNumberOfMessages=1,
                WaitTimeSeconds=block_seconds,  # Long polling
                MessageAttributeNames=["All"],
                AttributeNames=["All"]
            )
            
            messages = response.get("Messages", [])
            if messages:
                msg = messages[0]
                receipt_handle = msg["ReceiptHandle"]
                
                payload_data = json.loads(msg["Body"])
                payload = self.payload_type(**payload_data)
                
                # Guardar receipt_handle en metadata interna para acknowledge
                payload._internal_metadata = {"receipt_handle": receipt_handle}  # type: ignore
                
                logger.debug(f"📥 [SQS] Job recibido: {msg['MessageId']} | job_id={payload.job_id}")
                return receipt_handle, payload
            
            # Timeout: no hay mensajes
            return None, None
            
        except Exception as e:
            logger.error(f"❌ [SQS] Error obteniendo job: {e}")
            raise
    
    def acknowledge(self, message_id: str) -> bool:
        """
        En SQS, message_id es el receipt_handle.
        Delete message = acknowledge.
        """
        try:
            self.sqs.delete_message(
                QueueUrl=self.queue_url,
                ReceiptHandle=message_id
            )
            logger.debug(f"✅ [SQS] Job confirmado: {message_id}")
            return True
        except Exception as e:
            logger.error(f"❌ [SQS] Error confirmando: {e}")
            return False
    
    def nack(self, message_id: str, requeue: bool = True) -> bool:
        """
        En SQS: no hacer delete = el mensaje vuelve a la cola 
        tras visibility_timeout (default: 30s).
        """
        if not requeue:
            logger.warning(f"⚠️ [SQS] Job enviado a DLQ (requiere configuración previa): {message_id}")
        else:
            logger.debug(f"🔄 [SQS] Job volverá a la cola tras timeout: {message_id}")
        return True
    
    def get_pending_count(self) -> int:
        try:
            attrs = self.sqs.get_queue_attributes(
                QueueUrl=self.queue_url,
                AttributeNames=["ApproximateNumberOfMessages"]
            )
            return int(attrs["Attributes"]["ApproximateNumberOfMessages"])
        except Exception:
            return 0
    
    def create_consumer_group(self) -> None:
        """SQS no requiere consumer groups (cada worker compite por mensajes)"""
        logger.debug("ℹ️ [SQS] No requiere consumer group")
