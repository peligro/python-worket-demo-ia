# worker/core/worker.py
"""
Loop principal del worker RAG PDF.
Responsabilidad: consumir mensajes de múltiples colas con round-robin.
"""
import time
import logging

from worker.services.queue.factory import get_queue_backends
from worker.services.queue.models import JobPayload
from worker.services.rag.job_processor import process_job
from worker.core.signals import GracefulShutdown

logger = logging.getLogger(__name__)


def _get_source_name(queue) -> str:
    """Extrae nombre legible del backend de cola para logging"""
    class_name = queue.__class__.__name__
    return class_name.replace("Queue", "").lower()


def run_worker() -> None:
    """
    Loop principal del worker - consume de MÚLTIPLES colas con round-robin.
    
    Flujo:
        1. Inicializar backends de cola (Redis, SQS, etc.)
        2. Crear consumer groups
        3. Loop round-robin consumiendo mensajes
        4. Procesar cada job y confirmar (ACK)
        5. Cerrar limpiamente al recibir señal
    """
    logger.info("🚀 Worker RAG PDF iniciado (multi-cola, autocontenido)")
    
    shutdown = GracefulShutdown()
    
    # Inicializar TODOS los backends configurados
    queues = get_queue_backends(JobPayload)
    
    if not queues:
        logger.error("❌ No se configuraron colas. Verifica QUEUE_PROVIDERS en .env")
        return
    
    # Crear consumer groups (solo aplica a Redis Streams)
    for queue in queues:
        try:
            queue.create_consumer_group()
            logger.info(f"🔗 Consumer group registrado en {_get_source_name(queue).upper()}")
        except Exception as e:
            logger.warning(f"⚠️ No se pudo crear consumer group en {_get_source_name(queue)}: {e}")
    
    queue_names = ", ".join(_get_source_name(q) for q in queues)
    logger.info(f"⏳ Escuchando {len(queues)} cola(s): [{queue_names}] (Ctrl+C para salir)")
    
    while shutdown.running:
        processed_any = False
        
        # Round-robin: intenta obtener mensaje de cada cola en orden
        for queue in queues:
            if not shutdown.running:
                break
                
            source = _get_source_name(queue)
            
            try:
                # Usar block_seconds bajo para no bloquear otras colas
                message_id, payload = queue.dequeue(block_seconds=1)
                
                if payload is None:
                    continue  # Timeout en esta cola, probar la siguiente
                
                # Procesar job (con origen para debugging)
                process_job(payload, source=source)
                
                # Confirmar mensaje (ACK - quita de pending)
                queue.acknowledge(message_id)
                logger.debug(f"✅ [{source.upper()}] ACK enviado: {message_id}")
                
                # ELIMINAR mensaje permanentemente del stream/queue
                if hasattr(queue, 'delete_message'):
                    queue.delete_message(message_id)
                    logger.debug(f"🗑️ [{source.upper()}] Mensaje eliminado permanentemente: {message_id}")
                
                processed_any = True
                # Break para ser justo: procesamos 1 mensaje y reiniciamos loop
                break
                
            except KeyboardInterrupt:
                logger.info("👋 Interrupción manual detectada")
                shutdown.running = False
                break
            except Exception as e:
                logger.error(f"❌ [{source.upper()}] Error procesando mensaje: {e}", exc_info=True)
                # No hacer break: seguimos intentando con otras colas
        
        # Si no procesamos nada en ninguna cola, esperamos un poco antes de retry
        if not processed_any:
            time.sleep(0.5)
    
    # Limpieza final
    logger.info("🔚 Cerrando conexiones de cola...")
    for queue in queues:
        try:
            if hasattr(queue, 'close'):
                queue.close()
        except Exception as e:
            logger.warning(f"⚠️ Error cerrando cola {_get_source_name(queue)}: {e}")
    
    logger.info("✅ Worker detenido limpiamente")
