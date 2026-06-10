#!/usr/bin/env python3
# worker/main.py
"""
Worker RAG PDF - Multi-cola (Redis Streams + SQS)
- Consume de múltiples colas configuradas (Redis, SQS, RabbitMQ, Kafka)
- Procesamiento autocontenido: cada job se procesa completamente dentro del worker
- Logging detallado con origen de la cola para debugging
- Manejo de shutdown limpio con señalización (SIGINT/SIGTERM)

Uso:
    PYTHONPATH=/usr/src/app python worker/main.py
"""
import os
import sys
import time
import signal
import logging
import tempfile
from pathlib import Path
from dotenv import load_dotenv
from datetime import datetime, timezone

# Cargar variables de entorno
load_dotenv()

# Configurar logging
logging.basicConfig(
    level=getattr(logging, os.getenv("LOG_LEVEL", "INFO")),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Agregar ruta base al PYTHONPATH
BASE_DIR = Path(__file__).parent.parent
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

# =============================================================================
# IMPORTS DEL WORKER (autocontenidos, SIN api/)
# =============================================================================
from worker.services.queue.factory import get_queue_backends
from worker.services.queue.models import JobPayload
from worker.models.rag_job import RAGJob, JobStatus
from worker.services.s3.s3_service import S3Service
from worker.services.rag.pdf_processor import process_pdf_to_chunks
from worker.database.database import get_session

# =============================================================================
# CONFIGURACIÓN
# =============================================================================
running = True

def handle_shutdown(signum, frame):
    """Cerrar worker limpiamente"""
    global running
    logger.info("🛑 Recibida señal de cierre. Finalizando jobs pendientes...")
    running = False

signal.signal(signal.SIGINT, handle_shutdown)
signal.signal(signal.SIGTERM, handle_shutdown)

# =============================================================================
# LÓGICA DEL WORKER
# =============================================================================
def process_job(payload: JobPayload, source: str = "unknown"):
    """
    Procesa un job individual (completamente autocontenido).
    
    Args:
        payload: Datos del job a procesar
        source: Origen de la cola para logging (redis, sqs, rabbitmq)
    """
    logger.info(f"📥 [{source.upper()}] Procesando job {payload.job_id}: {payload.filename}")
    
    with get_session() as session:
        try:
            # 1. Obtener y actualizar job a "processing"
            job = session.get(RAGJob, payload.job_id)
            if not job:
                logger.error(f"❌ [{source.upper()}] Job {payload.job_id} no encontrado en DB")
                return
            
            job.status = JobStatus.PROCESSING
            # ⚠️ started_at está comentado en el modelo - descomentar si lo agregas
            # job.started_at = datetime.now(timezone.utc)
            session.add(job)
            session.commit()
            
            # 2. Descargar PDF de S3
            s3 = S3Service()
            local_pdf_path = s3.download_file_to_temp(payload.s3_key)
            logger.debug(f"📄 [{source.upper()}] PDF descargado: {local_pdf_path}")
            
            # 3. Procesar PDF (extracción + embeddings)
            start_time = time.time()
            chunks_created = process_pdf_to_chunks(
                pdf_path=local_pdf_path,
                session=session,
                source_pdf=payload.filename
            )
            elapsed_ms = int((time.time() - start_time) * 1000)
            
            # 4. Actualizar job a "completed"
            job.status = JobStatus.COMPLETED
            job.chunks_created = chunks_created
            job.processing_time_ms = elapsed_ms
            # ⚠️ completed_at está comentado en el modelo - descomentar si lo agregas
            # job.completed_at = datetime.now(timezone.utc)
            session.add(job)
            session.commit()
            
            logger.info(f"✅ [{source.upper()}] Job {payload.job_id} completado: {chunks_created} chunks en {elapsed_ms}ms")
            
            # 5. Limpiar archivo temporal local
            if os.path.exists(local_pdf_path):
                os.unlink(local_pdf_path)
                logger.debug(f"🧹 [{source.upper()}] Archivo temporal eliminado: {local_pdf_path}")
            
            # 6. ARCHIVAR PDF en S3 (para auditoría)
            try:
                archived_key = s3.archive_file(
                    s3_key=payload.s3_key,
                    job_id=payload.job_id,
                    user_id=payload.user_id,
                    original_filename=payload.filename
                )
                logger.debug(f"📦 [{source.upper()}] PDF archivado: {archived_key}")
            except Exception as archive_err:
                # No fallar el job si el archivado falla (es opcional)
                logger.warning(f"⚠️ [{source.upper()}] No se pudo archivar PDF: {archive_err}")
            
        except Exception as e:
            logger.error(f"❌ [{source.upper()}] Error procesando job {payload.job_id}: {e}", exc_info=True)
            
            # Actualizar a "failed"
            job = session.get(RAGJob, payload.job_id)
            if job:
                job.status = JobStatus.FAILED
                job.error_message = str(e)
                # ⚠️ failed_at está comentado en el modelo - descomentar si lo agregas
                # job.failed_at = datetime.now(timezone.utc)
                session.add(job)
                session.commit()
            
            # ⚠️ Si falla, NO archivamos (dejamos en rag-pdfs/ para debug)
            logger.warning(f"⚠️ [{source.upper()}] Job {payload.job_id} marcado como FAILED - PDF original preservado para debug")

def _get_source_name(queue) -> str:
    """Extrae nombre legible del backend de cola para logging"""
    class_name = queue.__class__.__name__
    # Mapeo: RedisQueue -> redis, SqsQueue -> sqs, RabbitMQQueue -> rabbitmq
    return class_name.replace("Queue", "").lower()

def main():
    global running
    """Loop principal del worker - consume de MÚLTIPLES colas con round-robin"""
    logger.info("🚀 Worker RAG PDF iniciado (multi-cola, autocontenido)")
    
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
    
    while running:
        processed_any = False
        
        # Round-robin: intenta obtener mensaje de cada cola en orden
        for queue in queues:
            if not running:
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
                running = False
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

if __name__ == "__main__":
    main()