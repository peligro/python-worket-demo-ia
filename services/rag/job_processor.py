
# worker/services/rag/job_processor.py
"""
Lógica de negocio para procesar un job RAG PDF.
Responsabilidad: orquestar descarga → procesamiento → archivado.
"""
import os
import time
import logging

from worker.models.rag_job import RAGJob, JobStatus
from worker.services.s3.s3_service import S3Service
from worker.services.rag.pdf_processor import process_pdf_to_chunks
from worker.database.database import get_session
from worker.services.queue.models import JobPayload

logger = logging.getLogger(__name__)


def process_job(payload: JobPayload, source: str = "unknown") -> None:
    """
    Procesa un job individual (completamente autocontenido).
    
    Flujo:
        1. Marcar job como PROCESSING
        2. Descargar PDF de S3
        3. Extraer pares P/R + embeddings
        4. Marcar job como COMPLETED
        5. Limpiar archivo temporal
        6. Archivar PDF en S3 y actualizar BD
    
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
            session.add(job)
            session.commit()
            
            logger.info(f"✅ [{source.upper()}] Job {payload.job_id} completado: {chunks_created} chunks en {elapsed_ms}ms")
            
            # 5. Limpiar archivo temporal local
            if os.path.exists(local_pdf_path):
                os.unlink(local_pdf_path)
                logger.debug(f"🧹 [{source.upper()}] Archivo temporal eliminado: {local_pdf_path}")
            
            # 6. ARCHIVAR PDF en S3 (para auditoría) + ACTUALIZAR BD
            try:
                archived_key = s3.archive_file(
                    s3_key=payload.s3_key,
                    job_id=payload.job_id,
                    user_id=payload.user_id,
                    original_filename=payload.filename
                )
                logger.debug(f"📦 [{source.upper()}] PDF archivado: {archived_key}")
                
                # ✅ ACTUALIZAR el s3_key en la BD para que apunte a la nueva ubicación
                job.s3_key = archived_key
                session.add(job)
                session.commit()
                logger.info(f"✅ [{source.upper()}] s3_key actualizado en BD: {archived_key}")
                
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
                session.add(job)
                session.commit()
            
            logger.warning(f"⚠️ [{source.upper()}] Job {payload.job_id} marcado como FAILED - PDF original preservado para debug")