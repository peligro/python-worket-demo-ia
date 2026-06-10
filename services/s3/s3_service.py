# worker/services/s3/s3_service.py
import os
import tempfile
import logging
from botocore.exceptions import ClientError
from worker.aws.aws import get_conection

logger = logging.getLogger(__name__)


class S3Service:
    """Servicio S3 para worker (descargar y archivar archivos)"""
    
    def __init__(self):
        self.s3 = get_conection()
        self.bucket = os.getenv("S3_BUCKET_NAME", "curso-udemy")
    
    def download_file_to_temp(self, s3_key: str) -> str:
        """Descarga archivo de S3 a temporal y retorna la ruta"""
        tmp_path = tempfile.mktemp(suffix=".pdf")
        try:
            self.s3.download_file(self.bucket, s3_key, tmp_path)
            logger.info(f"⬇️ Descargado: {s3_key} → {tmp_path}")
            return tmp_path
        except ClientError as e:
            logger.error(f"❌ Error descargando de S3: {e}")
            if os.path.exists(tmp_path):
                os.unlink(tmp_path)
            raise
    
    def archive_file(self, s3_key: str, job_id: int, user_id: int, original_filename: str) -> str:
        """
        Mueve archivo de rag-pdfs/ a archive/ con nombre descriptivo.
        """
        try:
            from datetime import datetime
            timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
            year = timestamp[:4]
            month = timestamp[4:6]
            
            # Sanitizar nombre
            safe_filename = "".join(c for c in original_filename if c.isalnum() or c in ('.', '_', '-'))
            
            # Nueva ruta descriptiva
            new_key = f"archive/{year}/{month}/job-{job_id}_user-{user_id}_{timestamp}_{safe_filename}"
            
            # Copiar a archive/
            self.s3.copy_object(
                Bucket=self.bucket,
                Key=new_key,
                CopySource={"Bucket": self.bucket, "Key": s3_key},
                Metadata={
                    "original-filename": original_filename,
                    "job-id": str(job_id),
                    "user-id": str(user_id),
                    "archived-at": datetime.utcnow().isoformat()
                }
            )
            
            # Eliminar de rag-pdfs/
            self.s3.delete_object(Bucket=self.bucket, Key=s3_key)
            
            logger.info(f" Archivo archivado: {s3_key} → {new_key}")
            return new_key
            
        except ClientError as e:
            logger.error(f" Error archivando archivo: {e}")
            raise
        except Exception as e:
            logger.error(f"❌ Error inesperado archivando: {e}")
            raise