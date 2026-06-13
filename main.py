#!/usr/bin/env python3
# worker/main.py
"""
Entry point del Worker RAG PDF.
Solo se encarga de:
- Configurar logging
- Configurar PYTHONPATH
- Registrar signal handlers
- Llamar al loop principal

La lógica de negocio vive en:
- worker/core/worker.py (loop principal)
- worker/services/rag/job_processor.py (procesamiento de jobs)
"""
import os
import sys
import signal
import logging
from pathlib import Path
from dotenv import load_dotenv

# =============================================================================
# BOOTSTRAP: Variables de entorno y PYTHONPATH
# =============================================================================
load_dotenv()

# Agregar ruta base al PYTHONPATH
BASE_DIR = Path(__file__).parent.parent
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

# =============================================================================
# LOGGING
# =============================================================================
logging.basicConfig(
    level=getattr(logging, os.getenv("LOG_LEVEL", "INFO")),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# =============================================================================
# ENTRY POINT
# =============================================================================
def main():
    """Punto de entrada del worker."""
    from core.worker import run_worker
    from core.signals import GracefulShutdown
    
    # Registrar signal handlers globalmente
    shutdown = GracefulShutdown()
    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)
    
    # Ejecutar loop principal
    run_worker()


if __name__ == "__main__":
    main()