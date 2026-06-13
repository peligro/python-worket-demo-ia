# worker/core/signals.py
"""
Manejo de señales del sistema (SIGINT/SIGTERM) para shutdown limpio.
"""
import signal
import logging

logger = logging.getLogger(__name__)


class GracefulShutdown:
    """
    Maneja señales de terminación para cerrar worker limpiamente.
    
    Uso:
        shutdown = GracefulShutdown()
        signal.signal(signal.SIGINT, shutdown)
        signal.signal(signal.SIGTERM, shutdown)
        
        while shutdown.running:
            ...
    """
    def __init__(self):
        self.running = True
    
    def __call__(self, signum, frame):
        """Callback para signal.signal()"""
        sig_name = signal.Signals(signum).name
        logger.info(f"🛑 Recibida señal {sig_name}. Finalizando jobs pendientes...")
        self.running = False
