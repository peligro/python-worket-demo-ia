# worker/core/signals.py
"""
Manejo de señales del sistema (SIGINT/SIGTERM) para shutdown limpio.
"""
import signal
import logging
import sys
import time

logger = logging.getLogger(__name__)


class GracefulShutdown:
    """
    Maneja señales de terminación para cerrar worker limpiamente.
    """
    def __init__(self):
        self.running = True
        self.shutdown_requested = False
    
    def __call__(self, signum, frame):
        """Callback para signal.signal()"""
        sig_name = signal.Signals(signum).name
        
        if self.shutdown_requested:
            # Segunda señal = forzar salida inmediata
            logger.warning(f"⚡ {sig_name} recibido nuevamente. Forzando salida...")
            sys.exit(0)
        
        logger.info(f"🛑 Recibida señal {sig_name}. Finalizando jobs pendientes...")
        logger.info("💡 Presiona Ctrl+C nuevamente para forzar salida inmediata")
        
        self.running = False
        self.shutdown_requested = True
        
        # Programar timeout de 10 segundos para forzar salida
        def force_exit(signum, frame):
            logger.error("⏰ Timeout de shutdown alcanzado. Forzando terminación...")
            sys.exit(1)
        
        signal.signal(signal.SIGALRM, force_exit)
        signal.alarm(10)  # 10 segundos máximo