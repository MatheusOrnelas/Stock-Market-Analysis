import logging
import os
import sys
from datetime import datetime

def setup_logging(log_dir="logs", log_filename="etl_execution.log"):
    """
    Configura o sistema de logging para escrever em arquivo e console.
    """
    # Garante que o diretório de logs existe
    os.makedirs(log_dir, exist_ok=True)
    
    # Caminho completo do arquivo de log
    log_path = os.path.join(log_dir, log_filename)

    # Configuração básica
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            # Handler para arquivo (append mode)
            logging.FileHandler(log_path, mode='a', encoding='utf-8'),
            # Handler para console (stdout)
            logging.StreamHandler(sys.stdout)
        ]
    )

    # Cria um logger root
    logger = logging.getLogger("StockMarketETL")
    logger.info(f"Logging iniciado. Arquivo: {log_path}")
    
    return logger

def get_logger(name):
    """Retorna um logger configurado para um módulo específico."""
    return logging.getLogger(name)

