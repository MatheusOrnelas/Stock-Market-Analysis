import os
import pandas as pd
from src.utils.db import insert_dataframe, get_db_engine
from src.config.logging_config import get_logger

logger = get_logger(__name__)

# Mapping filename (without extension) -> table_name
CSV_TO_TABLE_MAP = {
    # Funds Explorer
    "funds_ativos": "funds_ativos",
    "funds_info_completa": "funds_info_completa",
    "funds_localizacao": "funds_localizacao",
    "funds_rendimentos": "funds_rendimentos",
    "funds_simulacao": "funds_simulacao",
    "funds_indicadores_diarios": "funds_indicadores_diarios", # New table
    "funds_page_snapshots": "funds_page_snapshots",

    # Yahoo
    "yahoo_cotacoes": "cotacoes_historico"
}

def load_bronze_to_silver(bronze_dir):
    logger.info(f"Iniciando carga de {bronze_dir} para o banco de dados...")
    try:
        engine = get_db_engine()
    except Exception as e:
        logger.critical(f"Falha ao conectar ao banco de dados: {e}", exc_info=True)
        return
    
    if not os.path.exists(bronze_dir):
        logger.error(f"Diretório não encontrado: {bronze_dir}")
        return

    for filename in os.listdir(bronze_dir):
        if filename.endswith(".csv"):
            file_key = os.path.splitext(filename)[0]
            table_name = CSV_TO_TABLE_MAP.get(file_key)
            
            if table_name:
                filepath = os.path.join(bronze_dir, filename)
                try:
                    logger.info(f"Processando arquivo: {filename} -> Tabela: {table_name}")
                    try:
                        df = pd.read_csv(filepath)
                    except UnicodeDecodeError:
                         logger.warning(f"Encoding padrão falhou para {filename}, tentando latin1")
                         df = pd.read_csv(filepath, encoding='latin1')
                    
                    # Special handling for appending history instead of replacing
                    if table_name == "funds_indicadores_diarios":
                        # HISTÓRICO: append (não dropa tabela)
                        insert_dataframe(df, table_name, engine, if_exists="append", drop_existing=False)
                    else:
                        insert_dataframe(df, table_name, engine)
                except Exception as e:
                    logger.error(f"Erro ao carregar {filename}: {e}", exc_info=True)
            else:
                logger.debug(f"Arquivo {filename} ignorado (sem mapeamento definido).")
                
    logger.info("Carga concluída.")

if __name__ == "__main__":
    from src.config.logging_config import setup_logging
    setup_logging()
    BRONZE_DIR = os.path.join("stock-market-etl", "data", "bronze")
    load_bronze_to_silver(BRONZE_DIR)
