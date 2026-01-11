import os
import sys
import pandas as pd
from dotenv import load_dotenv

# Adiciona o diretório raiz ao path para imports funcionarem
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(os.path.dirname(current_dir))
sys.path.append(os.path.join(project_root, 'stock-market-etl'))

from src.config.logging_config import setup_logging
from src.extract.funds_explorer import FundsExplorerScraper
from src.extract.yahoo_finance import YahooFinanceScraper
from src.transform.loader import load_bronze_to_silver

def load_manual_tickers(filepath):
    if not os.path.exists(filepath):
        return []
    with open(filepath, 'r') as f:
        tickers = [line.strip() for line in f.readlines() if line.strip()]
    return tickers

def main():
    load_dotenv()
    
    # Configurações de caminhos
    BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    BRONZE_DIR = os.path.join(BASE_DIR, 'data', 'bronze')
    INPUT_DIR = os.path.join(BASE_DIR, 'data', 'input')
    LOG_DIR = os.path.join(BASE_DIR, 'logs')
    
    # Setup Logging
    logger = setup_logging(log_dir=LOG_DIR)
    logger.info("Iniciando processo ETL...")
    
    # 1. Funds Explorer Scraper
    logger.info("=== Iniciando Funds Explorer Scraper ===")
    try:
        funds_scraper = FundsExplorerScraper(headless=False)
        funds_scraper.scrape_all(BRONZE_DIR)
    except Exception as e:
        logger.error(f"Erro no Funds Explorer Scraper: {e}", exc_info=True)
    
    # Carregar tickers descobertos
    funds_ativos_path = os.path.join(BRONZE_DIR, 'funds_ativos.csv')
    if os.path.exists(funds_ativos_path):
        df_funds = pd.read_csv(funds_ativos_path)
        tickers_funds = df_funds['Ticker'].tolist()
    else:
        tickers_funds = []
        logger.warning(f"Arquivo {funds_ativos_path} não encontrado. Nenhum ticker de FIIs carregado.")
        
    # Carregar tickers manuais
    manual_tickers = load_manual_tickers(os.path.join(INPUT_DIR, 'possiveis ativos.txt'))
    
    # Unificar lista de tickers
    all_tickers = list(set(tickers_funds + manual_tickers))
    logger.info(f"Total de tickers para processar: {len(all_tickers)}")
    
    # Criar DataFrame único para passar para os scrapers
    df_all_tickers = pd.DataFrame({'Ticker': all_tickers})
    
    # 3. Yahoo Finance Scraper
    logger.info("=== Iniciando Yahoo Finance Scraper ===")
    try:
        yahoo_scraper = YahooFinanceScraper()
        yahoo_scraper.scrape_all(all_tickers, BRONZE_DIR)
    except Exception as e:
         logger.error(f"Erro no Yahoo Finance Scraper: {e}", exc_info=True)
    
    # 4. Loader (Bronze -> Silver)
    logger.info("=== Iniciando Carga para Banco de Dados ===")
    try:
        load_bronze_to_silver(BRONZE_DIR)
    except Exception as e:
        logger.error(f"Erro no Loader: {e}", exc_info=True)
    
    logger.info("=== Processo ETL Finalizado ===")

if __name__ == "__main__":
    main()
