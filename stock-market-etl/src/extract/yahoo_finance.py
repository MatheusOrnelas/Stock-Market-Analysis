import yfinance as yf
import pandas as pd
import re
import os
from src.config.logging_config import get_logger

# Configurar logger da biblioteca yfinance para WARNING para reduzir ruído
import logging
logging.getLogger('yfinance').setLevel(logging.WARNING)

logger = get_logger(__name__)

class YahooFinanceScraper:
    def __init__(self):
        self.intervalos = ['1m', '2m', '5m', '15m', '30m', '60m', '90m', '1d', '5d', '1wk', '1mo', '3mo']
        logger.info(f"YahooFinanceScraper inicializado. Intervalos: {self.intervalos}")

    def buscar_cotacao_por_intervalo_variantes(self, ticker_original, intervalo):
        # Criar as variantes do ticker seguindo a lógica definida
        variantes = [
            ticker_original + '.SA',  # Exemplo: ANCR11B.SA
            ticker_original,          # Exemplo: ANCR11B
            re.sub(r'[A-Za-z]+$', '', ticker_original),  # Remover letras após o 11, Exemplo: ANCR11
            re.sub(r'[A-Za-z]+$', '', ticker_original) + '.SA'  # Exemplo: ANCR11.SA
        ]
        
        # Iterar sobre as variantes e tentar buscar os dados
        for ticker_modificado in variantes:
            try:
                ativo = yf.Ticker(ticker_modificado)
                # Usa 'max' para pegar histórico completo
                historico = ativo.history(period="max", interval=intervalo)
                
                if not historico.empty:
                    logger.debug(f"Dados capturados para {ticker_modificado} no intervalo {intervalo} ({len(historico)} registros)")
                    historico['Ticker'] = ticker_original  
                    historico['Ticker_Yahoo'] = ticker_modificado 
                    historico['Intervalo'] = intervalo 
                    # Reset index to keep 'Date' or 'Datetime' as column
                    historico = historico.reset_index()
                    return historico[['Date', 'Ticker', 'Ticker_Yahoo', 'Open', 'High', 'Low', 'Close', 'Volume', 'Intervalo']] if 'Date' in historico.columns else historico[['Datetime', 'Ticker', 'Ticker_Yahoo', 'Open', 'High', 'Low', 'Close', 'Volume', 'Intervalo']]
            except Exception as e:
                # Log debug se falhar, mas continua tentando variantes
                # logger.debug(f"Falha ao buscar {ticker_modificado}: {e}")
                pass
        return None

    def obter_historicos_ativos(self, lista_tickers):
        todos_historicos = []
        total = len(lista_tickers)
        
        for idx, ticker_original in enumerate(lista_tickers):
            # Log apenas a cada 10 ativos ou o primeiro para reduzir ruído no log principal
            if idx == 0 or (idx + 1) % 10 == 0:
                logger.info(f"[{idx+1}/{total}] Processando... (Atual: {ticker_original})")
            
            # Iterar sobre cada intervalo e buscar os dados
            for intervalo in self.intervalos:
                historico = self.buscar_cotacao_por_intervalo_variantes(ticker_original, intervalo)
                
                if historico is not None:
                    todos_historicos.append(historico)
        
        if todos_historicos:
            historico_df = pd.concat(todos_historicos, ignore_index=True)
            return historico_df
        else:
            return pd.DataFrame()

    def scrape_all(self, lista_tickers, output_dir):
        os.makedirs(output_dir, exist_ok=True)
        
        logger.info("Iniciando coleta do Yahoo Finance...")
        historicos_df = self.obter_historicos_ativos(lista_tickers)
        
        if not historicos_df.empty:
            # Remover linhas com valores nulos nas colunas principais
            historicos_df_clean = historicos_df.dropna(subset=['Open', 'High', 'Low', 'Close'])
            # Remover duplicados
            historicos_df_clean = historicos_df_clean.drop_duplicates()
            
            output_path = os.path.join(output_dir, 'yahoo_cotacoes.csv')
            historicos_df_clean.to_csv(output_path, index=False)
            logger.info(f"Histórico Yahoo salvo em: {output_path} ({len(historicos_df_clean)} registros)")
        else:
            logger.warning("Nenhum dado Yahoo capturado.")
