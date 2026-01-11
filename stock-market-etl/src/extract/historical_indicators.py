import yfinance as yf
import pandas as pd
import os
import sys

# Adiciona diretório raiz ao path
current_dir = os.path.dirname(os.path.abspath(__file__))
# current: stock-market-etl/src/extract
# parent 1: stock-market-etl/src
# parent 2: stock-market-etl
# parent 3: project_root
project_root = os.path.dirname(os.path.dirname(os.path.dirname(current_dir)))
sys.path.append(os.path.join(project_root, 'stock-market-etl'))

try:
    from src.config.logging_config import get_logger
except ImportError:
    # Se falhar, tenta adicionar o diretório src diretamente
    sys.path.append(os.path.join(project_root, 'stock-market-etl', 'src'))
    try:
        from config.logging_config import get_logger
    except ImportError:
         # Fallback final: logging básico se tudo falhar
        import logging
        def get_logger(name):
            logging.basicConfig(level=logging.INFO)
            return logging.getLogger(name)

logger = get_logger(__name__)

class HistoricalIndicatorsExtractor:
    def __init__(self, output_dir):
        self.output_dir = output_dir
        os.makedirs(self.output_dir, exist_ok=True)

    def get_historical_vp(self, ticker):
        """
        Tenta obter o histórico de Patrimônio Líquido e VP por cota via Yahoo Finance.
        Nota: Para FIIs brasileiros, o Yahoo muitas vezes tem dados limitados de balanço.
        """
        try:
            # Tentar sufixo .SA se não tiver
            ticker_sa = ticker if ticker.endswith('.SA') else f"{ticker}.SA"
            stock = yf.Ticker(ticker_sa)
            
            # Balanço Anual e Trimestral
            balance_sheet = stock.quarterly_balance_sheet
            if balance_sheet.empty:
                balance_sheet = stock.balance_sheet
            
            if balance_sheet.empty:
                logger.warning(f"Sem dados de balanço (VP) para {ticker} no Yahoo Finance.")
                return pd.DataFrame()

            # Transpor para ter datas nas linhas
            df_bs = balance_sheet.T
            
            # Buscar chaves comuns para Patrimônio Líquido
            # Keys possíveis: 'Total Stockholder Equity', 'Stockholders Equity', 'Total Assets' (menos passivo)
            equity_col = None
            possible_keys = ['Total Stockholder Equity', 'Stockholders Equity', 'Total Equity Gross Minority Interest']
            
            for key in possible_keys:
                if key in df_bs.columns:
                    equity_col = key
                    break
            
            if not equity_col:
                logger.warning(f"Coluna de Patrimônio Líquido não encontrada para {ticker}.")
                return pd.DataFrame()

            # Preparar DataFrame
            df_hist = df_bs[[equity_col]].copy()
            df_hist.columns = ['Patrimonio_Liquido']
            df_hist['Ticker'] = ticker
            df_hist = df_hist.reset_index().rename(columns={'index': 'Date'})
            
            # Tentar obter número de cotas para calcular VP por cota
            # O Yahoo nem sempre tem o histórico de "Share Issued" preciso trimestralmente na API pública
            # Vamos tentar usar o dado atual de 'sharesOutstanding' do info como aproximação ou buscar 'Share Issued' no balanço
            shares_col = None
            if 'Share Issued' in df_bs.columns:
                shares_col = 'Share Issued'
            elif 'Ordinary Shares Number' in df_bs.columns:
                shares_col = 'Ordinary Shares Number'
                
            if shares_col:
                df_hist['Cotas'] = df_bs[shares_col]
                df_hist['VP_Cota'] = df_hist['Patrimonio_Liquido'] / df_hist['Cotas']
            else:
                # Fallback: Usar cotas atuais (impreciso para passado distante, mas melhor que nada)
                info = stock.info
                current_shares = info.get('sharesOutstanding')
                if current_shares:
                    df_hist['Cotas'] = current_shares
                    df_hist['VP_Cota'] = df_hist['Patrimonio_Liquido'] / current_shares
                else:
                    df_hist['VP_Cota'] = None

            return df_hist
        except Exception as e:
            logger.error(f"Erro ao extrair histórico de VP para {ticker}: {e}")
            return pd.DataFrame()

    def process_tickers(self, tickers):
        all_data = []
        for ticker in tickers:
            logger.info(f"Processando histórico fundamentalista para {ticker}...")
            df = self.get_historical_vp(ticker)
            if not df.empty:
                all_data.append(df)
        
        if all_data:
            final_df = pd.concat(all_data, ignore_index=True)
            output_path = os.path.join(self.output_dir, 'yahoo_historical_indicators.csv')
            final_df.to_csv(output_path, index=False)
            logger.info(f"Dados históricos salvos em: {output_path}")
        else:
            logger.warning("Nenhum dado histórico fundamentalista encontrado.")

# --- Mapeamento de Alternativas ---
# Caso o Yahoo Finance não funcione (muito provável para FIIs), as alternativas são:
# 1. Funds Explorer (Charts):
#    A URL dos gráficos costuma ser algo como: https://www.fundsexplorer.com.br/funds/{ticker}/chart/pvp
#    É protegido por Cloudflare, necessitando do scraper com selenium/cloudscraper já existente.
#    Ação recomendada: Estender o `FundsExplorerScraper` para interceptar chamadas XHR ou ler o JSON embutido na página.
#
# 2. Status Invest:
#    Rico em dados, mas difícil de raspar.
#
# 3. Clube FII:
#    Dados bons, acesso pago para downloads em massa, mas visualização gratuita.

if __name__ == "__main__":
    # Teste
    extractor = HistoricalIndicatorsExtractor(output_dir="stock-market-etl/data/bronze")
    extractor.process_tickers(["MXRF11", "KNRI11"]) # Exemplos
