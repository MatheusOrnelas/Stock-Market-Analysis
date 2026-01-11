import pandas as pd
import os
import sys
import numpy as np
import yfinance as yf # Importando yfinance para fallback de DY

# Caminho para os dados (relativo à execução ou estrutura do projeto)
# Assumindo que este script está em analise_de_Indicadores/
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR_BRONZE = os.path.join(PROJECT_ROOT, 'stock-market-etl', 'data', 'bronze')
DATA_DIR_OCEANS = os.path.join(PROJECT_ROOT, 'dados', 'oceans14_output_csvs')

class IndicadoresCalculator:
    def __init__(self, data_dir_bronze=DATA_DIR_BRONZE, data_dir_oceans=DATA_DIR_OCEANS):
        self.data_dir_bronze = data_dir_bronze
        self.data_dir_oceans = data_dir_oceans
        
        self.df_rendimentos = self._load_rendimentos()
        self.df_indicadores_atual = self._load_indicadores_atual()
        self.df_yahoo = self._load_yahoo()
        # Carregar Oceans (fallback)
        self.df_indicadores_oceans = self._load_oceans_indicadores()
        # Carregar Yahoo Histórico (prioridade)
        self.df_yahoo_hist = self._load_yahoo_historical()

    def _load_rendimentos(self):
        path = os.path.join(self.data_dir_bronze, 'funds_rendimentos.csv')
        if not os.path.exists(path):
            return pd.DataFrame()
        
        try:
            # Tentar ler com pandas default
            df = pd.read_csv(path)
            
            # Limpeza e Formatação
            cols_to_fix = ['Fechamento (R$)', 'Valor por Cota (R$)', 'Yield 1M', 'Yield 12M']
            for col in cols_to_fix:
                if col in df.columns:
                    # Remove pontos de milhar, troca vírgula por ponto
                    df[col] = df[col].astype(str).str.replace('.', '', regex=False).str.replace(',', '.', regex=False)
                    df[col] = pd.to_numeric(df[col], errors='coerce')
            
            # Converter datas
            if 'Data Base' in df.columns:
                df['Data Base'] = pd.to_datetime(df['Data Base'], format='%d/%m/%Y', errors='coerce')
            if 'Data de Pagamento' in df.columns:
                df['Data de Pagamento'] = pd.to_datetime(df['Data de Pagamento'], format='%d/%m/%Y', errors='coerce')
            
            # Limpeza Adicional: Remover linhas onde Valor por Cota é NaN
            df = df.dropna(subset=['Valor por Cota (R$)'])
            
            # Se Data Base falhar, tentar usar Data de Pagamento como proxy
            if 'Data Base' in df.columns and 'Data de Pagamento' in df.columns:
                df['Data Base'] = df['Data Base'].fillna(df['Data de Pagamento'])
                
            return df
        except Exception as e:
            print(f"Erro ao carregar rendimentos: {e}")
            return pd.DataFrame()

    def _load_indicadores_atual(self):
        path = os.path.join(self.data_dir_bronze, 'funds_indicadores_diarios.csv')
        if not os.path.exists(path):
            return pd.DataFrame()
        return pd.read_csv(path)

    def _load_yahoo(self):
        path = os.path.join(self.data_dir_bronze, 'yahoo_cotacoes.csv')
        if not os.path.exists(path):
            return pd.DataFrame()
        df = pd.read_csv(path)
        if 'Date' in df.columns:
            df['Date'] = pd.to_datetime(df['Date'], utc=True).dt.tz_localize(None)
        elif 'Datetime' in df.columns: # Yahoo as vezes retorna Datetime
             df['Date'] = pd.to_datetime(df['Datetime'], utc=True).dt.tz_localize(None)
        return df

    def _load_yahoo_historical(self):
        """Carrega os dados históricos extraídos recentemente do Yahoo (Equity/PL)"""
        path = os.path.join(self.data_dir_bronze, 'yahoo_historical_indicators.csv')
        if not os.path.exists(path):
            return pd.DataFrame()
        try:
            df = pd.read_csv(path)
            if 'Date' in df.columns:
                df['Date'] = pd.to_datetime(df['Date']).dt.tz_localize(None)
            return df
        except Exception as e:
            print(f"Erro ao carregar Yahoo Histórico: {e}")
            return pd.DataFrame()

    def _load_oceans_indicadores(self):
        """
        Carrega o histórico trimestral de indicadores (P/VP, VP por cota) da Oceans14.
        """
        path = os.path.join(self.data_dir_oceans, 'Indicadores.csv')
        if not os.path.exists(path):
            return pd.DataFrame()

        try:
            df = pd.read_csv(path)
            
            # Limpar valores numéricos
            cols_numeric = ['P/VP', 'VP por cota']
            for col in cols_numeric:
                if col in df.columns:
                    df[col] = df[col].astype(str).str.replace('.', '', regex=False).str.replace(',', '.', regex=False)
                    df[col] = pd.to_numeric(df[col], errors='coerce')

            # Converter Trimestre (1T2023) para Data
            def parse_trimestre(t_str):
                try:
                    if not isinstance(t_str, str) or 'T' not in t_str: return pd.NaT
                    quarter, year = t_str.split('T')
                    # Definir fim do trimestre
                    month_map = {'1': 3, '2': 6, '3': 9, '4': 12}
                    last_day_map = {'1': 31, '2': 30, '3': 30, '4': 31}
                    
                    if quarter in month_map:
                        return pd.Timestamp(year=int(year), month=month_map[quarter], day=last_day_map[quarter])
                    return pd.NaT
                except:
                    return pd.NaT

            if 'Trimestre' in df.columns:
                df['Date'] = df['Trimestre'].apply(parse_trimestre)
                df = df.dropna(subset=['Date'])
            
            return df
        except Exception as e:
            print(f"Erro ao carregar Oceans14 Indicadores: {e}")
            return pd.DataFrame()

    def get_dy_history(self, ticker):
        """
        Calcula o histórico de Dividend Yield (Mensal).
        Tenta usar dados locais (Funds Explorer). Se falhar, tenta Yahoo Finance ao vivo.
        Retorna DataFrame com [Date, Dividend, Price_at_Database, DY_Monthly]
        """
        # 1. Tentar dados locais
        result = pd.DataFrame()
        if not self.df_rendimentos.empty:
            df = self.df_rendimentos[self.df_rendimentos['Ticker'] == ticker].copy()
            if not df.empty:
                df = df.sort_values('Data Base')
                cols_map = {
                    'Data Base': 'Date',
                    'Valor por Cota (R$)': 'Dividend',
                    'Fechamento (R$)': 'Price_at_Database',
                    'Yield 1M': 'DY_Monthly'
                }
                existing_cols = [c for c in cols_map.keys() if c in df.columns]
                result = df[existing_cols].copy()
                result = result.rename(columns=cols_map)
                result = result.dropna(subset=['Date'])
                
                # Calcular se necessário
                if 'DY_Monthly' not in result.columns and 'Dividend' in result.columns and 'Price_at_Database' in result.columns:
                     result['DY_Monthly'] = np.where(
                         result['Price_at_Database'] > 0, 
                         (result['Dividend'] / result['Price_at_Database']) * 100, 
                         0
                     )
        
        # 2. Se local falhou (vazio), tenta Yahoo Finance
        if result.empty:
            try:
                result = self._get_dy_history_from_yahoo(ticker)
            except Exception as e:
                print(f"Erro no fallback do Yahoo para {ticker}: {e}")

        return result.sort_values('Date') if not result.empty else pd.DataFrame()

    def _get_dy_history_from_yahoo(self, ticker):
        """
        Busca histórico de dividendos e preços do Yahoo Finance para calcular DY.
        """
        ticker_sa = ticker if ticker.endswith('.SA') else f"{ticker}.SA"
        stock = yf.Ticker(ticker_sa)
        
        # Histórico de Dividendos
        dividends = stock.dividends
        if dividends.empty:
            return pd.DataFrame()
        
        # Converter Series para DataFrame
        df_divs = dividends.reset_index()
        df_divs.columns = ['Date', 'Dividend']
        df_divs['Date'] = df_divs['Date'].dt.tz_localize(None)
        
        # Histórico de Preços (para o dia ex-dividendo ou próximo)
        # Pegamos o histórico completo para garantir cobertura
        history = stock.history(period="max")
        if history.empty:
            return pd.DataFrame()
        
        df_prices = history.reset_index()[['Date', 'Close']]
        df_prices['Date'] = df_prices['Date'].dt.tz_localize(None)
        
        # Merge (procurando preço na data do dividendo)
        # Usamos merge_asof para pegar o preço mais próximo ANTERIOR ou IGUAL à data do dividendo
        df_divs = df_divs.sort_values('Date')
        df_prices = df_prices.sort_values('Date')
        
        merged = pd.merge_asof(df_divs, df_prices, on='Date', direction='backward')
        
        # Calcular DY Mensal
        merged['Price_at_Database'] = merged['Close']
        merged['DY_Monthly'] = (merged['Dividend'] / merged['Price_at_Database']) * 100
        
        return merged[['Date', 'Dividend', 'Price_at_Database', 'DY_Monthly']].dropna()

    def get_pvp_history(self, ticker):
        """
        Calcula o histórico de P/VP DIÁRIO.
        Prioridade:
        1. Histórico Yahoo (se disponível e completo)
        2. Histórico Oceans14 (se disponível)
        3. VP Atual (Fallback)
        """
        # 1. Pegar histórico de Preço (Yahoo)
        if self.df_yahoo.empty:
            return pd.DataFrame()

        ticker_variants = [ticker, ticker + ".SA"]
        df_price = self.df_yahoo[self.df_yahoo['Ticker'].isin(ticker_variants)].copy()
        
        if df_price.empty:
            return pd.DataFrame()
        
        df_price = df_price.sort_values('Date').set_index('Date')

        # 2. Fonte de VP (Prioridade: Yahoo Histórico -> Oceans14 -> VP Atual)
        source_used = "current"
        df_vp_hist = pd.DataFrame()

        # Tentar Yahoo Histórico
        if not self.df_yahoo_hist.empty:
             df_yh = self.df_yahoo_hist[self.df_yahoo_hist['Ticker'] == ticker].copy()
             if not df_yh.empty:
                 # Se tem Cotas e PL, calcula VP/Cota
                 # Se não tem Cotas (comum no Yahoo), tenta pegar Cotas Atuais para estimar
                 if 'Cotas' not in df_yh.columns or df_yh['Cotas'].isnull().all():
                      current_shares = self._get_current_shares(ticker)
                      if current_shares:
                          df_yh['VP_Cota'] = df_yh['Patrimonio_Liquido'] / current_shares
                          source_used = "yahoo_hist_estimated_shares"
                 else:
                      source_used = "yahoo_hist"
                 
                 if 'VP_Cota' in df_yh.columns:
                     df_yh = df_yh.dropna(subset=['VP_Cota']).sort_values('Date').set_index('Date')
                     df_vp_hist = df_yh[['VP_Cota']].resample('D').ffill()

        # Fallback para Oceans14 se Yahoo falhou
        if df_vp_hist.empty and not self.df_indicadores_oceans.empty:
            df_oc = self.df_indicadores_oceans[self.df_indicadores_oceans['Ticker'] == ticker].copy()
            if not df_oc.empty:
                df_oc = df_oc.sort_values('Date').set_index('Date')
                df_vp_hist = df_oc[['VP por cota']].rename(columns={'VP por cota': 'VP_Cota'}).resample('D').ffill()
                source_used = "oceans14"

        # 3. Cruzamento
        if not df_vp_hist.empty:
            merged = df_price.join(df_vp_hist, how='left')
            merged['VP_Used'] = merged['VP_Cota'].ffill()
            
            # Preencher começo com VP Atual se necessário
            vp_atual = self._get_current_vp(ticker)
            if vp_atual:
                 merged['VP_Used'] = merged['VP_Used'].fillna(vp_atual)

            merged['P_VP'] = merged['Close'] / merged['VP_Used']
            merged = merged.reset_index()
            # Adicionar metadado da fonte usada (hacky way: adicionar ao primeiro registro ou retornar tupla)
            # Para simplificar o dashboard, vamos apenas retornar os dados
            return merged[['Date', 'Close', 'VP_Used', 'P_VP']]

        else:
            # Modo Legado: VP Atual Fixo
            vp = self._get_current_vp(ticker)
            if vp is None or pd.isna(vp) or vp == 0:
                return pd.DataFrame()

            df_price['VP_Used'] = vp
            df_price['P_VP'] = df_price['Close'] / vp
            df_price = df_price.reset_index()
            return df_price[['Date', 'Close', 'VP_Used', 'P_VP']]

    def get_pvp_history_monthly(self, ticker):
        """
        Retorna o histórico MENSAL do P/VP (fechamento do mês).
        """
        df_daily = self.get_pvp_history(ticker)
        if df_daily.empty:
            return pd.DataFrame()
        
        # Define Data como índice para resample
        df_daily = df_daily.set_index('Date')
        
        # Resample para fim de mês ('ME' no pandas novo, 'M' no antigo)
        # Pegamos o último valor de P/VP do mês
        try:
            df_monthly = df_daily.resample('ME').last()
        except:
            # Fallback para versões pandas antigas
            df_monthly = df_daily.resample('M').last()
        
        # Remove meses sem dados
        df_monthly = df_monthly.dropna(subset=['P_VP'])
        
        return df_monthly.reset_index()

    def _get_current_vp(self, ticker):
        if self.df_indicadores_atual.empty:
             return None
        df_ind = self.df_indicadores_atual[self.df_indicadores_atual['Ticker'] == ticker]
        if df_ind.empty:
            return None
        return df_ind.sort_values('timestamp', ascending=False).iloc[0]['valor_patrimonial_cota']

    def _get_current_shares(self, ticker):
        """Tenta inferir o número de cotas atual: PL / VP_Cota"""
        if self.df_indicadores_atual.empty:
             return None
        df_ind = self.df_indicadores_atual[self.df_indicadores_atual['Ticker'] == ticker]
        if df_ind.empty:
            return None
        
        row = df_ind.sort_values('timestamp', ascending=False).iloc[0]
        pl = row.get('patrimonio_liquido')
        vp = row.get('valor_patrimonial_cota')
        
        if pd.notnull(pl) and pd.notnull(vp) and vp != 0:
            return pl / vp
        return None

if __name__ == "__main__":
    calc = IndicadoresCalculator()
    ticker_test = "CPTR11" 
    print(f"--- DY History for {ticker_test} ---")
    df = calc.get_dy_history(ticker_test)
    print(df.tail())
