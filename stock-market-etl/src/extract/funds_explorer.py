import cloudscraper
from bs4 import BeautifulSoup
import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from time import sleep
import os
import re
from datetime import datetime
from src.config.logging_config import get_logger

logger = get_logger(__name__)

class FundsExplorerScraper:
    def __init__(self, headless=True, user_data_dir=None):
        self.headless = headless
        self.user_data_dir = user_data_dir
        self.scraper = cloudscraper.create_scraper()
        self.driver = None
        self._init_driver()
        self.base_url = 'https://www.fundsexplorer.com.br'
        logger.info(f"FundsExplorerScraper inicializado (Headless: {headless})")

    def _init_driver(self):
        """Inicializa ou reinicializa o driver do Chrome."""
        if self.driver:
            try:
                self.driver.quit()
            except:
                pass
        
        chrome_options = Options()
        if self.headless:
            chrome_options.add_argument("--headless")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        
        # Otimizações
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--disable-extensions")
        chrome_options.add_argument("--disable-infobars")
        chrome_options.add_argument("--start-maximized")
        chrome_options.add_argument("--disable-notifications")
        chrome_options.add_argument('--log-level=3')
        
        if self.user_data_dir:
            chrome_options.add_argument(f"user-data-dir={self.user_data_dir}")

        self.driver_service = Service(ChromeDriverManager().install())
        self.driver = webdriver.Chrome(service=self.driver_service, options=chrome_options)
        self.driver.set_page_load_timeout(30)
        logger.debug("WebDriver inicializado.")

    def get_ativos_imobiliarios(self):
        urls = [
            f'{self.base_url}/funds',
            f'{self.base_url}/fiagros',
            f'{self.base_url}/fiinfras'
        ]
        
        all_data = []
        
        for url in urls:
            logger.info(f"Buscando lista de ativos em: {url}")
            try:
                self.driver.get(url)
                sleep(3)
                # Scroll para garantir carregamento (embora muitos usem paginação, o site atual parece listar tudo ou usar scroll infinito)
                last_height = self.driver.execute_script("return document.body.scrollHeight")
                while True:
                    self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                    sleep(2)
                    new_height = self.driver.execute_script("return document.body.scrollHeight")
                    if new_height == last_height:
                        break
                    last_height = new_height
                
                page_source = self.driver.page_source
                soup = BeautifulSoup(page_source, 'html.parser')

                tickers_found = soup.find_all('div', class_='tickerBox link-tickers-container')
                logger.info(f"Encontrados {len(tickers_found)} ativos em {url}.")

                for ticker_box in tickers_found:
                    tipo = ticker_box.find('span', class_='tickerBox__type').text.strip() if ticker_box.find('span', class_='tickerBox__type') else '-'
                    ticker = ticker_box.find('div', class_='tickerBox__title').text.strip() if ticker_box.find('div', class_='tickerBox__title') else '-'
                    descricao = ticker_box.find('div', class_='tickerBox__desc').text.strip() if ticker_box.find('div', class_='tickerBox__desc') else '-'
                    info_boxes = ticker_box.find_all('div', class_='tickerBox__info__box')
                    primeira_info = info_boxes[0].text.strip() if len(info_boxes) > 0 else '-'
                    segunda_info = info_boxes[1].text.strip() if len(info_boxes) > 1 else '-'
                    
                    categoria = 'FII'
                    if 'fiagros' in url:
                        categoria = 'FIAGRO'
                    elif 'fiinfras' in url:
                        categoria = 'FIINFRA'
                    
                    all_data.append([categoria, tipo, ticker, descricao, primeira_info, segunda_info])
            except Exception as e:
                logger.error(f"Erro ao buscar ativos em {url}: {e}", exc_info=True)

        df = pd.DataFrame(all_data, columns=['Categoria', 'Tipo', 'Ticker', 'Descrição', 'DY (%)', 'PL (R$)'])
        df = df.drop_duplicates(subset=['Ticker'])
        
        logger.info(f"Total de ativos únicos encontrados: {len(df)}")
        return df

    def _save_raw_page(self, *, output_dir: str, endpoint: str, ticker: str) -> tuple[str, str]:
        """
        Salva o HTML bruto da página do ativo.
        Retorna (url, filepath).
        """
        url = f"{self.base_url}/{endpoint}/{ticker.lower()}"
        raw_dir = os.path.join(output_dir, "raw_pages", endpoint)
        os.makedirs(raw_dir, exist_ok=True)
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        filepath = os.path.join(raw_dir, f"{ticker.lower()}_{ts}.html")
        try:
            html = self.driver.page_source
            with open(filepath, "w", encoding="utf-8") as f:
                f.write(html)
            return url, filepath
        except Exception as e:
            logger.warning(f"Falha ao salvar HTML bruto para {ticker} ({endpoint}): {e}")
            return url, ""

    def _get_soup(self, url, retry_count=0):
        try:
            self.driver.get(url)
            self.driver.implicitly_wait(5)
            sleep(2) 
            page_source = self.driver.page_source
            return BeautifulSoup(page_source, 'html.parser')
        except Exception as e:
            if retry_count < 2:
                logger.warning(f"Erro ao carregar {url} (tentativa {retry_count+1}): {e}. Reiniciando driver...")
                self._init_driver()
                return self._get_soup(url, retry_count + 1)
            else:
                logger.error(f"Falha ao carregar {url} após tentativas.")
                raise e

    def get_header_indicators(self, soup, ticker):
        """
        Extrai os indicadores do topo da página (Preço, Liquidez, DY, P/VP, etc.)
        """
        indicators = {"Ticker": ticker, "timestamp": datetime.now()}
        
        try:
            # 1. Preço e Variação (Header principal)
            price_div = soup.find('div', class_='headerTicker__content__price')
            if price_div:
                price_p = price_div.find('p')
                indicators['preco_atual'] = price_p.text.strip().replace('R$', '').replace('.', '').replace(',', '.').strip() if price_p else None
            
            # 2. Caixas de Indicadores (Carrossel ou Grid)
            # A classe pode variar, procurando padrão geral de caixas
            indicator_boxes = soup.find_all('div', class_='indicators__box')
            
            for box in indicator_boxes:
                title_elem = box.find('p')
                value_elem = box.find('b') or box.find('p', class_='indicators__box__value') # Fallback para outra estrutura
                
                if title_elem and value_elem:
                    title = title_elem.text.strip().lower()
                    value = value_elem.text.strip()
                    
                    # Limpeza básica de valores
                    clean_value = value.replace('R$', '').replace('%', '').replace('.', '').replace(',', '.').strip()
                    
                    # Tratar abreviações (K, M, B)
                    try:
                        if 'k' in clean_value.lower():
                            clean_value = float(clean_value.lower().replace('k', '')) * 1000
                        elif 'm' in clean_value.lower():
                            clean_value = float(clean_value.lower().replace('m', '')) * 1000000
                        elif 'b' in clean_value.lower():
                            clean_value = float(clean_value.lower().replace('b', '')) * 1000000000
                    except:
                        pass # Mantém o valor como string se falhar conversão
                    
                    # Mapeamento para nomes de colunas
                    if 'liquidez' in title:
                        indicators['liquidez_media_diaria'] = clean_value
                    elif 'último rendimento' in title:
                        indicators['ultimo_rendimento'] = clean_value
                    elif 'dividend yield' in title:
                        indicators['dy_12m'] = clean_value
                    elif 'patrimônio líquido' in title:
                        indicators['patrimonio_liquido'] = clean_value
                    elif 'valor patrimonial' in title:
                        indicators['valor_patrimonial_cota'] = clean_value
                    elif 'rentab. no mês' in title:
                        indicators['rentabilidade_mes'] = clean_value
                    elif 'p/vp' in title:
                        indicators['p_vp'] = clean_value

            return pd.DataFrame([indicators])
        except Exception as e:
            logger.warning(f"Erro ao extrair indicadores de cabeçalho para {ticker}: {e}")
            return pd.DataFrame()

    def get_dividend_history_full(self, soup, ticker):
        """
        Extrai a tabela completa de histórico de dividendos.
        """
        try:
            # Procura tabelas na página
            tables = soup.find_all('table')
            target_table = None
            
            # Heurística para encontrar a tabela certa (contém 'Data com' ou 'Pagamento')
            for table in tables:
                headers = [th.text.strip() for th in table.find_all('th')]
                if 'Data com' in headers and 'Pagamento' in headers:
                    target_table = table
                    break
            
            if not target_table:
                return pd.DataFrame()

            data = []
            rows = target_table.find_all('tr')
            
            # Pega headers da tabela encontrada
            headers = [th.text.strip() for th in target_table.find_all('th')]
            
            for row in rows:
                cols = row.find_all('td')
                cols = [ele.text.strip() for ele in cols]
                if len(cols) == len(headers):
                    data.append([ticker] + cols)
            
            if data:
                final_columns = ['Ticker'] + headers
                return pd.DataFrame(data, columns=final_columns)
                
            return pd.DataFrame()
        except Exception as e:
            logger.warning(f"Erro ao extrair histórico de dividendos para {ticker}: {e}")
            return pd.DataFrame()

    def get_info_completa(self, soup, ticker):
        info_basicas = {"Ticker": ticker}

        try:
            basic_info_section = soup.find('section', {'id': 'carbon_fields_fiis_basic_informations-2'})
            if basic_info_section:
                grid_boxes = basic_info_section.find_all('div', class_='basicInformation__grid__box')
                for box in grid_boxes:
                    label = box.find('p').text.strip() if box.find('p') else ''
                    value = box.find('p').find_next('p').text.strip() if box.find('p') and box.find('p').find_next('p') else ''
                    if label:
                        info_basicas[label] = value

            descricao_section = soup.find('section', {'id': 'carbon_fields_fiis_description-3'})
            descricao = descricao_section.find('div', class_='newsContent').get_text(separator="\n").strip() if descricao_section and descricao_section.find('div', class_='newsContent') else ''
            info_basicas["Descrição"] = descricao

            admin_section = soup.find('div', class_='administration')
            if admin_section:
                admin_name = admin_section.find('p', class_='informations__adm__name').text.strip() if admin_section.find('p', class_='informations__adm__name') else ''
                admin_doc = admin_section.find('span', class_='informations__adm__doc').text.strip() if admin_section.find('span', class_='informations__adm__doc') else ''
                
                admin_tel_div = admin_section.find('div', class_='informations__contact__tel')
                admin_tel = admin_tel_div.find('p').text.strip() if admin_tel_div and admin_tel_div.find('p') else ''
                
                admin_email_div = admin_section.find('div', class_='informations__contact__email')
                admin_email = admin_email_div.find('p').text.strip() if admin_email_div and admin_email_div.find('p') else ''
                
                admin_site_div = admin_section.find('div', class_='informations__contact__site')
                admin_site = admin_site_div.find('p').text.strip() if admin_site_div and admin_site_div.find('p') else ''

                info_basicas["Administrador"] = admin_name
                info_basicas["CNPJ Administrador"] = admin_doc
                info_basicas["Telefone Administrador"] = admin_tel
                info_basicas["Email Administrador"] = admin_email
                info_basicas["Site Administrador"] = admin_site

            info_basicas["Link de Acesso"] = self.base_url + ticker 

            df_info_completa = pd.DataFrame(list(info_basicas.items()), columns=['Campo', 'Valor'])

            df_info_pivot = df_info_completa.set_index('Campo').T
            df_info_pivot['Ticker'] = ticker

            return df_info_pivot
        except Exception:
            return pd.DataFrame()

    def get_simulacao(self, soup, ticker):
        try:
            simulation_data = []
            simulation_div = soup.find('div', class_='simulation')
            simulation_boxes = simulation_div.find_all('div', class_='simulation__box') if simulation_div else []

            for box in simulation_boxes:
                descricao = box.find('p').text.strip() if box.find('p') else ''
                valor = box.find('b').text.strip() if box.find('b') else ''
                simulation_data.append([ticker, descricao, valor])

            if not simulation_data:
                return pd.DataFrame()

            return pd.DataFrame(simulation_data, columns=['Ticker', 'Descrição', 'Valor'])
        except Exception:
            return pd.DataFrame()

    def get_localizacao(self, soup, ticker):
        try:
            locations_data = []
            slides = soup.find_all('div', class_='swiper-slide')

            for slide in slides:
                title_div = slide.find('div', class_='locationGrid__title')
                title = title_div.text.strip() if title_div else ''
                items = slide.find_all('li')
                
                if len(items) >= 4:
                    endereco = items[0].find('b').next_sibling.strip() if items[0].find('b') and items[0].find('b').next_sibling else ''
                    bairro = items[1].find('b').next_sibling.strip() if items[1].find('b') and items[1].find('b').next_sibling else ''
                    cidade = items[2].find('b').next_sibling.strip() if items[2].find('b') and items[2].find('b').next_sibling else ''
                    area_bruta_locavel = items[3].find('b').next_sibling.strip() if items[3].find('b') and items[3].find('b').next_sibling else ''
                    
                    location_info = {
                        "Ticker": ticker,
                        "Title": title,
                        "Endereço": endereco,
                        "Bairro": bairro,
                        "Cidade": cidade,
                        "Área Bruta Locável": area_bruta_locavel
                    }
                    
                    locations_data.append(location_info)
            
            if not locations_data:
                 return pd.DataFrame()

            return pd.DataFrame(locations_data)
        except Exception:
            return pd.DataFrame()

    def get_rendimentos_old(self, ticker):
        """Método antigo de rendimentos como fallback, caso necessário"""
        url = f'https://www.fundsexplorer.com.br/rendimentos-e-amortizacoes?ticker={ticker}'
        try:
            soup = self._get_soup(url)
            table = soup.find('tbody', {'class': 'default-fiis-table__container__table__body skeleton-content'})
            if not table: return pd.DataFrame()
            header = [col.text.strip() for col in soup.find('thead').find_all('th')]
            data = []
            for row in table.find_all('tr'):
                cols = [ele.text.strip() for ele in row.find_all('td')]
                if len(cols) == len(header): data.append([ticker] + cols)
            if data: return pd.DataFrame(data, columns=['Ticker'] + header)
            return pd.DataFrame()
        except: return pd.DataFrame()

    def scrape_all(self, output_dir):
        os.makedirs(output_dir, exist_ok=True)
        
        logger.info("Coletando lista de ativos (FIIs, Fiagros, Fiinfras)...")
        df_ativos = self.get_ativos_imobiliarios()
        df_ativos.to_csv(os.path.join(output_dir, 'funds_ativos.csv'), index=False)
        logger.info(f"Lista de ativos salva em: {os.path.join(output_dir, 'funds_ativos.csv')}")
        
        dict_dfs = {
            "funds_rendimentos": pd.DataFrame(),
            "funds_info_completa": pd.DataFrame(),
            "funds_simulacao": pd.DataFrame(),
            "funds_localizacao": pd.DataFrame(),
            "funds_indicadores_diarios": pd.DataFrame(), # Nova tabela para dados voláteis
            "funds_page_snapshots": pd.DataFrame(),      # Nova tabela para rastrear HTML bruto
        }

        total = len(df_ativos)
        restart_frequency = 50 

        for idx, row in df_ativos.iterrows():
            ticker = row['Ticker']
            categoria = row.get('Categoria', 'FII')
            logger.info(f"[{idx+1}/{total}] Processando {ticker} ({categoria})...")
            
            if (idx + 1) % restart_frequency == 0:
                logger.info("Reiniciando driver para limpar memória...")
                self._init_driver()

            try:
                endpoint = 'funds'
                if categoria == 'FIAGRO':
                    endpoint = 'fiagros'
                elif categoria == 'FIINFRA':
                    endpoint = 'fiinfras'
                
                url = f'{self.base_url}/{endpoint}/{ticker.lower()}'
                soup = self._get_soup(url)
                
                if soup:
                    # Salvar HTML bruto da página para auditoria/histórico
                    page_url, html_path = self._save_raw_page(output_dir=output_dir, endpoint=endpoint, ticker=ticker)
                    snap_row = pd.DataFrame([{
                        "Ticker": ticker,
                        "Categoria": categoria,
                        "endpoint": endpoint,
                        "url": page_url,
                        "html_path": html_path,
                        "fetched_at": datetime.now(),
                    }])
                    dict_dfs["funds_page_snapshots"] = pd.concat([dict_dfs["funds_page_snapshots"], snap_row], ignore_index=True)

                    # Header Indicators (Novo)
                    df_header = self.get_header_indicators(soup, ticker)
                    if not df_header.empty:
                        dict_dfs["funds_indicadores_diarios"] = pd.concat([dict_dfs["funds_indicadores_diarios"], df_header], ignore_index=True)

                    # Info Completa
                    df_info = self.get_info_completa(soup, ticker)
                    if not df_info.empty:
                        dict_dfs["funds_info_completa"] = pd.concat([dict_dfs["funds_info_completa"], df_info], ignore_index=True)

                    # Simulação
                    df_sim = self.get_simulacao(soup, ticker)
                    if not df_sim.empty:
                        dict_dfs["funds_simulacao"] = pd.concat([dict_dfs["funds_simulacao"], df_sim], ignore_index=True)

                    # Localização
                    df_loc = self.get_localizacao(soup, ticker)
                    if not df_loc.empty:
                        dict_dfs["funds_localizacao"] = pd.concat([dict_dfs["funds_localizacao"], df_loc], ignore_index=True)

                    # Rendimentos (Agora tenta pegar da própria página ou variantes)
                    df_rend = self.get_dividend_history_full(soup, ticker)
                    if df_rend.empty:
                        df_rend = self.get_rendimentos_old(ticker)
                    
                    if not df_rend.empty:
                        dict_dfs["funds_rendimentos"] = pd.concat([dict_dfs["funds_rendimentos"], df_rend], ignore_index=True)

            except Exception as e:
                logger.error(f"Erro ao capturar dados para {ticker}: {e}")

        for name, df in dict_dfs.items():
            filepath = os.path.join(output_dir, f"{name}.csv")
            df.to_csv(filepath, index=False)
            logger.info(f"Salvo: {filepath} ({len(df)} registros)")

        self.driver.quit()

if __name__ == "__main__":
    from src.config.logging_config import setup_logging
    setup_logging()
    scraper = FundsExplorerScraper()
    scraper.scrape_all("stock-market-etl/data/bronze")
