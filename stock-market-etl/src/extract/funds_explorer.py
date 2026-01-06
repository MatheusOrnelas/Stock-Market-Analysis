import cloudscraper
from bs4 import BeautifulSoup
import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from time import sleep
import os
from src.config.logging_config import get_logger

logger = get_logger(__name__)

class FundsExplorerScraper:
    def __init__(self, headless=True, user_data_dir=None):
        self.headless = headless
        self.user_data_dir = user_data_dir
        self.scraper = cloudscraper.create_scraper()
        self.driver = None
        self._init_driver()
        self.base_url = 'https://www.fundsexplorer.com.br/funds/'
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
        
        # Otimizações para evitar travamentos
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--disable-extensions")
        chrome_options.add_argument("--disable-infobars")
        chrome_options.add_argument("--start-maximized")
        chrome_options.add_argument("--disable-notifications")
        chrome_options.add_argument('--log-level=3')  # Suprimir logs
        
        if self.user_data_dir:
            chrome_options.add_argument(f"user-data-dir={self.user_data_dir}")

        self.driver_service = Service(ChromeDriverManager().install())
        self.driver = webdriver.Chrome(service=self.driver_service, options=chrome_options)
        self.driver.set_page_load_timeout(30) # Timeout menor para evitar hang
        logger.debug("WebDriver inicializado.")

    def get_ativos_imobiliarios(self):
        url = 'https://www.fundsexplorer.com.br/funds'
        logger.info(f"Buscando lista de ativos em: {url}")
        try:
            response = self.scraper.get(url)
            soup = BeautifulSoup(response.text, 'html.parser')

            data = []
            tickers_found = soup.find_all('div', class_='tickerBox link-tickers-container')
            logger.info(f"Encontrados {len(tickers_found)} ativos na página principal.")

            for ticker_box in tickers_found:
                tipo = ticker_box.find('span', class_='tickerBox__type').text.strip() if ticker_box.find('span', class_='tickerBox__type') else '-'
                ticker = ticker_box.find('div', class_='tickerBox__title').text.strip() if ticker_box.find('div', class_='tickerBox__title') else '-'
                descricao = ticker_box.find('div', class_='tickerBox__desc').text.strip() if ticker_box.find('div', class_='tickerBox__desc') else '-'
                info_boxes = ticker_box.find_all('div', class_='tickerBox__info__box')
                primeira_info = info_boxes[0].text.strip() if len(info_boxes) > 0 else '-'
                segunda_info = info_boxes[1].text.strip() if len(info_boxes) > 1 else '-'
                
                data.append([tipo, ticker, descricao, primeira_info, segunda_info])

            return pd.DataFrame(data, columns=['Tipo', 'Ticker', 'Descrição', 'DY (%)', 'PL (R$)'])
        except Exception as e:
            logger.error(f"Erro ao buscar ativos imobiliários: {e}", exc_info=True)
            return pd.DataFrame()

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

    def get_rendimentos(self, ticker):
        url = f'https://www.fundsexplorer.com.br/rendimentos-e-amortizacoes?ticker={ticker}'
        try:
            soup = self._get_soup(url)
            table = soup.find('tbody', {'class': 'default-fiis-table__container__table__body skeleton-content'})
            
            if not table:
                logger.debug(f"Tabela de rendimentos não encontrada para {ticker}")
                return pd.DataFrame()
            
            header_el = soup.find('thead').find_all('th')
            header = [col.text.strip() for col in header_el]

            data = []
            rows = table.find_all('tr')
            for row in rows:
                cols = row.find_all('td')
                cols = [ele.text.strip() for ele in cols]
                if len(cols) == len(header):
                    data.append([ticker] + cols)
                else:
                    logger.warning(f"Discrepância de colunas em rendimentos para {ticker}. Esperado {len(header)}, recebido {len(cols)}")

            if not data:
                return pd.DataFrame()

            final_columns = ['Ticker'] + header
            
            if len(data[0]) != len(final_columns):
                 return pd.DataFrame()

            return pd.DataFrame(data, columns=final_columns)
            
        except Exception as e:
            logger.error(f"Erro ao pegar rendimentos de {ticker}: {e}", exc_info=True)
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
                
                endereco = items[0].find('b').next_sibling.strip() if len(items) > 0 and items[0].find('b') and items[0].find('b').next_sibling else ''
                bairro = items[1].find('b').next_sibling.strip() if len(items) > 1 and items[1].find('b') and items[1].find('b').next_sibling else ''
                cidade = items[2].find('b').next_sibling.strip() if len(items) > 2 and items[2].find('b') and items[2].find('b').next_sibling else ''
                area_bruta_locavel = items[3].find('b').next_sibling.strip() if len(items) > 3 and items[3].find('b') and items[3].find('b').next_sibling else ''
                
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

    def scrape_all(self, output_dir):
        os.makedirs(output_dir, exist_ok=True)
        
        logger.info("Coletando lista de ativos...")
        df_ativos = self.get_ativos_imobiliarios()
        df_ativos.to_csv(os.path.join(output_dir, 'funds_ativos.csv'), index=False)
        logger.info(f"Lista de ativos salva em: {os.path.join(output_dir, 'funds_ativos.csv')}")
        
        dict_dfs = {
            "funds_rendimentos": pd.DataFrame(),
            "funds_info_completa": pd.DataFrame(),
            "funds_simulacao": pd.DataFrame(),
            "funds_localizacao": pd.DataFrame()
        }

        total = len(df_ativos)
        restart_frequency = 50 

        for idx, row in df_ativos.iterrows():
            ticker = row['Ticker']
            logger.info(f"[{idx+1}/{total}] Processando {ticker}...")
            
            if (idx + 1) % restart_frequency == 0:
                logger.info("Reiniciando driver para limpar memória...")
                self._init_driver()

            try:
                url = self.base_url + ticker
                soup = self._get_soup(url)
                
                if soup:
                    df_info = self.get_info_completa(soup, ticker)
                    if not df_info.empty:
                        dict_dfs["funds_info_completa"] = pd.concat([dict_dfs["funds_info_completa"], df_info], ignore_index=True)

                    df_sim = self.get_simulacao(soup, ticker)
                    if not df_sim.empty:
                        dict_dfs["funds_simulacao"] = pd.concat([dict_dfs["funds_simulacao"], df_sim], ignore_index=True)

                    df_loc = self.get_localizacao(soup, ticker)
                    if not df_loc.empty:
                        dict_dfs["funds_localizacao"] = pd.concat([dict_dfs["funds_localizacao"], df_loc], ignore_index=True)

                    df_rend = self.get_rendimentos(ticker)
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
