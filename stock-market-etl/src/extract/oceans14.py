import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import Select
from selenium.common.exceptions import NoSuchElementException, ElementClickInterceptedException, TimeoutException
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup
from time import sleep
import os
from src.config.logging_config import get_logger

logger = get_logger(__name__)

class Oceans14Scraper:
    def __init__(self, email, headless=True):
        self.email = email
        self.driver_service = Service(ChromeDriverManager().install())
        
        # Configuração do Chrome
        chrome_options = webdriver.ChromeOptions()
        if headless:
            chrome_options.add_argument("--headless")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        
        self.driver = webdriver.Chrome(service=self.driver_service, options=chrome_options)
        self.base_url = 'https://www.oceans14.com.br/fundos-imobiliarios/'
        self.dataframes = {
            "ocean_indicadores": pd.DataFrame(),
            "ocean_lucratividade": pd.DataFrame(),
            "ocean_balanco": pd.DataFrame(),
            "ocean_lista_fiis": pd.DataFrame(),
            "ocean_lista_imoveis": pd.DataFrame(),
            "ocean_lista_cri": pd.DataFrame()
        }
        self.login()
        logger.info("Oceans14Scraper inicializado.")

    def login(self):
        url_login = 'https://www.oceans14.com.br/usuarios/frmLogin.aspx'
        logger.info(f"Realizando login em {url_login}")
        self.driver.get(url_login)
        self.driver.implicitly_wait(10)

        try:
            email_input = self.driver.find_element(By.ID, "ctl00_conteudoPrincipal_txtLogin")
            email_input.send_keys(self.email)

            login_button = self.driver.find_element(By.ID, "ctl00_conteudoPrincipal_cmdEntrar")
            
            try:
                login_button.click()
            except ElementClickInterceptedException:
                self.driver.execute_script("arguments[0].click();", login_button)

            WebDriverWait(self.driver, 20).until(EC.url_changes(url_login))
            logger.info("Login realizado com sucesso.")
        except Exception as e:
            logger.error(f"Erro no login: {e}", exc_info=True)

    def _get_soup(self, url):
        self.driver.get(url)
        self.driver.implicitly_wait(10)
        sleep(3)

        # Verifica se a página contém "Página não encontrada"
        if "Página não encontrada" in self.driver.page_source:
            logger.warning(f"Página não encontrada em {url}. Pulando para a próxima iteração.")
            return None
        
        page_source = self.driver.page_source
        return BeautifulSoup(page_source, 'html.parser')

    def get_indicators(self, ticker):
        ticker_lower = ticker.lower()
        url = f'{self.base_url}{ticker_lower}'
        soup = self._get_soup(url)
        
        if soup is None:
            return

        try:
            apresentacao_dropdown = WebDriverWait(self.driver, 20).until(
                EC.presence_of_element_located((By.ID, 'apresentacaoQuadroIndicadores'))
            )
        except TimeoutException:
            logger.warning(f"Dropdown de indicadores não encontrado para {ticker}.")
            return

        apresentacao_dropdown = Select(apresentacao_dropdown)
        apresentacao_dropdown.select_by_visible_text('Trimestral')
        sleep(3)
        soup = BeautifulSoup(self.driver.page_source, 'html.parser')

        tables = soup.find_all('table')
        for table in tables:
            if table.find('tr', {'id': 'trIndicadores01Periodo'}):
                data = []
                header_row = table.find('tr', {'id': 'trIndicadores01Periodo'})
                headers = ['Indicador'] + [td.text.strip() for td in header_row.find_all('td')]

                for row in table.find_all('tr')[1:]:
                    label = row.find('th').text.strip()
                    values = [td.text.strip() for td in row.find_all('td')]
                    data.append([label] + values)

                df_indicadores = pd.DataFrame(data, columns=headers)
                df_indicadores['Ticker'] = ticker

                df_melted = pd.melt(df_indicadores, id_vars=["Ticker", "Indicador"], 
                                    var_name="Trimestre", value_name="Valor")

                df_pivoted = df_melted.pivot_table(index=["Ticker", "Trimestre"], 
                                                   columns="Indicador", values="Valor", 
                                                   aggfunc='first').reset_index()

                df_pivoted.columns.name = None
                self.dataframes["ocean_indicadores"] = pd.concat([self.dataframes["ocean_indicadores"], df_pivoted], ignore_index=True)
                break

    def get_profitability(self, ticker):
        ticker_lower = ticker.lower()
        url = f'{self.base_url}{ticker_lower}'
        soup = self._get_soup(url)

        if soup is None:
            return

        try:
            apresentacao_dropdown = Select(self.driver.find_element(By.ID, 'apresentacaoQuadroLucratividade'))
            apresentacao_dropdown.select_by_visible_text('Trimestral')
            sleep(3)
            soup = BeautifulSoup(self.driver.page_source, 'html.parser')

            tables = soup.find_all('table')
            for table in tables:
                if table.find('tr', {'id': 'trLucratividadePeriodo'}):
                    data = []
                    header_row = table.find('tr', {'id': 'trLucratividadePeriodo'})
                    headers = ['Indicador'] + [td.text.strip() for td in header_row.find_all('td')]

                    for row in table.find_all('tr')[1:]:
                        label = row.find('th').text.strip()
                        values = [td.text.strip() for td in row.find_all('td')]
                        data.append([label] + values)

                    df_lucratividade = pd.DataFrame(data, columns=headers)
                    df_lucratividade['Ticker'] = ticker

                    df_melted = pd.melt(df_lucratividade, id_vars=["Ticker", "Indicador"], 
                                        var_name="Trimestre", value_name="Valor")

                    df_pivoted = df_melted.pivot_table(index=["Ticker", "Trimestre"], 
                                                       columns="Indicador", values="Valor", 
                                                       aggfunc='first').reset_index()

                    df_pivoted.columns.name = None
                    self.dataframes["ocean_lucratividade"] = pd.concat([self.dataframes["ocean_lucratividade"], df_pivoted], ignore_index=True)
                    break
        except Exception as e:
            logger.error(f"Erro em get_profitability para {ticker}: {e}", exc_info=True)

    def get_balance(self, ticker):
        ticker_lower = ticker.lower()
        url = f'{self.base_url}{ticker_lower}'
        soup = self._get_soup(url)

        if soup is None:
            return

        try:
            apresentacao_dropdown = Select(self.driver.find_element(By.ID, 'apresentacaoQuadroBalanco'))
            apresentacao_dropdown.select_by_visible_text('Mensal')
            sleep(1)
            soup = BeautifulSoup(self.driver.page_source, 'html.parser')

            tables = soup.find_all('table')
            for table in tables:
                if table.find('tr', {'id': 'trBalancoPeriodo'}):
                    data = []
                    header_row = table.find('tr', {'id': 'trBalancoPeriodo'})
                    headers = ['Indicador'] + [td.text.strip() for td in header_row.find_all('td')]

                    for row in table.find_all('tr')[1:]:
                        label = row.find('th').text.strip()
                        values = [td.text.strip() for td in row.find_all('td')]
                        data.append([label] + values)

                    df_balanco = pd.DataFrame(data, columns=headers)
                    df_balanco['Ticker'] = ticker

                    df_melted = pd.melt(df_balanco, id_vars=["Ticker", "Indicador"], 
                                        var_name="Periodo", value_name="Valor")

                    df_pivoted = df_melted.pivot_table(index=["Ticker", "Periodo"], 
                                                       columns="Indicador", values="Valor", 
                                                       aggfunc='first').reset_index()

                    df_pivoted.columns.name = None
                    self.dataframes["ocean_balanco"] = pd.concat([self.dataframes["ocean_balanco"], df_pivoted], ignore_index=True)
                    break
        except Exception as e:
            logger.error(f"Erro em get_balance para {ticker}: {e}", exc_info=True)

    def get_assets_data(self, ticker):
        ticker_lower = ticker.lower()
        url = f'{self.base_url}{ticker_lower}'
        self.driver.get(url)
        self.driver.implicitly_wait(10)

        # Captura os anos e categorias
        years = self.get_years()
        categories = {'imoveis': 'ocean_lista_imoveis', 'fiis': 'ocean_lista_fiis', 'cri': 'ocean_lista_cri'}

        self.capture_data(years, categories, ticker)

    def get_years(self):
        try:
            year_dropdown = WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.ID, 'periodoQuadroAtivos'))
            )
            select = Select(year_dropdown)
            years = [option.get_attribute("value") for option in select.options]
            return years
        except TimeoutException:
            logger.warning("Dropdown de anos não encontrado. Continuando sem filtrar por ano.")
            return [None]

    def scroll_to_element(self, element):
        # Scroll para o elemento com alinhamento ao centro da visualização
        self.driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", element)
        sleep(1)

    def capture_data(self, years, categories, ticker):
        try:
            tabela_element = WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.ID, 'tabelaAtivos'))
            )
            self.scroll_to_element(tabela_element)
            
            # print('Tabela carregada') # Logger debug se necessário

        except TimeoutException:
            pass

        for year in years:
            if year:
                try:
                    year_dropdown = WebDriverWait(self.driver, 20).until(
                        EC.presence_of_element_located((By.ID, 'periodoQuadroAtivos'))
                    )
                    Select(year_dropdown).select_by_value(year)
                    sleep(1) 
                except:
                    pass

            for category_key, category_df_name in categories.items():
                if category_key:
                    try:
                        category_dropdown = WebDriverWait(self.driver, 20).until(
                            EC.presence_of_element_located((By.ID, 'categoriaQuadroAtivos'))
                        )
                        Select(category_dropdown).select_by_value(category_key)
                        sleep(1) 

                        # Verifica se a tabela foi carregada antes de continuar
                        WebDriverWait(self.driver, 10).until(
                            EC.presence_of_element_located((By.ID, 'tabelaAtivos'))
                        )
                    except TimeoutException:
                        # logger.debug(f"Tabela não carregada para {category_key} no ano {year}. Pulando...")
                        continue
                    except Exception as e:
                        continue

                    data = []
                    while True:
                        page_source = self.driver.page_source
                        soup = BeautifulSoup(page_source, 'html.parser')
                        table = soup.find('table', {'id': 'tabelaAtivos', 'class': 'table table-striped table-hover table-condensed'})

                        if table:
                            rows = table.find_all('tr', class_='pagMrk')
                            headers_elements = table.find_all('th', class_='fonteDestaque')
                            headers = [header.text.strip() for header in headers_elements]
                            headers.extend(['Ticker', 'Ano'])

                            for row in rows:
                                cells = row.find_all('td')
                                if cells:  # Adiciona apenas se houver células na linha
                                    row_data = [cell.text.strip() for cell in cells]
                                    row_data.extend([ticker, year])
                                    data.append(row_data)

                        try:
                            next_button = self.driver.find_element(By.CSS_SELECTOR, "li.paginationjs-next")
                            if "disabled" in next_button.get_attribute("class"):
                                break
                            else:
                                next_button.click()
                                sleep(2)
                        except:
                            break

                    if data:
                        try:
                            df_temp = pd.DataFrame(data, columns=headers)
                            # Armazena no DataFrame correto e adiciona o Ticker
                            self.dataframes[category_df_name] = pd.concat([self.dataframes[category_df_name], df_temp], ignore_index=True)
                        except ValueError as ve:
                            logger.error(f"Erro ao criar DataFrame para categoria {category_key} e ano {year}: {ve}", exc_info=True)

    def scrape_all(self, df_ativos, output_dir):
        os.makedirs(output_dir, exist_ok=True)
        
        total = len(df_ativos)
        for idx, row in df_ativos.iterrows():
            ticker = row['Ticker']
            try:
                logger.info(f"[{idx+1}/{total}] Capturando dados Oceans14 para {ticker}...")
                self.get_indicators(ticker)
                self.get_profitability(ticker)
                self.get_balance(ticker)
                self.get_assets_data(ticker)
            except Exception as e:
                logger.error(f"Erro ao capturar dados para {ticker}: {e}", exc_info=True)

        # Salvar DataFrames
        for name, df in self.dataframes.items():
            filepath = os.path.join(output_dir, f"{name}.csv")
            df.to_csv(filepath, index=False)
            logger.info(f"Salvo: {filepath} ({len(df)} registros)")

        self.driver.quit()
