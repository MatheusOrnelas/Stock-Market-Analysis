# Análise de Indicadores de FIIs

Este diretório contém scripts para calcular e visualizar o histórico de indicadores de Fundos Imobiliários.

## Estrutura

- `calculo_indicadores.py`: Script responsável por carregar os dados brutos (CSV) e realizar os cálculos de P/VP e Dividend Yield.
- `dashboard.py`: Dashboard interativo desenvolvido em Streamlit para visualização dos dados.

## Pré-requisitos

Os dados são lidos da pasta `../stock-market-etl/data/bronze`. Certifique-se de que o processo de ETL foi executado e os arquivos `funds_rendimentos.csv`, `funds_indicadores_diarios.csv` e `yahoo_cotacoes.csv` existem.

## Como Executar

1. Certifique-se de ter as dependências instaladas:
   ```bash
   pip install pandas streamlit plotly
   ```

2. Execute o dashboard:
   ```bash
   streamlit run analise_de_Indicadores/dashboard.py
   ```

## Notas sobre os Cálculos

- **P/VP Histórico**: O cálculo utiliza o histórico de cotações (Yahoo Finance) dividido pelo **Valor Patrimonial Atual** (Funds Explorer), pois a base de dados atual não possui histórico diário/mensal de VP. Isso fornece uma estimativa de como o preço se comportou em relação ao valor patrimonial atual.
- **Dividend Yield Histórico**: Baseado nos registros de proventos (Tabela de Rendimentos), utilizando o valor do dividendo dividido pela cotação na data-base (fechamento).

