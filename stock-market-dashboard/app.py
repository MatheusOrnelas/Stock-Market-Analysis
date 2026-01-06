import dash
from dash import dcc, html, Input, Output
import pandas as pd
from utils.db_conn import get_db_engine
from components.graphs import plot_category_count, plot_dividend_chart

app = dash.Dash(__name__)
server = app.server

def get_data(query):
    engine = get_db_engine()
    return pd.read_sql(query, engine)

# Layout
app.layout = html.Div([
    html.H1("Stock Market Dashboard"),
    
    dcc.Tabs([
        dcc.Tab(label='Visão Geral FIIs', children=[
            html.Div([
                html.H3("Distribuição por Setor"),
                dcc.Graph(id='graph-sector-count')
            ])
        ]),
        dcc.Tab(label='Análise Individual', children=[
            html.Div([
                html.Label("Selecione o Ticker:"),
                dcc.Dropdown(id='ticker-dropdown', options=[], placeholder="Carregando..."),
                dcc.Graph(id='graph-dividend-history')
            ])
        ])
    ])
])

# Callbacks
@app.callback(
    Output('graph-sector-count', 'figure'),
    Input('graph-sector-count', 'id') # Dummy input to trigger on load
)
def update_sector_graph(_):
    try:
        df = get_data("SELECT * FROM funds_ativos")
        # Assumindo que exista uma coluna 'segmento' ou 'setor'. 
        # Ajuste conforme o nome real da coluna no DB (que foi limpo por clean_column_name)
        # Ex: 'segmento'
        if 'segmento' in df.columns:
            return plot_category_count(df, 'segmento', title="FIIs por Segmento")
        elif 'tipo' in df.columns:
             return plot_category_count(df, 'tipo', title="FIIs por Tipo")
        return {}
    except Exception as e:
        print(f"Erro ao carregar gráfico de setores: {e}")
        return {}

@app.callback(
    [Output('ticker-dropdown', 'options'),
     Output('ticker-dropdown', 'value')],
    Input('ticker-dropdown', 'search_value')
)
def update_dropdown(_):
    try:
        df = get_data("SELECT DISTINCT ticker FROM funds_ativos ORDER BY ticker")
        options = [{'label': t, 'value': t} for t in df['ticker']]
        return options, options[0]['value'] if options else None
    except:
        return [], None

@app.callback(
    Output('graph-dividend-history', 'figure'),
    Input('ticker-dropdown', 'value')
)
def update_dividend_graph(ticker):
    if not ticker:
        return {}
    try:
        # Exemplo de query para rendimentos
        # Ajuste os nomes das colunas conforme o schema real
        query = f"SELECT * FROM funds_rendimentos WHERE ticker = '{ticker}'"
        df = get_data(query)
        
        if df.empty:
            return {}
            
        # Converter datas e valores se necessário
        # Assumindo colunas 'data_pagamento', 'valor_rendimento', 'dividend_yield'
        # Ajuste conforme colunas reais
        col_date = 'data_base' if 'data_base' in df.columns else 'data_com'
        col_val = 'rendimento'
        col_dy = 'dividend_yield'
        
        if col_date in df.columns and col_val in df.columns:
             return plot_dividend_chart(df, col_date, col_val, col_dy if col_dy in df.columns else col_val)
        return {}
    except Exception as e:
        print(f"Erro ao carregar gráfico de dividendos: {e}")
        return {}

if __name__ == '__main__':
    app.run(debug=True)

