import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from calculo_indicadores import IndicadoresCalculator

# Configuração da Página
st.set_page_config(
    page_title="Análise de Indicadores FIIs",
    layout="wide",
    initial_sidebar_state="expanded"
)

st.title("📊 Análise de Indicadores de FIIs")
st.markdown("""
Este dashboard permite analisar o histórico de **Dividend Yield (DY)** e **P/VP** dos Fundos Imobiliários.
""")

# Inicializar Calculadora (Cache para não recarregar CSVs toda vez)
@st.cache_resource
def get_calculator():
    return IndicadoresCalculator()

try:
    calc = get_calculator()
except Exception as e:
    st.error(f"Erro ao inicializar calculadora: {e}")
    st.stop()

# Barra Lateral
st.sidebar.header("Filtros")

# Obter lista de tickers disponíveis
tickers_rend = calc.df_rendimentos['Ticker'].unique() if not calc.df_rendimentos.empty else []
tickers_yahoo = calc.df_yahoo['Ticker'].unique() if not calc.df_yahoo.empty else []
# Limpar sufixo .SA do Yahoo para unificar
tickers_yahoo_clean = [t.replace('.SA', '') for t in tickers_yahoo]
all_tickers = sorted(list(set(list(tickers_rend) + list(tickers_yahoo_clean))))

if not all_tickers:
    st.warning("Nenhum dado encontrado nos arquivos CSV. Verifique se o ETL foi executado.")
    st.stop()

selected_ticker = st.sidebar.selectbox("Selecione o Ativo", all_tickers)

if selected_ticker:
    st.header(f"Análise do Ativo: {selected_ticker}")
    
    col1, col2 = st.columns(2)
    
    # Variáveis para armazenar os dataframes e usar no gráfico comparativo
    df_dy = pd.DataFrame()
    df_pvp = pd.DataFrame()

    # 1. Histórico de Dividend Yield (Mensal)
    with col1:
        st.subheader("💰 Histórico de Dividend Yield (Mensal)")
        df_dy = calc.get_dy_history(selected_ticker)
        
        if not df_dy.empty:
            # Check if columns are correct numeric types for plotting
            if 'DY_Monthly' in df_dy.columns:
                 df_dy['DY_Monthly'] = pd.to_numeric(df_dy['DY_Monthly'], errors='coerce')
            
            # Gráfico
            fig_dy = px.line(
                df_dy, 
                x='Date', 
                y='DY_Monthly', 
                title=f'{selected_ticker} - DY Mensal (%)',
                markers=True,
                labels={'DY_Monthly': 'Dividend Yield (%)', 'Date': 'Data Base'}
            )
            fig_dy.update_layout(hovermode="x unified")
            st.plotly_chart(fig_dy, use_container_width=True)
            
            # Tabela Resumo
            st.dataframe(
                df_dy.sort_values('Date', ascending=False).head(12).style.format({
                    'Dividend': 'R$ {:.2f}',
                    'Price_at_Database': 'R$ {:.2f}',
                    'DY_Monthly': '{:.2f}%'
                }),
                use_container_width=True
            )
        else:
            st.warning("Sem dados históricos de dividendos para este ativo.")

    # 2. Histórico de P/VP (Mensal)
    with col2:
        st.subheader("📉 Histórico de P/VP (Mensal)")
        # Alteração: Usar histórico Mensal
        df_pvp = calc.get_pvp_history_monthly(selected_ticker)
        
        if not df_pvp.empty:
            # Verifica se o VP varia ao longo do tempo (mais que 1 valor único)
            unique_vps = df_pvp['VP_Used'].nunique()
            is_historical_vp = unique_vps > 1
            
            last_vp = df_pvp['VP_Used'].iloc[-1]
            last_pvp = df_pvp['P_VP'].iloc[-1]
            last_price = df_pvp['Close'].iloc[-1]
            
            st.metric("P/VP Atual (Fechamento Mês)", f"{last_pvp:.2f}", f"Preço: R$ {last_price:.2f}")
            
            if is_historical_vp:
                st.success(f"✅ Utilizando histórico de VP (Yahoo/Oceans14).")
                title_chart = f'{selected_ticker} - P/VP Histórico Mensal'
            else:
                st.info(f"ℹ️ Utilizando **VP Fixo (R$ {last_vp:.2f})** projetado para todo o período.")
                title_chart = f'{selected_ticker} - P/VP Histórico Mensal (VP Fixo)'
            
            fig_pvp = px.line(
                df_pvp, 
                x='Date', 
                y='P_VP', 
                title=title_chart,
                markers=True, # Adicionar marcadores já que é mensal
                labels={'P_VP': 'P/VP', 'Date': 'Mês', 'VP_Used': 'Valor Patrimonial'}
            )
            
            # Adicionar trace do VP para comparação (opcional, em eixo secundário seria melhor, mas tooltips já ajudam)
            fig_pvp.add_scatter(x=df_pvp['Date'], y=df_pvp['VP_Used'], mode='lines', name='Valor Patrimonial (R$)', visible='legendonly')

            # Linha de referência P/VP = 1
            fig_pvp.add_hline(y=1.0, line_dash="dash", line_color="red", annotation_text="Preço Justo (1.0)")
            fig_pvp.update_layout(hovermode="x unified")
            
            st.plotly_chart(fig_pvp, use_container_width=True)
            
             # Tabela Resumo P/VP
            st.dataframe(
                df_pvp.sort_values('Date', ascending=False).head(12).style.format({
                    'Close': 'R$ {:.2f}',
                    'VP_Used': 'R$ {:.2f}',
                    'P_VP': '{:.2f}'
                }),
                use_container_width=True
            )
        else:
            st.warning("Sem dados de cotação ou Valor Patrimonial para calcular P/VP.")

    # 3. Gráfico Comparativo (DY vs P/VP)
    st.markdown("---")
    st.subheader("🔄 Comparativo: Dividend Yield vs P/VP")
    
    if not df_dy.empty and not df_pvp.empty:
        # Merge dataframes
        df_merged = pd.merge(
            df_dy[['Date', 'DY_Monthly']], 
            df_pvp[['Date', 'P_VP']], 
            on='Date', 
            how='outer'
        ).sort_values('Date')
        
        # Filtro de data para não ficar muito extenso se um histórico for muito maior que o outro
        df_merged = df_merged.dropna(subset=['DY_Monthly', 'P_VP'], how='all')

        fig_comp = go.Figure()
        
        # DY Trace (Red) - Left Axis
        fig_comp.add_trace(go.Scatter(
            x=df_merged['Date'],
            y=df_merged['DY_Monthly'],
            name='Dividend Yield (%)',
            mode='lines',
            connectgaps=True, # Conectar pontos mesmo se houver falhas
            line=dict(color='red', width=2),
            yaxis='y'
        ))
        
        # P/VP Trace (Yellow) - Right Axis
        fig_comp.add_trace(go.Scatter(
            x=df_merged['Date'],
            y=df_merged['P_VP'],
            name='P/VP',
            mode='lines',
            connectgaps=True, # Conectar pontos mesmo se houver falhas
            line=dict(color='#FFD700', width=2), # Gold/Yellow
            yaxis='y2'
        ))
        
        # Layout with Dual Axis
        fig_comp.update_layout(
            title=f'{selected_ticker} - Correlação DY vs P/VP',
            xaxis=dict(title='Data'),
            yaxis=dict(
                title=dict(text='Dividend Yield (%)', font=dict(color='red')),
                tickfont=dict(color='red'),
                showgrid=True,
                zeroline=False
            ),
            yaxis2=dict(
                title=dict(text='P/VP', font=dict(color='#FFD700')),
                tickfont=dict(color='#FFD700'),
                overlaying='y',
                side='right',
                showgrid=False # Evitar grid lines conflitantes
            ),
            hovermode='x unified',
            legend=dict(x=0, y=1.1, orientation='h'),
            height=500
        )
        
        st.plotly_chart(fig_comp, use_container_width=True)
    else:
        st.warning("Não há dados suficientes de ambos os indicadores para gerar o gráfico comparativo.")

else:
    st.info("Selecione um ativo na barra lateral para começar.")
