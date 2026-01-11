import pandas as pd
from analise_de_Indicadores.calculo_indicadores import IndicadoresCalculator

calc = IndicadoresCalculator()
df_rend = calc.df_rendimentos

print("--- Análise de Cobertura de Dados de DY ---")
if df_rend.empty:
    print("ERRO CRÍTICO: DataFrame de rendimentos vazio.")
else:
    # Agrupar por Ticker e contar registros válidos
    coverage = df_rend.groupby('Ticker').count()['Valor por Cota (R$)']
    
    print(f"Total de Ativos com dados de rendimento: {len(coverage)}")
    print("\nTop 10 Ativos com mais histórico:")
    print(coverage.sort_values(ascending=False).head(10))
    
    # Verificar especificamente CPTR11
    if 'VINO11' in coverage.index:
        print(f"\nVINO11 tem {coverage['VINO11']} registros válidos.")
    else:
        print("\nVINO11 NÃO tem registros válidos no dataframe processado.")
        
        # Debug deeper: por que foi filtrado?
        raw_path = calc.data_dir_bronze + '/funds_rendimentos.csv'
        df_raw = pd.read_csv(raw_path)
        cptr_raw = df_raw[df_raw['Ticker'] == 'VINO11']
        print(f"Registros RAW do VINO11: {len(cptr_raw)}")
        if not cptr_raw.empty:
            print("Exemplo raw:")
            print(cptr_raw.iloc[0])

