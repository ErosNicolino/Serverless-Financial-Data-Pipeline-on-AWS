import pandas as pd
import glob
import os
from datetime import datetime

# Configuração de Caminhos
DATA_HOJE = datetime.now().strftime('%Y-%m-%d')
INPUT_PATH = f"../datalake/raw/data={DATA_HOJE}/*.parquet"
OUTPUT_DIR = "datalake/refined"

def job_glue_simulado():
    print("--- Iniciando Pipeline ETL (Simulação Local) ---")

    arquivos = glob.glob(INPUT_PATH)

    if not arquivos:
        print(f"Nenhum arquivo encontrado no diretório: {INPUT_PATH}")
        return

    dfs = []
    for arq in arquivos:
        df_temp = pd.read_parquet(arq)
        dfs.append(df_temp)

    # Consolidação dos DataFrames
    df_completo = pd.concat(dfs, ignore_index=True)
    print("Dados carregados. Aplicando regras de negócio...")

    # Transformação: Renomeação de colunas
    df_completo = df_completo.rename(columns={
        'Close': 'Valor_Fechamento',
        'Volume': 'Volume_Negociado'
    })

    # Transformação: Cálculo de Média Móvel (7 dias)
    df_completo = df_completo.sort_values(by=['symbol', 'Date'])
    df_completo['Media_Movel_7d'] = df_completo.groupby('symbol')['Valor_Fechamento'].transform(
        lambda x: x.rolling(window=7, min_periods=1).mean()
    )

    # Transformação: Cálculo de Valor Total Negociado
    df_completo['Valor_Total_Negociado'] = df_completo['Valor_Fechamento'] * df_completo['Volume_Negociado']

    # Carga: Persistência na camada Refined
    print("Iniciando gravação na camada Refined...")

    df_completo['data_particao'] = df_completo['Date'].dt.strftime('%Y-%m-%d')

    try:
        df_completo.to_parquet(
            OUTPUT_DIR,
            index=False,
            engine='pyarrow',
            partition_cols=['data_particao', 'symbol']
        )
        print(f"Pipeline finalizado. Dados persistidos em: {OUTPUT_DIR}")

    except Exception as e:
        print(f"Erro crítico na gravação dos dados: {e}")

if __name__ == "__main__":
    job_glue_simulado()