import pandas as pd

# Definição do diretório raiz da camada Refined
caminho_refined = "../datalake/refined"

print("--- Iniciando Validação do Data Lake (Refined) ---")

try:
    # Leitura com descoberta automática de partições (Hive-style)
    df = pd.read_parquet(caminho_refined, engine='pyarrow')
    
    print("\n[Amostra de Dados]")
    print(df.head())
    
    print("\n[Esquema de Dados Identificado]")
    print(df.columns.tolist())
    
    # Verificação de integridade do esquema
    colunas_obrigatorias = ['Valor_Fechamento', 'Volume_Negociado', 'Media_Movel_7d', 'Valor_Total_Negociado']
    schema_valido = all(col in df.columns for col in colunas_obrigatorias)
    
    if schema_valido:
        print("\n[STATUS: OK] Esquema validado com sucesso.")
    else:
        print("\n[STATUS: FALHA] Inconsistência no esquema de dados.")

except Exception as e:
    print(f"Erro fatal na leitura do Data Lake: {e}")