import yfinance as yf
import pandas as pd
import os
from datetime import datetime

# Parâmetros de Execução
TICKERS = ['PETR4.SA', 'VALE3.SA', 'ITUB4.SA', '^BVSP']
DATA_HOJE = datetime.now().strftime('%Y-%m-%d')

def ingestao_raw():
    print(f"--- Iniciando Rotina de Ingestão: {DATA_HOJE} ---")
    
    # Coleta de dados de mercado (D-1)
    print(f"Solicitando dados para os ativos: {TICKERS}")
    dados = yf.download(TICKERS, period='1d', group_by='ticker', auto_adjust=True)
    
    # Processamento e Persistência
    for ticker in TICKERS:
        try:
            df_ticker = dados[ticker].copy()
            
            if df_ticker.empty:
                print(f"[AVISO] Dados indisponíveis para: {ticker}")
                continue
            
            # Tratamento inicial e metadados
            df_ticker = df_ticker.reset_index()
            df_ticker['symbol'] = ticker.replace('.SA', '').replace('^', '')
            
            # Definição de diretório de saída (Particionamento por data)
            output_dir = f"../datalake/raw/data={DATA_HOJE}"
            os.makedirs(output_dir, exist_ok=True)
            
            arquivo_saida = f"{output_dir}/{ticker}.parquet"
            
            # Gravação em formato Parquet
            df_ticker.to_parquet(arquivo_saida, index=False, engine='pyarrow')
            print(f"[SUCESSO] Arquivo gerado: {arquivo_saida}")
            
        except Exception as e:
            print(f"[ERRO] Falha ao processar {ticker}: {e}")

if __name__ == "__main__":
    ingestao_raw()