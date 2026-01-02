import yfinance as yf
import pandas as pd
import boto3
import os
from datetime import datetime

# Configurações de Ambiente
S3_BUCKET = os.environ.get('S3_BUCKET', 'nome-do-bucket-aqui')
TICKERS = ['PETR4.SA', 'VALE3.SA', 'ITUB4.SA', '^BVSP']

def lambda_handler(event, context):
    print("Iniciando execução da Lambda de Ingestão.")
    s3_client = boto3.client('s3')
    data_hoje = datetime.now().strftime('%Y-%m-%d')
    
    # Extração de dados via API yfinance
    dados = yf.download(TICKERS, period='1d', group_by='ticker', auto_adjust=True)
    
    for ticker in TICKERS:
        try:
            df = dados[ticker].copy()
            if df.empty:
                print(f"Nenhum dado retornado para o ticker: {ticker}")
                continue
            
            # Normalização do DataFrame
            df = df.reset_index()
            df['symbol'] = ticker.replace('.SA', '').replace('^', '')
            
            # Persistência temporária (Ephemeral storage /tmp)
            local_path = f"/tmp/{ticker}.parquet"
            df.to_parquet(local_path, index=False)
            
            # Upload para S3 (Camada Raw)
            s3_path = f"raw/data={data_hoje}/{ticker}.parquet"
            s3_client.upload_file(local_path, S3_BUCKET, s3_path)
            
            print(f"Upload realizado com sucesso: s3://{S3_BUCKET}/{s3_path}")
            
        except Exception as e:
            print(f"Erro ao processar ticker {ticker}: {e}")
            
    return {
        'statusCode': 200,
        'body': 'Processo de ingestão finalizado.'
    }