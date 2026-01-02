import boto3
import os
import json

GLUE_JOB_NAME = os.environ.get('GLUE_JOB_NAME', 'JobBovespaETL')
glue_client = boto3.client('glue')

def lambda_handler(event, context):
    """
    Função Lambda acionada por evento do S3 (PutObject na pasta raw)
    para iniciar automaticamente o Job ETL no AWS Glue.
    """
    print(f"Evento S3 recebido. Tentando iniciar Job Glue: {GLUE_JOB_NAME}")
    print("Detalhes do evento:", json.dumps(event))

    try:
        # Inicia o job de forma assíncrona
        response = glue_client.start_job_run(JobName=GLUE_JOB_NAME)
        job_run_id = response['JobRunId']
        print(f"✅ Job Glue iniciado com sucesso! Run ID: {job_run_id}")
        return job_run_id

    except Exception as e:
        print(f"❌ Erro crítico ao tentar iniciar o Job Glue: {str(e)}")
        raise e