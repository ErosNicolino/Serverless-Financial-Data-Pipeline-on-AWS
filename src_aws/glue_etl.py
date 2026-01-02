import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, avg, to_date, date_format
from pyspark.sql.window import Window

# --- Configuração Inicial ---
# Captura parâmetros passados na execução do Job
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'BUCKET_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Configura caminhos baseados no bucket informado
BUCKET_NAME = args['BUCKET_NAME']
S3_RAW_PATH = f"s3://{BUCKET_NAME}/raw/"
S3_REFINED_PATH = f"s3://{BUCKET_NAME}/refined/"

print(f"--- Iniciando ETL Bovespa ---")
print(f"Lendo dados brutos de: {S3_RAW_PATH}")

# --- 1. Extract (Leitura) ---
# Lê todos os arquivos Parquet da camada Raw, inclusive em subpastas de data
try:
    df_raw = spark.read.option("recursiveFileLookup", "true").parquet(S3_RAW_PATH)
except Exception as e:
    print(f"Erro ao ler dados do S3. Verifique se a pasta raw não está vazia. Erro: {e}")
    sys.exit(1)

# --- 2. Transform (Transformação) ---
# Seleciona e renomeia colunas, garantindo a tipagem correta
df_transformed = df_raw.select(
    to_date(col("Date")).alias("data_pregao"),
    col("symbol"),
    col("Close").cast("double").alias("valor_fechamento"),
    col("Volume").cast("long").alias("volume_negociado")
)

# Definição de Janela para cálculo da Média Móvel
# Agrupa por símbolo, ordena por data, e pega as 7 linhas anteriores (incluindo a atual)
windowSpec7D = Window.partitionBy("symbol").orderBy("data_pregao").rowsBetween(-6, 0)

# Aplica as regras de negócio: Média Móvel e Valor Total
df_enriched = df_transformed.withColumn(
    "media_movel_7d",
    avg(col("valor_fechamento")).over(windowSpec7D)
).withColumn(
    "valor_total_negociado",
    col("valor_fechamento") * col("volume_negociado")
)

# Cria coluna de partição para organização no S3 Refined
df_final = df_enriched.withColumn(
    "data_particao",
    date_format(col("data_pregao"), "yyyy-MM-dd")
)

print("--- Schema do DataFrame Final ---")
df_final.printSchema()

# --- 3. Load (Carga) ---
print(f"Escrevendo dados processados em: {S3_REFINED_PATH}")

df_final.write.mode("overwrite") \
    .partitionBy("data_particao", "symbol") \
    .option("compression", "snappy") \
    .parquet(S3_REFINED_PATH)

print("--- ETL Concluído com Sucesso! ---")
job.commit()