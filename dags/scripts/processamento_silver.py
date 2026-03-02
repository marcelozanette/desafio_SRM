from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType

# Configuração da Sessão Spark para conectar no MinIO
spark = SparkSession.builder \
    .appName("CNPJ_Silver_Processing") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://datalake_minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "password123") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

# 1. Definição do Schema (Baseado no Layout da Receita)
# Aqui pegamos apenas o que importa para o desafio
schema_estab = StructType([
    StructField("cnpj_basico", StringType()),
    StructField("cnpj_ordem", StringType()),
    StructField("cnpj_dv", StringType()),
    StructField("identificador_matriz_filial", StringType()), # 1-Matriz, 2-Filial
    StructField("nome_fantasia", StringType()),
    StructField("situacao_cadastral", StringType()),         # 02-Ativa
    # ... (outros campos ocultos para brevidade)
    StructField("municipio_id", StringType(), True)          # Coluna 24 no layout original
])

# 2. Leitura dos Dados (Bronze)
# O Spark consegue ler direto do .zip se estiver descompactado ou via bibliotecas extras.
# Dica: Para o desafio, assuma que você descompactou ou use o CSV extraído.
df_estab = spark.read.csv("s3a://bronze/receita/ESTABELE.csv", sep=";", schema=schema_estab)
df_muni = spark.read.csv("s3a://bronze/receita/MUNICIPIOS.csv", sep=";", header=False)

# 3. Transformação (Regras de Negócio)
# Filtrar: São Paulo (7107) AND Situação Ativa (02)
df_silver = df_estab.filter(
    (F.col("municipio_id") == "7107") & 
    (F.col("situacao_cadastral") == "02")
)

# Criar coluna legível para Matriz/Filial
df_silver = df_silver.withColumn(
    "tipo_unidade", 
    F.when(F.col("identificador_matriz_filial") == "1", "MATRIZ").otherwise("FILIAL")
)

# 4. Escrita na Silver (Parquet é vida!)
df_silver.write.mode("overwrite").parquet("s3a://silver/estabelecimentos_sp_ativos")

print("Processamento Silver concluído com sucesso!")