import os
import zipfile
import shutil
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, array, array_contains, regexp_replace
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# --- CONFIGURAÇÕES ---
BRONZE_BUCKET = "bronze"
SILVER_BUCKET = "silver"
MINIO_CONN_ID = "minio_conn"
MINIO_ENDPOINT = "http://minio:9000" 
LOCAL_EXTRACT_PATH = "/tmp/extract_cnpj"

# --- SCHEMAS (Para uso após validação das colunas) ---
SCHEMA_MUNICIPIOS = StructType([
    StructField("municipio_cod", StringType(), True),
    StructField("municipio_nome", StringType(), True)
])

def extrair_do_s3(s3_hook, bucket, prefix):
    """Baixa e extrai os ZIPs para o disco local do container."""
    folder_path = os.path.join(LOCAL_EXTRACT_PATH, prefix.replace("/", ""))
    if not os.path.exists(folder_path):
        os.makedirs(folder_path)
        
    keys = s3_hook.list_keys(bucket_name=bucket, prefix=prefix)
    if not keys:
        print(f"⚠️ Aviso: Nenhum arquivo encontrado em {bucket}/{prefix}")
        return folder_path

    for key in keys:
        if not key.endswith(".zip"): continue
        local_zip = os.path.join(folder_path, "temp.zip")
        print(f"📥 Baixando {key} para extração...")
        s3_hook.get_key(key, bucket).download_file(local_zip)
        
        with zipfile.ZipFile(local_zip, 'r') as z:
            z.extractall(folder_path)
        os.remove(local_zip)
    return folder_path

def bronze_to_silver_pyspark():
    s3_hook = S3Hook(aws_conn_id=MINIO_CONN_ID)
    credentials = s3_hook.get_credentials()
    
    # Garantir bucket Silver
    if not s3_hook.check_for_bucket(SILVER_BUCKET):
        s3_hook.create_bucket(SILVER_BUCKET)

    if os.path.exists(LOCAL_EXTRACT_PATH):
        shutil.rmtree(LOCAL_EXTRACT_PATH)

    spark = SparkSession.builder \
        .appName("CNPJ_Debug_and_Join") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262") \
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT) \
        .config("spark.hadoop.fs.s3a.access.key", credentials.access_key) \
        .config("spark.hadoop.fs.s3a.secret.key", credentials.secret_key) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()

    try:
        # 1. Processando Municípios
        print("📂 Lendo Municípios...")
        path_mun = extrair_do_s3(s3_hook, BRONZE_BUCKET, "municipios_2025-12/")
        df_mun = spark.read.csv(f"file://{path_mun}/*", sep=";", schema=SCHEMA_MUNICIPIOS, encoding="iso-8859-1")
        
        # Limpeza numérica (Código 7107 = São Paulo)
        df_mun = df_mun.withColumn("municipio_cod", regexp_replace(col("municipio_cod"), '[^0-9]', '').cast(IntegerType()))
        df_mun_sp = df_mun.filter(col("municipio_cod") == 7107)
        print(f"📊 Municípios SP encontrados: {df_mun_sp.count()}")

        # 2. Processando Estabelecimentos (DEBUG MODE)
        print("📂 Lendo Estabelecimentos (Sem Schema para Debug)...")
        path_est = extrair_do_s3(s3_hook, BRONZE_BUCKET, "estabelecimentos_2025-12/")
        # Lemos sem schema para ver onde os dados caem originalmente
        df_est = spark.read.csv(f"file://{path_est}/*", sep=";", encoding="iso-8859-1", inferSchema=False)
        
        print("🔍 DEBUG: Amostra bruta das colunas:")
        df_est.show(5)

        # Busca cega pelo código de SP em todas as colunas geradas (_c0, _c1, etc)
        cols_search = [col(c).cast("string") for c in df_est.columns]
        df_debug_find = df_est.filter(array_contains(array(cols_search), "7107"))
        
        found_count = df_debug_find.count()
        print(f"🔎 Busca '7107' encontrou: {found_count} linhas.")

        if found_count > 0:
            print("✅ Linhas de exemplo encontradas na busca cega:")
            df_debug_find.show(5)
            
            # --- MAPEAMENTO DINÂMICO ---
            # Se você descobriu que o código do município está na coluna _c20:
            # df_est = df_est.withColumnRenamed("_c0", "cnpj_basico")... etc
            # Aqui vamos assumir o layout padrão da RFB para tentar o Join:
            
            print("🔗 Tentando Join baseado na posição padrão da RFB (Coluna 20 para Município)...")
            # Ajuste o índice abaixo conforme o resultado do seu df_debug_find.show()
            col_municipio_index = "_c20" 
            col_situacao_index = "_c5"
            
            df_est_clean = df_est.withColumn("mun_clean", regexp_replace(col(col_municipio_index), '[^0-9]', '').cast(IntegerType())) \
                                 .withColumn("sit_clean", regexp_replace(col(col_situacao_index), '[^0-9]', '').cast(IntegerType()))

            df_silver = df_est_clean.join(df_mun_sp, df_est_clean.mun_clean == df_mun_sp.municipio_cod) \
                                    .filter(col("sit_clean") == 2)

            total_final = df_silver.count()
            print(f"🚀 Resultado Final: {total_final} registros filtrados.")

            if total_final > 0:
                print(f"📤 Gravando em s3a://{SILVER_BUCKET}/estabelecimentos_2025-12...")
                df_silver.write.mode("overwrite").parquet(f"s3a://{SILVER_BUCKET}/estabelecimentos_2025-12")
        else:
            print("❌ O código 7107 não foi encontrado em nenhuma coluna. Verifique o delimitador ou o arquivo.")

    except Exception as e:
        print(f"💥 Erro: {e}")
        raise e
    finally:
        spark.stop()
        if os.path.exists(LOCAL_EXTRACT_PATH):
            shutil.rmtree(LOCAL_EXTRACT_PATH)

# --- DAG ---
with DAG(
    'etl_silver', 
    default_args={'owner': 'airflow'}, 
    schedule_interval=None, 
    start_date=datetime(2024, 1, 1), 
    catchup=False
) as dag:

    PythonOperator(
        task_id='pyspark_debug_task', 
        python_callable=bronze_to_silver_pyspark
    )
