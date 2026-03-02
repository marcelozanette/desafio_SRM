from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def rodar_spark_job():
    spark = SparkSession.builder \
        .appName("CNPJ_SP_Job") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://datalake_minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "admin") \
        .config("spark.hadoop.fs.s3a.secret.key", "password123") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()

    # Leitura (Exemplo simplificado de path)
    # No desafio real, você mapearia o Schema conforme o PDF da Receita
    df = spark.read.csv("s3a://bronze/receita/*.csv", sep=";", header=False)

    # Transformação
    # Coluna 5: Matriz/Filial | Coluna 6: Situação | Coluna 20: Cidade
    df_sp = df.filter((F.col("_c5") == "02") & (F.col("_c20") == "7107"))
    
    df_final = df_sp.withColumn("tipo", F.when(F.col("_c3") == "1", "Matriz").otherwise("Filial"))
    
    # Gold: Agregação
    df_gold = df_final.groupBy("tipo").count()
    
    # Salvar resultado
    df_gold.write.mode("overwrite").csv("s3a://gold/resultado_final")
    spark.stop()