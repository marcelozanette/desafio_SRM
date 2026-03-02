import os
import requests
import boto3
from botocore.client import Config

# Configurações do MinIO (S3 Local)
MINIO_ENDPOINT = "http://datalake_minio:9000"
MINIO_ACCESS_KEY = "admin"
MINIO_SECRET_KEY = "password123"
BUCKET_NAME = "bronze"

def get_s3_client():
    return boto3.client(
        's3',
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        config=Config(signature_version='s3v4')
    )

def baixar_e_subir_minio(url_arquivo, nome_destino):
    s3 = get_s3_client()
    
    # Garante que o bucket existe
    try:
        s3.create_bucket(Bucket=BUCKET_NAME)
    except:
        pass

    print(f"Iniciando download de: {url_arquivo}")
    
    # Stream do download para não estourar a memória RAM do container
    with requests.get(url_arquivo, stream=True) as r:
        r.raise_for_status()
        # Upload direto para o MinIO (Multipart Upload automático pelo boto3)
        s3.upload_fileobj(r.raw, BUCKET_NAME, nome_destino)
        
    print(f"Arquivo {nome_destino} salvo com sucesso na camada Bronze!")

if __name__ == "__main__":
    # Exemplo de URLs da Receita (Estabelecimentos e Municípios)
    # No desafio, você pode automatizar a busca dessas URLs no site
    base_url = "https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj/2025-12/"
    
    arquivos = {
        "estabelecimentos": "Estabelecimentos0.zip",
        "municipios": "Municipios.zip"
    }

    for chave, arquivo in arquivos.items():
        url = f"{base_url}{arquivo}"
        baixar_e_subir_minio(url, f"receita/{arquivo}")