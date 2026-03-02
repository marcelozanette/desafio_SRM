import requests
import boto3
from botocore.client import Config

def executar_ingestao(url, nome_arquivo):
    s3 = boto3.client(
        's3',
        endpoint_url="http://datalake_minio:9000",
        aws_access_key_id="admin",
        aws_secret_access_key="password123",
        config=Config(signature_version='s3v4')
    )
    
    # Criar bucket se não existir
    try: s3.create_bucket(Bucket="bronze")
    except: pass

    print(f"Baixando {url}...")
    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        s3.upload_fileobj(r.raw, "bronze", f"receita/{nome_arquivo}")
    print("Upload concluído!")