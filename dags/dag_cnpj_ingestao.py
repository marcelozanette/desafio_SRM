from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import sys

# Importa sua função de ingestão (garanta que o path está no PYTHONPATH)
sys.path.append('/opt/airflow/dags/scripts')
from ingestao_receita import baixar_e_subir_minio

default_args = {
    'owner': 'data_eng',
    'start_date': datetime(2025, 1, 1),
}

with DAG(
    'pipeline_cnpj_receita',
    default_args=default_args,
    schedule_interval='@monthly',
    catchup=False
) as dag:

    def task_download_func(**kwargs):
        # Aqui você pode usar kwargs['ds'] para pegar a data da execução
        # e montar a URL dinâmica da Receita Federal
        base_url = "https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj/2025-12/"
        baixar_e_subir_minio(f"{base_url}Municipios.zip", "receita/municipios.zip")

    ingestao_bronze = PythonOperator(
        task_id='ingestao_municipios_bronze',
        python_callable=task_download_func
    )