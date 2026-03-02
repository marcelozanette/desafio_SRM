from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import sys

# Adiciona pasta scripts ao path
sys.path.append('/opt/airflow/dags/scripts')
from ingestao_bronze import executar_ingestao
from processamento_silver import rodar_spark_job

with DAG(
    'pipeline_receita_federal',
    start_date=datetime(2025, 12, 1),
    schedule_interval='@once',
    catchup=False
) as dag:

    ingestao = PythonOperator(
        task_id='ingestao_dados_receita',
        python_callable=executar_ingestao,
        op_kwargs={
            'url': 'https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj/2025-12/Estabelecimentos0.zip',
            'nome_arquivo': 'estabelecimentos.zip'
        }
    )

    processamento = PythonOperator(
        task_id='processamento_spark_silver_gold',
        python_callable=rodar_spark_job
    )

    ingestao >> processamento