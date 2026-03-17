# 🏢 Pipeline de Dados CNPJ - Receita Federal (São Paulo)

Este projeto é um MVP de uma pipeline de dados automatizada para processar os Dados Abertos do CNPJ da Receita Federal. O objetivo é analisar a quantidade de matrizes e filiais ativas na cidade de São Paulo.

## 🏗️ Arquitetura da Solução

A solução foi construída seguindo a **Arquitetura Medallion** para garantir organização, qualidade e linhagem dos dados:

* **Bronze (Raw):** Dados extraídos diretamente da Receita Federal em formato `.zip`/`csv`.
* **Silver (Processed):** Dados limpos, com tipos convertidos, filtrados para a cidade de São Paulo (Município 7107) e situação cadastral "Ativa".
* **Gold (Curated):** Agregações finais prontas para consumo de BI (Contagem de Matrizes vs Filiais).

### Tech Stack
* **Orquestração:** Apache Airflow
* **Processamento:** Apache Spark (PySpark)
* **Data Lake:** MinIO (S3-Compatible Storage)
* **Infraestrutura:** Docker & Docker Compose



---

## 🚀 Como Rodar o Projeto

### Pré-requisitos
* Docker e Docker Compose instalados.

### Passo a Passo
1. **Clonar o repositório:**
   ```bash
2. Subir o ambiente:

Bash
docker-compose up -d

3. Acessar as interfaces:

Airflow: http://localhost:8081 (Login: admin / Senha: Ver comando abaixo)
MinIO: http://localhost:9001 (Login: admin / Senha: password123)

4. Obter senha do Airflow:

Bash
docker exec -it airflow_webserver cat /opt/airflow/standalone_admin_password.txt

Spark Master: http://localhost:8080
   git clone https://github.com/marcelozanette/desafio_SRM.git

5. Configurações nas Interfaces Web
Agora que os containers estão rodando, configure as ferramentas:
A. MinIO (Armazenamento)
Acesse: http://localhost:9001 (User: admin / Pass: password123).
Vá em Buckets -> Create Bucket -> Nome: cnpj-data.
B. Criar a conexão do minio no Airflow (Orquestrador)
Acesse: http://localhost:8081 (User: admin / Senha do passo 4).
Vá em Admin -> Connections -> Add New.
Conn Id: minio_conn
Conn Type: Amazon S3 (ou Generic)
Extra:
json
{"aws_access_key_id": "admin", "aws_secret_access_key": "password123", "endpoint_url": "http://minio:9000", "region_name": "us-east-1"}
Use o código com cuidado.
