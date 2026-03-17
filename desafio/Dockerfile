FROM apache/airflow:2.7.1

USER root
# Instala Java (Spark), Chromium (Browser) e Driver
RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-17-jdk \
    chromium \
    chromium-driver \
    wget \
    unzip \
    && apt-get clean

USER airflow
# Instala as bibliotecas Python necessárias
RUN pip install --no-cache-dir \
    selenium \
    webdriver-manager \
    pyspark==3.5.0 \
    apache-airflow-providers-amazon \
    boto3
