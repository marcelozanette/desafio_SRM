import os
import time
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
caminho = ["Dados", "Cadastros", "CNPJ", "2025-12"]
ano_mes=caminho[-1]
# --- CONFIGURAÇÕES ---
BUCKET_NAME = "bronze"
MINIO_CONN_ID = "minio_conn" 
TEMP_DIR = "/tmp/cnpj"
URL_BASE = "https://arquivos.receitafederal.gov.br"

def upload_to_minio(file_path, file_name):
    """Envia para o MinIO e deleta o arquivo local imediatamente."""
    try:
        hook = S3Hook(aws_conn_id=MINIO_CONN_ID)
        print(f"📤 Subindo {file_name} para o MinIO...")
        hook.load_file(
            filename=file_path,
            key=f"estabelecimentos_{ano_mes}/{file_name}",
            bucket_name=BUCKET_NAME,
            replace=True
        )
        os.remove(file_path)
        print(f"✅ {file_name} processado e removido do disco local.")
    except Exception as e:
        print(f"❌ Erro no upload: {e}")
        raise

def baixar_apenas_estabelecimentos():
    # 1. Preparação do ambiente
    hook = S3Hook(aws_conn_id=MINIO_CONN_ID)
    if not hook.check_for_bucket(BUCKET_NAME):
        print(f"Criando bucket {BUCKET_NAME}...")
        hook.create_bucket(BUCKET_NAME)
    
    if not os.path.exists(TEMP_DIR): 
        os.makedirs(TEMP_DIR)

    # Limpa a pasta temporária de resíduos de execuções anteriores
    for f in os.listdir(TEMP_DIR):
        if f.endswith((".zip", ".crdownload", ".tmp")):
            os.remove(os.path.join(TEMP_DIR, f))

    # 2. Configurações do Navegador (Headless)
    chrome_options = Options()
    chrome_options.add_argument("--headless=new")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.binary_location = "/usr/bin/chromium"
    
    driver = webdriver.Chrome(service=Service("/usr/bin/chromedriver"), options=chrome_options)
    driver.execute_cdp_cmd("Page.setDownloadBehavior", {"behavior": "allow", "downloadPath": TEMP_DIR})
    
    # Lista para evitar baixar o mesmo arquivo após o upload/delete
    arquivos_processados = []

    try:
        print(f"🌐 Iniciando em: {URL_BASE}")
        driver.get(URL_BASE)
        time.sleep(10)

        # 3. NAVEGAÇÃO ATÉ A PASTA 2025-12 (Com rolagem agressiva)
        caminho = ["Dados", "Cadastros", "CNPJ", "2025-12"]
        for pasta in caminho:
            print(f"📂 Procurando pasta: {pasta}")
            clicado = False
            for i in range(50): # Tentativas de scroll para Lazy Load
                try:
                    xpath = f"//tr[contains(@data-cy-files-list-row-name, '{pasta}')]//button | //a[text()='{pasta}']"
                    btn = driver.find_element(By.XPATH, xpath)
                    if btn.is_displayed():
                        driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", btn)
                        time.sleep(2)
                        driver.execute_script("arguments[0].click();", btn)
                        print(f"✔️ Entrou na pasta: {pasta}")
                        clicado = True
                        time.sleep(8) # Aguarda carregar conteúdo da pasta
                        break
                except:
                    # Rola o container interno ou a janela para forçar o carregamento
                    driver.execute_script("""
                        var c = document.querySelector('#app-content') || document.querySelector('.files-list') || window;
                        c.scrollBy(0, 500);
                    """)
                    time.sleep(1)
            if not clicado: raise Exception(f"Pasta {pasta} não encontrada após várias rolagens.")

        # 4. LOOP DE DOWNLOAD DE ARQUIVOS
        print("🔍 Iniciando busca por arquivos ESTABELECIMENTOS...")
        while True:
            # Rola para garantir que o Lazy Load carregue todos os itens da lista
            driver.execute_script("var c = document.querySelector('#app-content') || window; c.scrollBy(0, 3000);")
            time.sleep(5)

            # Captura todos os elementos 'Estabelecimentos' carregados
            elementos = driver.find_elements(By.XPATH, "//tr[contains(@data-cy-files-list-row-name, 'Estabelecimentos')]")
            
            # Monta lista de nomes e ordena
            lista_nomes = sorted([el.get_attribute("data-cy-files-list-row-name") for el in elementos if el.get_attribute("data-cy-files-list-row-name")])

            # Seleciona o primeiro que ainda não foi processado
            nome_alvo = next((n for n in lista_nomes if n not in arquivos_processados), None)
            
            if not nome_alvo:
                print("🏁 Fim da lista. Todos os arquivos ESTABELECIMENTOS foram processados.")
                break

            print(f"📥 Iniciando download: {nome_alvo}...")
            
            # Localiza o botão específico do arquivo alvo
            xpath_btn = f"//tr[@data-cy-files-list-row-name='{nome_alvo}']//button[contains(@class, 'files-list__row-name-link')]"
            btn_down = driver.find_element(By.XPATH, xpath_btn)
            driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", btn_down)
            time.sleep(2)
            driver.execute_script("arguments[0].click();", btn_down)

            # 5. MONITORAMENTO DO DOWNLOAD
            path_file = os.path.join(TEMP_DIR, nome_alvo)
            start_wait = time.time()
            last_size = -1

            while True:
                time.sleep(30)
                arquivos_pasta = os.listdir(TEMP_DIR)
                
                # Verifica se existe o arquivo final e se o temporário sumiu
                baixando = any(f.endswith(".crdownload") or f.endswith(".tmp") for f in arquivos_pasta)
                concluido = os.path.exists(path_file)

                if concluido and not baixando:
                    print(f"✅ Download finalizado: {nome_alvo}")
                    break
                
                # Log de progresso (mostra o tamanho crescendo)
                try:
                    arquivo_atual = [f for f in arquivos_pasta if nome_alvo in f][0]
                    tamanho_atual = os.path.getsize(os.path.join(TEMP_DIR, arquivo_atual))
                    if tamanho_atual > last_size:
                        print(f"   ... progresso {nome_alvo}: {tamanho_atual // (1024*1024)} MB")
                        last_size = tamanho_atual
                except:
                    pass

                if (time.time() - start_wait) > 14400: # Timeout 4h
                    raise Exception(f"Timeout no download de {nome_alvo}")

            # 6. UPLOAD, LIMPEZA E MARCAÇÃO
            upload_to_minio(path_file, nome_alvo)
            arquivos_processados.append(nome_alvo)

    except Exception as e:
        driver.save_screenshot(f"{TEMP_DIR}/erro_fatal.png")
        print(f"💥 ERRO: {e}")
        raise e
    finally:
        print("Encerrando navegador...")
        driver.quit()

# --- DAG ---
with DAG(
    'estabelecimentos',
    default_args={'owner': 'marcelo', 'retries': 0},
    schedule_interval=None, 
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['cnpj', 'estabelecimentos']
) as dag:

    PythonOperator(
        task_id='baixar_e_upload_minio',
        python_callable=baixar_apenas_estabelecimentos
    )
