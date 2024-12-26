import os
import datetime
import shutil
import time
import zipfile
import inquirer
import pandas as pd
import requests
from tqdm import tqdm
from config.settings import DOWNLOAD_FILE
from core.elastic import get_elastic_client
from dto.legal_person import transform_row_to_entity
import concurrent.futures
import traceback

def cnpj_load(index_name, directory, chunksize, max_workers):
    try:
        es = get_elastic_client()
        download_files(directory, 'Empresas')
        download_files(directory, 'Estabelecimentos')
    except Exception as e:
        print(f"Erro ao baixar arquivos: {e}")
        return
    
    if not es.indices.exists(index=index_name):
        es.indices.create(index=index_name)

    # TODOS OS ARQUIVOS ZIP NA PASTA
    for filename in os.listdir(directory):
        if filename.endswith(".zip") and (filename.startswith("Estabelecimentos") or filename.startswith("Empresas")):
            file_path = os.path.join(directory, filename)
            process_csv(file_path, index_name, chunksize, max_workers)
        else:
            print(f"Arquivo {filename} não é um arquivo ZIP.\n")

def interactive_menu(prompt, options):
    question = [
        inquirer.List(
            'choice',
            message=prompt,
            choices=options,
        )
    ]
    answer = inquirer.prompt(question)
    return int(answer['choice'])

def process_chunk(chunk, index_name, down_file, worker_id):
    bulk_data = []
    cnpjs = [row[0] for row in chunk.itertuples(index=False, name=None)]  # Coleta os CNPJs de todas as linhas do chunk
    
    # Consulta em massa ao Elasticsearch para todos os CNPJs do chunk
    query = {
        "query": {
            "terms": {
                "cnpj_base": cnpjs  # Consulta múltiplos CNPJs de uma vez
            }
        },
        "size": len(cnpjs)  # Define o tamanho da consulta para retornar todos os resultados
    }
    es = get_elastic_client()
    pj = es.search(index=index_name, body=query)
    
    # Mapeia os dados retornados pela busca
    pj_data_map = {hit['_source']['cnpj_base']: hit['_source'] for hit in pj['hits']['hits']} if pj['hits']['hits'] else {}

    # Processa cada linha do chunk
    for row in chunk.itertuples(index=False, name=None):
        cnpj_base = row[0]  # O CNPJ da linha
        pj_data = pj_data_map.get(cnpj_base)  # Obtém os dados do Elasticsearch usando o CNPJ como chave

        # Processa a linha
        entity = transform_row_to_entity(row, down_file, pj_data)
        if entity:
            bulk_data.append({
                "index": {
                    "_id": entity.id  # Supondo que 'id' é um atributo único do 'entity'
                }
            })
            bulk_data.append(entity.__dict__)
    
    # Envia os dados para o Elasticsearch em batch
    if bulk_data:
        try:
            start_time = time.time()
            print(f"Worker {worker_id} está enviando {len(chunk)} linhas para o Elasticsearch.\n")
            response = es.bulk(index=index_name, body=bulk_data)
            end_time = time.time()
        except Exception as e:
            end_time = time.time()
            print(f"Worker {worker_id} com erro ao enviar dados para o Elasticsearch em {end_time - start_time:.2f} segundos.\n")
            print(e)

    print(f"Worker {worker_id} processou {len(chunk)} linhas em {end_time - start_time:.2f} segundos.\n")
    return len(chunk)

def process_csv(file_path, index_name, chunksize, max_workers):
    start_time = time.time()
    print("Processamento iniciado as", datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    down_file = 'Estabelecimentos' if 'Estabelecimentos' in file_path else 'Empresas'
    date_time = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
    temp_dir = f'temp_csv_files_{os.path.basename(file_path)}_{date_time}'
    try:
        extract_csv_from_zip(file_path, temp_dir)
        for csv_file in os.listdir(temp_dir):
            csv_path = os.path.join(temp_dir, csv_file)

            # Lendo o CSV em chunks
            with pd.read_csv(csv_path, sep=';', header=None, encoding='latin1', chunksize=chunksize, low_memory=False) as df:
                file_start_time = time.time()
                file_total_lines = 0
                with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:  # Especificando 4 workers
                    
                     futures = {executor.submit(process_chunk, chunk, index_name, down_file, i): i for i, chunk in enumerate(tqdm(df, desc=f"Processando chunks do arquivo {csv_file}", unit=" chunks", colour="green"))}
                    
                     for future in concurrent.futures.as_completed(futures):
                         file_total_lines += future.result()

                print(f"Processado {file_total_lines} linhas do arquivo {csv_file} em {time.time() - file_start_time:.2f} segundos.")

    except Exception as e:
        print(f"Erro ao processar {file_path}: {e}\n")
        traceback.print_exc()
        if os.path.exists(temp_dir):
            try:
                shutil.rmtree(temp_dir, ignore_errors=True)
                print(f"A pasta '{temp_dir}' foi removida com sucesso.\n")
            except Exception as e:
                print(f"Ocorreu um erro ao tentar remover a pasta: {e}\n")
        else:
            print(f"A pasta '{temp_dir}' não existe.")
        print(f"Processado {file_total_lines} linhas do arquivo {csv_file} em {time.time() - file_start_time:.2f} segundos.\n")

    finally:
        print(f"Processamento finalizado em {time.time() - start_time:.2f} segundos.\n")
        if os.path.exists(temp_dir):
            try:
                shutil.rmtree(temp_dir, ignore_errors=True)
                print(f"A pasta '{temp_dir}' foi removida com sucesso.\n")
            except Exception as e:
                print(f"Ocorreu um erro ao tentar remover a pasta: {e}\n")
        else:
            print(f"A pasta '{temp_dir}' não existe ou já foi removida.\n")

def download_files(directory, downfile):
    try:
        os.makedirs(directory, exist_ok=True)
        while(True):
            month = datetime.datetime.now().strftime("%m")
            year = datetime.datetime.now().strftime("%Y")

            #VERIFICANDO SE A PASTA DO ANO E MÊS EXISTE NO SERVIDOR DA RECEITA FEDERAL
            url_file_exist = DOWNLOAD_FILE + str(year) + "-" + str(month)
            try:
                response = requests.get(url_file_exist)
                if response.status_code == 404:
                    print(f"Arquivos do mês {month} do ano {year} não estão disponíveis no servidor da Receita Federal.\n")
                    return
            except Exception as e:
                print(f"Erro ao verificar a existência de arquivos no servidor da Receita Federal: {e}\n")
                raise e

            file_index = 0
            url_file = DOWNLOAD_FILE + str(downfile) + str(file_index) + '.zip'
            print(f"Baixando arquivos de {url_file} para {directory}...\n")
            #download stream
            response = requests.get(url_file, stream=True)
            if response.status_code == 200:
                file_index += 1
                print(f"Arquivo {url_file} baixado com sucesso.\n")
            if response.status_code == 404:
                break

    except Exception as e:
        print(f"Erro ao baixar arquivos da Receita Federal: {e}\n")
        raise e


    # Implementar download de arquivos
    print(f"Arquivos baixados para {directory}.\n")

def extract_csv_from_zip(file_path, temp_dir):
    with zipfile.ZipFile(file_path, 'r') as zip_ref:
        os.makedirs(temp_dir, exist_ok=True)
        total_size = os.path.getsize(file_path)
        # Extraindo arquivos com tqdm
        with tqdm(total=total_size, unit='B', unit_scale=True, desc="Extraindo arquivos") as pbar:
            for member in zip_ref.infolist():
                file_size = member.file_size
                zip_ref.extract(member, temp_dir)
                pbar.update(file_size)
        
        print(f"Arquivo {file_path} extraído para a pasta {temp_dir}.\n")