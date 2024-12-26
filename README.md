# CNPJ CSV Processing Microservice

Este projeto é um microserviço para processamento de arquivos CSV de CNPJ, utilizando RabbitMQ para receber mensagens de processamento e Elasticsearch para armazenar os dados processados.

## Requisitos

- Python 3.8+
- RabbitMQ
- Elasticsearch

## Como enviar uma mensagem para a fila de processamento

```bash

python -c "import pika, json; \
RABBITMQ_HOST = 'localhost'; \
RABBITMQ_PORT = 5672; \
RABBITMQ_USER = 'admin'; \
RABBITMQ_PASS = 'admin'; \
QUEUE_NAME = 'cnpj_csv_processing_queue'; \
message = {'index_name': 'cnpj_index', 'directory': 'c:/tmp', 'chunksize': 1000, 'max_workers': 4}; \
credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS); \
connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST, port=RABBITMQ_PORT, credentials=credentials)); \
channel = connection.channel(); \
channel.basic_publish(exchange='', routing_key=QUEUE_NAME, body=json.dumps(message)); \
print('Mensagem enviada para a fila', QUEUE_NAME); \
connection.close()"

```
