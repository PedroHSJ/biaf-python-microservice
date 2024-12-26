import traceback
import pika
from config.settings import QUEUE_NAME, RABBITMQ_HOST, RABBITMQ_PASS, RABBITMQ_PORT, RABBITMQ_USER
from core.rabbitmq import start_rabbitmq_consumer
from core.utils import interactive_menu


if __name__ == "__main__":
    try:
        # Configurações do processamento
        chunksize = interactive_menu("Selecione o tamanho do chunk para processamento:", [1000, 2000, 5000, 10000, 25000, 50000])
        max_workers = interactive_menu("Selecione o número máximo de workers para processamento:", [1, 2, 4, 8, 16, 32])

        # Configuração do RabbitMQ
        credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST, port=RABBITMQ_PORT, credentials=credentials))
        channel = connection.channel()
        channel.queue_declare(queue=QUEUE_NAME, durable=True)

        # Iniciando o consumidor RabbitMQ com partial para passar chunksize e max_workers
        start_rabbitmq_consumer(channel, QUEUE_NAME, chunksize, max_workers)
    except KeyboardInterrupt:
        print('Encerrando o consumidor RabbitMQ...')
        connection.close()
    except Exception as e:
        print(f"Erro ao iniciar o consumidor RabbitMQ: {e}")
        traceback.print_exc()
        connection.close()