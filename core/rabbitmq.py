from functools import partial
import json
from core.utils import cnpj_load

def callback(ch, method, properties, body, chunksize, max_workers):
    """
    Callback que será chamado quando uma mensagem for recebida do RabbitMQ.
    """
    print("Recebida mensagem do RabbitMQ:", body)
    message = json.loads(body)
    index_name = message['index_name']
    directory = message['directory']
    cnpj_load(index_name, directory, chunksize, max_workers)
    ch.basic_ack(delivery_tag=method.delivery_tag)

def start_rabbitmq_consumer(channel, queue_name, chunksize, max_workers):
    """
    Inicia o consumidor RabbitMQ.
    """
    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue=queue_name, on_message_callback=partial(callback, chunksize=chunksize, max_workers=max_workers))

    print('Aguardando mensagens do RabbitMQ. Para sair, pressione CTRL+C')
    channel.start_consuming()
