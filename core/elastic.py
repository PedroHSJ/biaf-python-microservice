from elasticsearch import Elasticsearch
from config.settings import ELASTICSEARCH_HOST, ELASTICSEARCH_PASS, ELASTICSEARCH_USER

def get_elastic_client() -> Elasticsearch:
    """
    Retorna o cliente Elasticsearch configurado.
    """
    return Elasticsearch(
                    ELASTICSEARCH_HOST,
                    basic_auth=(ELASTICSEARCH_USER, ELASTICSEARCH_PASS),
                    request_timeout=60)

def index_data(index: str, document_id: str, body: dict) -> None:
    """
    Indexa os dados no Elasticsearch.
    Args:
        index (str): Nome do índice.
        document_id (str): ID do documento.
        body (dict): Dados do documento.
    """
    client = get_elastic_client()
    client.index(index=index, id=document_id, body=body)
