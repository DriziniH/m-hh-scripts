from src.utility import io
from src.utility.logger import logger

from elasticsearch import Elasticsearch


def convert_to_json_elastic(data, index_keys, remove_index_keys = False):
    """Iterates over dict array and creates elastic bulk file with index

    Args:
        data (list): List with dicts representing json data
        index_keys (list): Keys in data to use for indexing in elasticsearch
        remove_index_keys (Boolean): Whether to remove the keys from the data
   """
    elastic_bulk = []

    try:

        for line in data:
            index = ""

            for index_key in index_keys:
                if remove_index_keys:
                    index += str(line.pop(index_key,""))
                else:
                    index += str(line[index_key])

            elastic_bulk.append({"index": {"_id": index }})
            elastic_bulk.append(line)

    except Exception as e:
        logger.error(
            f'Error converting data to ElasticSearch json format: {str(e)}')

    return elastic_bulk


def upload_bulk_to_es(host, port, body, index):
    """Takes connection parameters and uploads the body to the given index

    Args:
        host (String): Host address of ElasticSearch Cluster
        port (int): Port of ElasticSearch Cluster
        body (list): List with dicts containing index information and data
        index (String): Top level index
    """
    try:
        es = Elasticsearch(hosts=[{'host': host, 'port': port}])
        es.bulk(body=body, index=index, pretty=True)
    except Exception as e:
        logger.error(
            f'Error uploading bulk with index {index} to ElasticSearch: {str(e)}')
