import json
import urllib.parse
import boto3
from elasticsearch import Elasticsearch
import os

print('Loading function')

s3 = boto3.client('s3')


def lambda_handler(event, context):

    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    filename = os.path.splitext(key)[0]
    
    try:
        response = s3.get_object(Bucket=bucket, Key=key)
        
        json_string = response["Body"].read().decode()
        
        data = []
        for line in iter(json_string.splitlines()):
            data.append(json.loads(line))

        
        es = Elasticsearch(hosts=[{'host': '192.168.178.58', 'port': 9200}])
        es.bulk(body=data, index=filename, pretty=True)
        
        return 'Success'
        
    except Exception as e:
        print(e)
        print(f'Error getting object {key} from bucket {bucket}: {str(e)}')
        raise e


