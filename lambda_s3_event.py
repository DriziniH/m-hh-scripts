import jsonlines
from src.utility import dict_tools
from src.utility import io
from src.utility import elasticsearch
import pandas as pd
import boto3
from urllib.parse import unquote_plus
import uuid
import os

#json_file = r'C:\Showcase\Projekt\M-HH-scripts\data\.part-7-0.inprogress-1.be815de0-9ed6-42b3-b110-77d6fd932f66'
json_file = r'C:\Showcase\Projekt\M-HH-scripts\data\car-eu-sample.json'
parquet_file = r'C:\Showcase\Projekt\M-HH-scripts\data\dm-analysis\car-avg.parquet'

s3_client = boto3.client('s3')


#Function handler
def lambda_handler(event, context):
    for record in event['Records']:
        bucket = record['s3']['bucket']['name']
        key = unquote_plus(record['s3']['object']['key'])
        tmpkey = key.replace('/', '')
        download_path = '/tmp/{}{}'.format(uuid.uuid4(), tmpkey)
        filename, file_extension= os.path.splitext(download_path)
        s3_client.download_file(bucket, key, download_path)

        pdf = io.read_file_to_pdf(download_path, "json_lines")
        body = elasticsearch.convert_to_json_elastic(pdf.to_dict(orient='records'), "id", True)
        elasticsearch.upload_bulk_to_es("localhost", 9200, elastic, filename)


pdf = io.read_file_to_pdf(json_file, "json")
elastic = elasticsearch.convert_to_json_elastic(
    pdf.to_dict(orient='records'), "id", True)
elasticsearch.upload_bulk_to_es("localhost", 9200, elastic, "car") #
