import pandas as pd
import jsonlines
from datetime import datetime
import pyarrow as pa
import pyarrow.parquet as pq
from hdfs import InsecureClient
import posixpath as psp
from s3fs import S3FileSystem

from src.utility.logger import logger
from src.utility import elasticsearch

hdfs_path = '/flink/car/waiting-time/2021-04-08--10/'

bucket_destination = "s3://eu-consumer/waiting-time/2021-04-08--10.parquet"

hdfs_client = InsecureClient('http://localhost:9870', user="hoeinghe")
data = []

# Get all files under a given folder
fpaths = [
    psp.join(dpath, fname)
    for dpath, _, fnames in hdfs_client.walk(hdfs_path)
    for fname in fnames
]

# Download files, convert and enrich lines to json and append to data array
for file_name in fpaths:
    with hdfs_client.read(file_name) as reader:
        for json_line in jsonlines.Reader(reader.read().decode().split()).iter(skip_invalid=True):
            data.append(json_line)

# Persist data to partitioned parquet file based on date
pdf = pd.DataFrame(data)
#pdf.to_parquet(bucket_destination,engine="pyarrow")

# Index files to elastic search
elastic_data = elasticsearch.convert_to_json_elastic(data, ["id", "timestamp"], False)
elasticsearch.upload_bulk_to_es("localhost", 9200, elastic_data, "waiting-time")




