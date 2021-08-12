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

hdfs_paths = ['/flink/car/2021-08-06--07']
bucket_destination = "C:\Masterarbeit\data\ds1"#"s3://eu-consumer/car/parquet"
index = "car"


hdfs_client = InsecureClient('http://localhost:9870', user="hoeinghe")
data = []

for hdfs_path in hdfs_paths:
    # Get all files under a given folder
    fpaths = [
        psp.join(dpath, fname)
        for dpath, _, fnames in hdfs_client.walk(hdfs_path)
        for fname in fnames
    ]

    def update_time_information(row, timestamp):
        date = datetime.fromtimestamp(timestamp/1000.0)
        row.update({
            "timestamp": timestamp,
            "year": date.year,
            "month": date.month,
            "day": date.day,
        })
        return row

    # Download files, convert and enrich lines to json and append to data array
    for file_name in fpaths:
        with hdfs_client.read(file_name) as reader:
            for json_line in jsonlines.Reader(reader.read().decode().split()).iter(skip_invalid=True):
                row = update_time_information(
                    json_line["value"], json_line["timestamp"])  # json_line (raw)
                data.append(row)

# # Persist data to partitioned parquet file based on date
pdf = pd.DataFrame(data)
pq.write_to_dataset(pa.Table.from_pandas(pdf), bucket_destination,
                    # filesystem=S3FileSystem(),
                    partition_cols=["year", "month", "day"])

# # Index files to elastic search
# elastic_data = elasticsearch.convert_to_json_elastic(
#     data[0:200], ["id", "timestamp"], False)
# elasticsearch.upload_bulk_to_es("localhost", 9200, elastic_data, index)
