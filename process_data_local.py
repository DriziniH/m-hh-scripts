import pandas as pd
import jsonlines
from datetime import datetime
import os
import pyarrow as pa
import pyarrow.parquet as pq

from src.utility.logger import logger
from src.utility import elasticsearch

source=r"C:\Masterarbeit\data\eu\raw\car"
target=r"C:\Masterarbeit\data\eu\processed"

index = "car"

def update_time_information(row, timestamp):
    date = datetime.fromtimestamp(timestamp/1000.0)
    row.update({
        "timestamp": timestamp,
        "year": date.year,
        "month": date.month,
        "day": date.day,
    })
    return row

data = []

os.chdir(source)
for file in os.listdir():
    with open(file, 'r') as f:
        for json_line in jsonlines.Reader(f.read().split()).iter(skip_invalid=True):
                row = update_time_information(
                    json_line["value"], json_line["timestamp"])  # json_line (raw)
                data.append(row)

pdf = pd.DataFrame(data)
pq.write_to_dataset(pa.Table.from_pandas(pdf), target,
                    # filesystem=S3FileSystem(),
                    partition_cols=["year", "month", "day"])

# Index files to elastic search
elastic_data = elasticsearch.convert_to_json_elastic(
    data[0:200], ["id", "timestamp"], False)
with jsonlines.open(r'C:\Masterarbeit\data\eu\raw\bulk\ds5_bulk.json', 'w') as writer:
    writer.write_all(elastic_data)
# elasticsearch.upload_bulk_to_es("bu-aut.ausy-technologies.de/data-mesh-elasticsearch", 9200, elastic_data, index)
