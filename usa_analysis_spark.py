import json
import jsonlines
import pandas as pd
from datetime import datetime
import os 

from src.utility import io, elasticsearch, spark_util

source="C:/Masterarbeit/data/usa/processed/"
target="C:/Masterarbeit/data/usa/analyzed/"

spark = spark_util.create_spark_aws_session()




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

os.chdir(r"C:\Masterarbeit\data\usa\raw")
for file in os.listdir():
    with open(file, 'r') as f:
        for json_line in jsonlines.Reader(f.read().split()).iter(skip_invalid=True):
                row = update_time_information(
                    json_line["value"], json_line["timestamp"])  # json_line (raw)
                data.append(row)


pdf = pd.DataFrame(data)
df = spark.createDataFrame(pdf)
#df = spark.read.parquet(source)

df_avg_mile = df.groupBy(df['model']).avg('consumptionMile')
df_max_mph = df.groupBy(df['model']).max('mph')

df_avg_mile = spark_util.rename_columns('avg', df_avg_mile)
df_max_mph = spark_util.rename_columns('max', df_max_mph)

avg_mile_json_lines = list(map(json.loads, df_avg_mile.toJSON().collect()))
max_mph_json_lines = list(map(json.loads, df_max_mph.toJSON().collect()))

avg_mile_elastic = elasticsearch.convert_to_json_elastic(
    avg_mile_json_lines, ['model'])
max_mph_elastic = elasticsearch.convert_to_json_elastic(
    max_mph_json_lines, ['model'])

with jsonlines.open(target + "avg_mile_bulk", 'w') as writer:
    writer.write_all(avg_mile_elastic)
with jsonlines.open(target + "max_mph_bulk", 'w') as writer:
    writer.write_all(max_mph_elastic)

# elasticsearch.upload_bulk_to_es(
#     "bu-aut.ausy-technologies.de/data-mesh-elasticsearch", 9200, avg_mile_elastic, "mile-avg-usa")
# elasticsearch.upload_bulk_to_es(
#     "bu-aut.ausy-technologies.de/data-mesh-elasticsearch", 9200, max_mph_elastic, "mph-max-usa")
