import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as psf
from datetime import datetime
import json
import plotly.express as px
import plotly.graph_objs as go

from src.utility import format_conversion, io, elasticsearch, spark_util

spark = spark_util.create_spark_aws_session()


df = spark.read.parquet("s3a://eu-consumer/car-data/parquet/")

df_car_wearing_parts_avg = df.groupBy(df['id'], df["year"], df["month"], df["day"]).avg("breaksHealth",
                                                                              "mufflerHealth", "engineHealth", "tireHealth", "gearsHealth", "batteryHealth")
df_total_wearing_parts_avg = df.groupBy(df["year"], df["month"], df["day"]).avg("breaksHealth",
                                                                              "mufflerHealth", "engineHealth", "tireHealth", "gearsHealth", "batteryHealth")

df_car_wearing_parts_avg = spark_util.rename_columns("avg", df_car_wearing_parts_avg)
df_total_wearing_parts_avg = spark_util.rename_columns("avg", df_total_wearing_parts_avg)

car_avg_json_lines = list(map(json.loads, df_car_wearing_parts_avg.toJSON().collect()))
total_avg_json_lines = list(map(json.loads, df_total_wearing_parts_avg.toJSON().collect()))

# Convert to ElasticSearch Bulk Format
car_avg_elastic = elasticsearch.convert_to_json_elastic(
    car_avg_json_lines, ["id","year","month","day"])
total_avg_elastic = elasticsearch.convert_to_json_elastic(
    total_avg_json_lines, ["year","month","day"])


elasticsearch.upload_bulk_to_es("localhost", 9200, car_avg_elastic, "car_wearing_parts_avg")
elasticsearch.upload_bulk_to_es("localhost", 9200, total_avg_elastic, "total_wearing_parts_avg")
