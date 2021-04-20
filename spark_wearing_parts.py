import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions
from pyspark.sql.functions import col
from datetime import datetime
import json
import plotly.express as px
import plotly.graph_objs as go

from src.utility import format_conversion, io, elasticsearch, spark_util

spark = spark_util.create_spark_aws_session()

year = "2021"
month = "4"
day = "20"

# year={year}\month={month}\day={day}' #s3a://eu-consumer/car-data/parquet/
source_path = f'C:\Showcase\Projekt\M-HH-scripts\data\consumer\car\\'
destination = f'C:\Showcase\Projekt\M-HH-scripts\data\\analytics\year={year}\month={month}\day={day}'


df = spark.read.parquet(source_path)

grouped_df = df.groupBy(df['id'], df["year"], df["month"], df["day"])

df_car_wearing_parts = grouped_df.max("breaksHealth", "mufflerHealth", "engineHealth", "tireHealth", "gearsHealth", "batteryHealth")
df_car_wearing_parts = spark_util.rename_columns("max", df_car_wearing_parts)

# Get Minimum and maximum health status of each car with each month
df_wearing_parts_min_max = grouped_df.agg(functions.min(df["breaksHealth"]), functions.min(df["mufflerHealth"]), functions.min(df["engineHealth"]), functions.min(df["tireHealth"]), functions.min(df["gearsHealth"]),
                                                                             functions.min(df["batteryHealth"]), functions.max(df["breaksHealth"]), functions.max(df["mufflerHealth"]), functions.max(df["engineHealth"]), functions.max(df["tireHealth"]), functions.max(df["gearsHealth"]), functions.max(df["batteryHealth"]))

# Compute average decrease of health per day
df_wearing_parts_min_max = df_wearing_parts_min_max.withColumn("breaksHealthDecrease", (
    df_wearing_parts_min_max["max(breaksHealth)"] - df_wearing_parts_min_max["min(breaksHealth)"]))
df_wearing_parts_min_max = df_wearing_parts_min_max.withColumn("mufflerHealthDecrease", (
    df_wearing_parts_min_max["max(mufflerHealth)"] - df_wearing_parts_min_max["min(mufflerHealth)"]))
df_wearing_parts_min_max = df_wearing_parts_min_max.withColumn("engineHealthDecrease", (
    df_wearing_parts_min_max["max(engineHealth)"] - df_wearing_parts_min_max["min(engineHealth)"]))
df_wearing_parts_min_max = df_wearing_parts_min_max.withColumn("tireHealthDecrease", (
    df_wearing_parts_min_max["max(tireHealth)"] - df_wearing_parts_min_max["min(tireHealth)"]))
df_wearing_parts_min_max = df_wearing_parts_min_max.withColumn("gearsHealthDecrease", (
    df_wearing_parts_min_max["max(gearsHealth)"] - df_wearing_parts_min_max["min(gearsHealth)"]))
df_wearing_parts_min_max = df_wearing_parts_min_max.withColumn("batteryHealthDecrease", (
    df_wearing_parts_min_max["max(batteryHealth)"] - df_wearing_parts_min_max["min(batteryHealth)"]))

df_wearing_parts_min_max = df_wearing_parts_min_max.groupBy(df_wearing_parts_min_max["year"], df_wearing_parts_min_max["month"],  df_wearing_parts_min_max["day"]).avg(
    "breaksHealthDecrease", "mufflerHealthDecrease", "engineHealthDecrease", "tireHealthDecrease", "gearsHealthDecrease", "batteryHealthDecrease")

spark_util.rename_columns("avg", df_wearing_parts_min_max)

car_json_lines = list(map(json.loads, df_car_wearing_parts.toJSON().collect()))
decrease_json_lines = list(
    map(json.loads, df_wearing_parts_min_max.toJSON().collect()))

# Convert to ElasticSearch Bulk Format
car_elastic = elasticsearch.convert_to_json_elastic(
    car_json_lines, ["id", "year", "month", "day"])
decrease_elastic = elasticsearch.convert_to_json_elastic(
    decrease_json_lines, ["year", "month", "day"])

elasticsearch.upload_bulk_to_es(
    "localhost", 9200, car_elastic, "car_health")
elasticsearch.upload_bulk_to_es(
    "localhost", 9200, decrease_elastic, "wearing_parts")
