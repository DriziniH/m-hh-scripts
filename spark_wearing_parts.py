import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions
from pyspark.sql.functions import col
from datetime import datetime
import json
import plotly.express as px
import plotly.graph_objs as go

from src.utility import format_conversion, io, elasticsearch, spark_util

destination = ""

spark = spark_util.create_spark_aws_session()

#s3a://eu-consumer/car-data/parquet/
source_path = 'C:\Showcase\Projekt\M-HH-scripts\data\consumer\car'


df = spark.read.parquet(source_path)
grouped_df = df.groupBy(df['id'], df["year"], df["month"], df["day"])

#Get aggregated max and min values per car and date
df_car_wearing_parts_max = grouped_df.max(
    "breaksHealth", "mufflerHealth", "engineHealth", "tireHealth", "gearsHealth", "batteryHealth")
df_car_wearing_parts_min = grouped_df.min(
    "breaksHealth", "mufflerHealth", "engineHealth", "tireHealth", "gearsHealth", "batteryHealth")

#Count entries per car and date
df_grouped_count = grouped_df.count()

#Join MIN and MAX values with amount of aggregated rows
df_wearing_parts_min_max = df_car_wearing_parts_max.join(
    df_car_wearing_parts_min, on=['id', 'year', 'month', 'day'], how="full_outer")
df_wearing_parts_min_max = df_wearing_parts_min_max.join(
    df_grouped_count, on=['id', 'year', 'month', 'day'], how="full_outer")

df_car_wearing_parts_min = spark_util.rename_columns(
    "min", df_car_wearing_parts_min)

# Compute average decrease of health per car and date
df_wearing_parts_min_max = df_wearing_parts_min_max.withColumn("breaksHealthDecrease", (
    (df_wearing_parts_min_max["max(breaksHealth)"] - df_wearing_parts_min_max["min(breaksHealth)"])/df_wearing_parts_min_max["count"]))
df_wearing_parts_min_max = df_wearing_parts_min_max.withColumn("mufflerHealthDecrease", (
    (df_wearing_parts_min_max["max(mufflerHealth)"] - df_wearing_parts_min_max["min(mufflerHealth)"])/df_wearing_parts_min_max["count"]))
df_wearing_parts_min_max = df_wearing_parts_min_max.withColumn("engineHealthDecrease", (
    (df_wearing_parts_min_max["max(engineHealth)"] - df_wearing_parts_min_max["min(engineHealth)"])/df_wearing_parts_min_max["count"]))
df_wearing_parts_min_max = df_wearing_parts_min_max.withColumn("tireHealthDecrease", (
    (df_wearing_parts_min_max["max(tireHealth)"] - df_wearing_parts_min_max["min(tireHealth)"])/df_wearing_parts_min_max["count"]))
df_wearing_parts_min_max = df_wearing_parts_min_max.withColumn("gearsHealthDecrease", (
    (df_wearing_parts_min_max["max(gearsHealth)"] - df_wearing_parts_min_max["min(gearsHealth)"])/df_wearing_parts_min_max["count"]))
df_wearing_parts_min_max = df_wearing_parts_min_max.withColumn("batteryHealthDecrease", (
    (df_wearing_parts_min_max["max(batteryHealth)"] - df_wearing_parts_min_max["min(batteryHealth)"])/df_wearing_parts_min_max["count"]))

# Compute total average per wearing part
df_wearing_parts_decrease = df_wearing_parts_min_max.groupBy(df_wearing_parts_min_max["year"], df_wearing_parts_min_max["month"],  df_wearing_parts_min_max["day"]).avg(
    "breaksHealthDecrease", "mufflerHealthDecrease", "engineHealthDecrease", "tireHealthDecrease", "gearsHealthDecrease", "batteryHealthDecrease")

# Persist data to consumer bucket - data ready for consumption
df_car_wearing_parts_max.write.mode('append').partitionBy("year", "month", "day").parquet(f'{destination}/wearing-parts-car')
df_wearing_parts_decrease.write.mode('append').partitionBy("year", "month", "day").parquet(f'{destination}/wearing-parts-decrease')


# Convert to ElasticSearch Bulk Format
car_json_lines = list(map(json.loads, df_car_wearing_parts_min.toJSON().collect()))
decrease_json_lines = list(map(json.loads, df_wearing_parts_decrease.toJSON().collect()))

car_elastic = elasticsearch.convert_to_json_elastic(car_json_lines, ["id", "year", "month", "day"])
decrease_elastic = elasticsearch.convert_to_json_elastic(decrease_json_lines, ["year", "month", "day"])

elasticsearch.upload_bulk_to_es("localhost", 9200, car_elastic, "wearing-parts-car")
elasticsearch.upload_bulk_to_es("localhost", 9200, decrease_elastic, "wearing-parts-decrease")
