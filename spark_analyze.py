import pandas as pd
from pyspark.sql import SparkSession, functions
from pyspark import SparkConf, SparkContext
from datetime import datetime
import json
import jsonlines

from src.utility import io, elasticsearch, spark_util

spark = spark_util.create_spark_aws_session()

# 's3a://eu-consumer'
s3_path = 'C:\Showcase\Projekt\M-HH-scripts\data\consumer\car'
destination = 'C:\Showcase\Projekt\M-HH-scripts\data\\analytics\metrics'

df = spark.read.parquet(s3_path)
df_grouped = df.groupBy(df['id'], df["year"], df["month"], df["day"])

df_car_avg = df_grouped.avg('kmh', 'rpm', 'oilLevel', 'breakFluidLevel', 'fuelLevel', 'temperatureEngine', 'temperatureInside',
                                'temperatureOutside', 'temperatureBreaks', 'temperatureTires', 'gasPower', 'breakPower', 'consumptionKm')
df_car_max = df_grouped.max('kmh', 'rpm', 'oilLevel', 'breakFluidLevel', 'fuelLevel', 'temperatureEngine', 'temperatureInside',
                                'temperatureOutside', 'temperatureBreaks', 'temperatureTires', 'gasPower', 'breakPower', 'consumptionKm')

df_car_avg = spark_util.rename_columns('avg', df_car_avg)
df_car_max = spark_util.rename_columns('max', df_car_max)

df_car_breaking = df.filter(df['breakActive'] == True).groupBy(df['id'], df["year"], df["month"], df["day"]).count(
).withColumn("breakingPercentage", (functions.col("count") / df.count()))

df_car_pos = df.filter(df['id']=='300b0247-632d-4401-97e7-f86f5fb7e8d3').select('id', 'timestamp', 'lat', 'lon', 'year','month','day')

# Persist data to consumer bucket - data ready for consumption
df_car_avg.write.mode('append').partitionBy(
    "year", "month", "day").parquet(f'{destination}/car-avg')
df_car_max.write.mode('append').partitionBy(
    "year", "month", "day").parquet(f'{destination}/car-max')
df_car_pos.write.mode('append').partitionBy(
    "year", "month", "day").parquet(f'{destination}/car-pos')
df_car_breaking.write.mode('append').partitionBy(
    "year", "month", "day").parquet(f'{destination}/car-breaking')

# Transform to json lines
car_avg_json_lines = list(map(json.loads, df_car_avg.toJSON().collect()))
car_max_json_lines = list(map(json.loads, df_car_max.toJSON().collect()))
df_car_pos_json_lines = list(map(json.loads, df_car_pos.toJSON().collect()))
df_car_breaking_json_lines = list(
    map(json.loads, df_car_breaking.toJSON().collect()))

# Convert to ElasticSearch Bulk Format
car_avg_elastic = elasticsearch.convert_to_json_elastic(
    car_avg_json_lines, ["id", "year", "month", "day"])
car_max_elastic = elasticsearch.convert_to_json_elastic(
    car_max_json_lines, ["id", "year", "month", "day"])
car_pos_elastic = elasticsearch.convert_to_json_elastic(
    df_car_pos_json_lines, ["id", "timestamp"])
car_breaking_elastic = elasticsearch.convert_to_json_elastic(
    df_car_breaking_json_lines, ["id", "year", "month", "day"])


# Upload to ElasticSearch
elasticsearch.upload_bulk_to_es("localhost", 9200, car_avg_elastic, "car-avg")
elasticsearch.upload_bulk_to_es("localhost", 9200, car_max_elastic, "car-max")
elasticsearch.upload_bulk_to_es("localhost", 9200, car_pos_elastic, "car-pos")
elasticsearch.upload_bulk_to_es(
    "localhost", 9200, car_breaking_elastic, "car-breaking")
    
