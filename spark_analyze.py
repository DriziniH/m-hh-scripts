import pandas as pd
from pyspark.sql import SparkSession, functions
from pyspark import SparkConf, SparkContext
from datetime import datetime
import json

from src.utility import io, elasticsearch, spark_util

#spark = spark_util.create_spark_aws_session()

# Create spark session
spark = SparkSession.builder.appName(
    'Python Spark').config("spark.driver.memory", "2g").getOrCreate()

year = "2021"
month = "4"
day = "20"

# 's3a://eu-consumer'
source_path = f'C:\Showcase\Projekt\M-HH-scripts\data\consumer\car\year={year}\month={month}\day={day}'
destination = f'C:\Showcase\Projekt\M-HH-scripts\data\\analytics\year={year}\month={month}\day={day}'

df = spark.read.parquet(source_path)
df_grouped_car = df.groupBy(df['id'])


# Average/Min/Max values of cars
df_car_avg = df_grouped_car.avg('kmh', 'rpm', 'oilLevel', 'breakFluidLevel', 'fuelLevel', 'temperatureEngine', 'temperatureInside',
                                'temperatureOutside', 'temperatureBreaks', 'temperatureTires', 'gasPower', 'breakPower', 'consumptionKm')
df_car_max = df_grouped_car.max('kmh', 'rpm', 'oilLevel', 'breakFluidLevel', 'fuelLevel', 'temperatureEngine', 'temperatureInside',
                                'temperatureOutside', 'temperatureBreaks', 'temperatureTires', 'gasPower', 'breakPower', 'consumptionKm')


df_car_breaking = df.filter(df['breakActive'] == True).groupBy(df['id']).count(
).withColumn("breakingPercentage", (functions.col("count") / df.count()))

df_car_pos = df.select('id', 'timestamp', 'lat', 'lon')

df_car_avg = spark_util.rename_columns('avg', df_car_avg)
df_car_max = spark_util.rename_columns('max', df_car_max)

# Persist data to consumer bucket - data ready for consumption
df_car_avg.write.mode('append').parquet(f'{destination}/car-avg.parquet')
df_car_max.write.mode('append').parquet(f'{destination}/car-max.parquet')
df_car_pos.write.mode('append').parquet(f'{destination}/car-pos.parquet')
df_car_breaking.write.mode('append').parquet(f'{destination}/car-breaking.parquet')

# Transform to json lines
car_avg_json_lines = list(map(json.loads, df_car_avg.toJSON().collect()))
car_max_json_lines = list(map(json.loads, df_car_max.toJSON().collect()))
df_car_pos_json_lines = list(map(json.loads, df_car_pos.toJSON().collect()))
df_car_breaking_json_lines = list(map(json.loads, df_car_breaking.toJSON().collect()))

def update_time_information(json_lines, year, month, day):
    for row in json_lines:
        row.update({
            "year": year,
            "month": month,
            "day": day,
        })
    return json_lines

#Append date information
car_avg_json_lines = update_time_information(car_avg_json_lines, year, month, day)
car_max_json_lines = update_time_information(car_max_json_lines, year, month, day)
df_car_pos_json_lines = update_time_information(df_car_pos_json_lines, year, month, day)
df_car_breaking_json_lines = update_time_information(df_car_breaking_json_lines, year, month, day)

# Convert to ElasticSearch Bulk Format
car_avg_elastic = elasticsearch.convert_to_json_elastic(
    car_avg_json_lines, ["id","year","month","day"])
car_max_elastic = elasticsearch.convert_to_json_elastic(
    car_max_json_lines, ["id","year","month","day"])
car_pos_elastic = elasticsearch.convert_to_json_elastic(
    df_car_pos_json_lines, ["id","timestamp"])
car_breaking_elastic = elasticsearch.convert_to_json_elastic(
    df_car_breaking_json_lines, ["id","year","month","day"])

# Upload to ElasticSearch
elasticsearch.upload_bulk_to_es("localhost", 9200, car_avg_elastic, "car_avg")
elasticsearch.upload_bulk_to_es("localhost", 9200, car_max_elastic, "car_max")
elasticsearch.upload_bulk_to_es("localhost", 9200, car_pos_elastic, "car_pos")
elasticsearch.upload_bulk_to_es("localhost", 9200, car_breaking_elastic, "car_breaking")
