import pandas as pd
from pyspark.sql import SparkSession, functions
from pyspark import SparkConf, SparkContext
from datetime import datetime
import json

from src.utility import io, elasticsearch, spark_util

spark = spark_util.create_spark_aws_session()

df = spark.read.parquet("s3a://eu-consumer/car-data/parquet/")
df_grouped_car = df.groupBy(df['id'])
destination = 's3a://eu-consumer'

# Car locations
df_car_pos = df_grouped_car.agg(functions.collect_list(
    'lat').alias('lat'), functions.collect_list('lon').alias('lon'))

# Average/Min/Max values of cars
df_car_avg = df_grouped_car.avg('kmh', 'rpm', 'oilLevel', 'breakFluidLevel', 'fuelLevel', 'temperatureEngine', 'temperatureInside',
                                'temperatureOutside', 'temperatureBreaks', 'temperatureTires', 'gasPower', 'breakPower' , 'consumptionKm')
df_car_max = df_grouped_car.max('kmh', 'rpm', 'oilLevel', 'breakFluidLevel', 'fuelLevel', 'temperatureEngine', 'temperatureInside',
                                'temperatureOutside', 'temperatureBreaks', 'temperatureTires', 'gasPower', 'breakPower' , 'consumptionKm')
df_car_min = df_grouped_car.min('kmh', 'rpm', 'oilLevel', 'breakFluidLevel', 'fuelLevel', 'temperatureEngine', 'temperatureInside',
                                'temperatureOutside', 'temperatureBreaks', 'temperatureTires', 'gasPower', 'breakPower' , 'consumptionKm')

df_car_avg = spark_util.rename_columns('avg', df_car_avg)
df_car_max = spark_util.rename_columns('max', df_car_max)
df_car_min = spark_util.rename_columns('min', df_car_min)

# Persist data to analytics bucket - data ready for consumption
# df_car_avg.write.mode('append').parquet(f'{destination}/car-avg.parquet')
# df_car_max.write.mode('append').parquet(f'{destination}/car-max.parquet')
# df_car_min.write.mode('append').parquet(f'{destination}/car-min.parquet')
# df_car_pos.write.mode('append').parquet(f'{destination}/car-pos.parquet')

# Transform to json lines
car_avg_json_lines = list(map(json.loads, df_car_avg.toJSON().collect()))
car_max_json_lines = list(map(json.loads, df_car_max.toJSON().collect()))
car_min_json_lines = list(map(json.loads, df_car_min.toJSON().collect()))
#car_pos_json_lines = list(map(json.loads, df_car_pos.toJSON().collect()))


# Convert to ElasticSearch Bulk Format
car_avg_elastic = elasticsearch.convert_to_json_elastic(
    car_avg_json_lines, ["id"])
car_max_elastic = elasticsearch.convert_to_json_elastic(
    car_max_json_lines, ["id"])
car_min_elastic = elasticsearch.convert_to_json_elastic(
    car_min_json_lines, ["id"])
# car_pos_elastic = elasticsearch.convert_to_json_elastic(
#     car_pos_json_lines, ["id"])

# Upload to ElasticSearch
elasticsearch.upload_bulk_to_es("localhost", 9200, car_avg_elastic, "car_avg")
elasticsearch.upload_bulk_to_es("localhost", 9200, car_max_elastic, "car_max")
elasticsearch.upload_bulk_to_es("localhost", 9200, car_min_elastic, "car_min")
#elasticsearch.upload_bulk_to_es("localhost", 9200, car_pos_elastic, "car_pos")

