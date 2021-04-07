import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions
from datetime import datetime
import json

from src.utility import format_conversion, io, elasticsearch


# Silence spark logger
import logging
s_logger = logging.getLogger('py4j.java_gateway')
s_logger.setLevel(logging.ERROR)

# Create spark session
spark = SparkSession.builder.appName('Python Spark').config(key="spark.driver.memory",value="2g").getOrCreate()
sc = spark.sparkContext

parquet_file = r'C:\Showcase\Projekt\M-HH-scripts\data\car-parquet'
destination = r'C:\Showcase\Projekt\M-HH-scripts\data\dm-analysis'

df = spark.read.parquet(parquet_file)
df_grouped_car = df.groupBy(df['id'])

# Average/Min/Max values of cars
df_car_avg = df_grouped_car.avg('kmh', 'rpm', 'oilLevel', 'breakFluidLevel', 'fuelLevel', 'temperatureEngine', 'temperatureInside',
                                'temperatureOutside', 'temperatureBreaks', 'temperatureTires', 'gasPower', 'breakPower')
df_car_max = df_grouped_car.max('kmh', 'rpm', 'oilLevel', 'breakFluidLevel', 'fuelLevel', 'temperatureEngine', 'temperatureInside',
                                'temperatureOutside', 'temperatureBreaks', 'temperatureTires', 'gasPower', 'breakPower')
df_car_min = df_grouped_car.min('kmh', 'rpm', 'oilLevel', 'breakFluidLevel', 'fuelLevel', 'temperatureEngine', 'temperatureInside',
                                 'temperatureOutside', 'temperatureBreaks', 'temperatureTires', 'gasPower', 'breakPower')

def rename_columns(expr, df):
    for column in df.columns:
        name = column[column.find('(')+1:column.find(')')]
        df = df.withColumnRenamed(f'{expr}({name})',f'{expr}_{name}')
    print(df)
    return df

df_car_avg = rename_columns('avg', df_car_avg)
df_car_max = rename_columns('max', df_car_max)
df_car_min = rename_columns('min', df_car_min)

# Position of cars
df_car_pos = df_grouped_car.agg(functions.collect_list(
    'lat').alias('lat'), functions.collect_list('lon').alias('lon'))


# Persist data to analytics bucket - data ready for consumption
df_car_avg.write.mode('append').parquet(f'{destination}/car-avg.parquet')
df_car_max.write.mode('append').parquet(f'{destination}/car-max.parquet')
df_car_min.write.mode('append').parquet(f'{destination}/car-min.parquet')
df_car_pos.write.mode('append').parquet(f'{destination}/car-pos.parquet')

# Transform to json lines
car_avg_json_lines = list(map(json.loads, df_car_avg.toJSON().collect()))
car_max_json_lines = list(map(json.loads, df_car_max.toJSON().collect()))
car_min_json_lines = list(map(json.loads, df_car_min.toJSON().collect()))
car_pos_json_lines = list(map(json.loads, df_car_pos.toJSON().collect()))

# Convert to ElasticSearch Bulk Format
car_avg_elastic = elasticsearch.convert_to_json_elastic(car_avg_json_lines, ["id"])
car_max_elastic = elasticsearch.convert_to_json_elastic(car_max_json_lines, ["id"])
car_min_elastic = elasticsearch.convert_to_json_elastic(car_min_json_lines, ["id"])
car_pos_elastic = elasticsearch.convert_to_json_elastic(car_pos_json_lines, ["id"])

# Persist
io.write_json_lines(f'{destination}/car-avg.json', "a", car_avg_elastic)
io.write_json_lines(f'{destination}/car-max.json', "a", car_max_elastic)
io.write_json_lines(f'{destination}/car-min.json', "a", car_min_elastic)
io.write_json_lines(f'{destination}/car-pos.json', "a", car_pos_elastic)



