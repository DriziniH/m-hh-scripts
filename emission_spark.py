import pandas as pd
from pyspark.sql import SparkSession, functions
from pyspark import SparkConf, SparkContext
from datetime import datetime
import json
import jsonlines

from src.utility import io, elasticsearch, spark_util

s3_path_eu = 's3a:/eu-consumer/car/parquet'
s3_path_usa = 's3a:/usa-consumer/car'
destination = 's3a:/eu-analysis/car/parquet'

spark = spark_util.create_spark_aws_session()

df_eu = spark.read.parquet(s3_path_eu)
df_usa = spark.read.parquet(s3_path_usa)

df_eu = df_eu.select('model', 'consumptionKm', 'co2Km')
df_usa = df_usa.select('model', 'consumptionMile', 'co2Mile')

#Transform usa specific data to match unit of measurement
df_usa = df_usa.withColumn('consumptionKm', df_usa['consumptionMile']/1.6)
df_usa = df_usa.withColumn('co2Km', df_usa['co2Mile']/1.6)
df_usa = df_usa.drop('consumptionMile')
df_usa = df_usa.drop('co2Mile')

df_union = df_eu.union(df_usa)

df_results_consumption = df_union.groupBy('model').avg("consumptionKm")
df_results_emission = df_union.groupBy('model').avg("co2Km")

df_results = df_results_consumption.join(df_results_emission, on=['model'], how="full_outer")
df_results = spark_util.rename_columns('avg', df_results)

df_results.write.mode('append').parquet(f'{destination}/consumption-emission.parquet')

# Convert and index to ES
results_json_lines = list(map(json.loads, df_results.toJSON().collect()))
results_elastic = elasticsearch.convert_to_json_elastic(results_json_lines, ['model'])
elasticsearch.upload_bulk_to_es("localhost", 9200, results_elastic, "consumption-emission")
