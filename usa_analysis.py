import pandas as pd
from pyspark.sql import SparkSession, functions
from pyspark import SparkConf, SparkContext
from datetime import datetime
import json
import jsonlines

from src.utility import io, elasticsearch, spark_util

spark = spark_util.create_spark_aws_session()
df = spark.read.parquet("s3a:/usa-consumer/car")

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

elasticsearch.upload_bulk_to_es(
    "localhost", 9200, avg_mile_elastic, "mile-avg-usa")
elasticsearch.upload_bulk_to_es(
    "localhost", 9200, max_mph_elastic, "mph-max-usa")
