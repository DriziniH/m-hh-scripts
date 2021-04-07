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



