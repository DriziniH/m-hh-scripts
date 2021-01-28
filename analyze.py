import pandas as pd
from pyspark.sql import SparkSession
from datetime import datetime

from src.utility import format_conversion


#Silence spark logger
import logging
s_logger = logging.getLogger('py4j.java_gateway')
s_logger.setLevel(logging.ERROR) 

#Create spark session
spark = SparkSession.builder.appName("Python Spark").getOrCreate()
sc =spark.sparkContext

parquet_file = r"C:\Showcase\Projekt\M-HH-spark\data-lake\S3-eu_consumer\car-parquet"
destination = r'C:\Showcase\Projekt\M-HH-spark\data-lake\S3-eu_analytics\car-analysis'

df = spark.read.parquet(parquet_file)


df.filter(df.kmh > 180.0).groupBy(df["id"]).max().show()
