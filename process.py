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

json_file = r"C:\Showcase\Projekt\M-HH-spark\data-lake\S3-eu_landing\json\year=2021\month=1\day=28\car-eu-sample.json"
destination = r'C:\Showcase\Projekt\M-HH-spark\data-lake\S3-eu_consumer\car-parquet'

#Read data and create df
pdf = format_conversion.json_lines_to_pdf(json_file, datetime.now())
df = spark.createDataFrame(pdf)
df.printSchema()


#Process data here

#Persist data to consumer bucket - data ready for consumption
df.write.partitionBy("year","month","day").mode("append").parquet(destination)