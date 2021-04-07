import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as psf
from datetime import datetime
import json
import plotly.express as px
import plotly.graph_objs as go

from src.utility import format_conversion, io, elasticsearch, graph_tools


# Silence spark logger
import logging
s_logger = logging.getLogger('py4j.java_gateway')
s_logger.setLevel(logging.ERROR)

# Create spark session
spark = SparkSession.builder.appName('Python Spark').config(
    key="spark.driver.memory", value="2g").getOrCreate()
sc = spark.sparkContext

parquet_file = r'C:\Showcase\Projekt\M-HH-scripts\data\car-parquet'
destination = r'C:\Showcase\Projekt\M-HH-scripts\data\dm-analysis'

df = spark.read.parquet(parquet_file)
df_grouped_car = df.groupBy(df['id'])


# Wearing parts
df_wearing_parts = df.select("id", "timestamp", "model", "breaksHealth",
                             "mufflerHealth", "engineHealth", "tireHealth", "gearsHealth", "batteryHealth")

# Persist data to analytics bucket - data ready for consumption
df_wearing_parts.write.mode('append').parquet(
    f'{destination}/wearing-parts.parquet')

# Wearing per time per part for all cars
df_wearing_parts_asc = df_wearing_parts.sort(psf.asc("timestamp"))
pdf_wearing_parts_asc = df_wearing_parts.toPandas()
pdf_wearing_parts_asc["timestamp"] = pd.to_datetime(pdf_wearing_parts_asc["timestamp"], unit='ms').tolist()
fig = px.line(pdf_wearing_parts_asc, x="timestamp", y=[
              "breaksHealth", "mufflerHealth", "engineHealth", "tireHealth", "gearsHealth", "batteryHealth"], title="Wearing parts")
fig.show()

# Persist data in ElasticSearch json format
# wearing_parts_json_lines = list(
#     map(json.loads, df_wearing_parts.toJSON().collect()))
# wearing_parts_elastic = elasticsearch.convert_to_json_elastic(
#     wearing_parts_json_lines, ["id", "timestamp"])
# io.write_json_lines(f'{destination}/wearing-parts.json',
#                     "a", wearing_parts_elastic)
