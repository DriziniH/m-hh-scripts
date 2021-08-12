import pandas as pd
from pyspark.sql import SparkSession, functions
from pyspark import SparkConf, SparkContext
from datetime import datetime
import json
import jsonlines

from src.utility import io, elasticsearch, spark_util

source_usa = "C:/Masterarbeit/data/usa/raw/"
source_eu = r"C:\Masterarbeit\data\eu\raw\raw"
target = "C:/Masterarbeit/data/eu/analyzed/"

pdf_eu = pd.DataFrame(io.read_local_car_data(source_eu, True))[['model', 'consumptionKm', 'co2Km']]
pdf_usa = pd.DataFrame(io.read_local_car_data(source_usa, True))[['model', 'consumptionMile', 'co2Mile']]

#Transform usa specific data to match unit of measurement
pdf_usa["consumptionMile"] = pdf_usa["consumptionMile"].div(1.6)
pdf_usa["co2Mile"] = pdf_usa["co2Mile"].div(1.6)

pdf_usa.columns = ['model','consumptionKm', 'co2Km']

df_union = pdf_eu.append(pdf_usa)
df_mean = df_union.groupby('model').mean()
records = df_mean.reset_index().to_dict(orient="records")

# Convert and index to ES
results_elastic = elasticsearch.convert_to_json_elastic(records, ['model'])
with jsonlines.open(target + "emission_bulk.json", 'w') as writer:
    writer.write_all(results_elastic) 