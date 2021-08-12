import json
import jsonlines
import pandas as pd
from datetime import datetime
import os

from src.utility import io, elasticsearch, spark_util

source = "C:/Masterarbeit/data/usa/processed/"
target = "C:/Masterarbeit/data/usa/analyzed/"

pdf = pd.DataFrame(io.read_local_car_data(source, True))

df_avg_mile = pdf.groupby('model').mean('consumptionMile')
df_max_mph = pdf.groupby('model').max('mph')

# Resets the index and adds it back as column (model), select columns and convert to dict list
df_avg_mile = df_avg_mile.reset_index()[["model", "consumptionMile"]].to_dict(orient="records")
df_max_mph = df_max_mph.reset_index()[["model", "mph"]].to_dict(orient="records")


avg_mile_elastic = elasticsearch.convert_to_json_elastic(
    df_avg_mile, ['model'])
max_mph_elastic = elasticsearch.convert_to_json_elastic(
    df_max_mph, ['model'])

with jsonlines.open(target + "avg_mile_bulk.json", 'w') as writer:
    writer.write_all(avg_mile_elastic)
with jsonlines.open(target + "max_mph_bulk.json", 'w') as writer:
    writer.write_all(max_mph_elastic)

# elasticsearch.upload_bulk_to_es(
#     "bu-aut.ausy-technologies.de/data-mesh-elasticsearch", 9200, avg_mile_elastic, "mile-avg-usa")
# elasticsearch.upload_bulk_to_es(
#     "bu-aut.ausy-technologies.de/data-mesh-elasticsearch", 9200, max_mph_elastic, "mph-max-usa")
