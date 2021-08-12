import pandas as pd
from datetime import datetime
import json
import jsonlines
import os

from src.utility import format_conversion, io, elasticsearch, spark_util



source = r"C:\Masterarbeit\data\eu\raw\raw"
target = r'C:\Masterarbeit\data\eu\analyzed\\'

pdf = pd.DataFrame(io.read_local_car_data(source, True))

df_grouped = pdf.groupby(['id','year', 'month', 'day'])

#Get aggregated max and min values per car and date
df_car_wearing_parts_max = df_grouped.max()[["breaksHealth", "mufflerHealth", "engineHealth", "tireHealth", "gearsHealth", "batteryHealth"]]
df_car_wearing_parts_min = df_grouped.min()[["breaksHealth", "mufflerHealth", "engineHealth", "tireHealth", "gearsHealth", "batteryHealth"]]


#Count entries per car and date
df_wearing_parts_decrease = (df_car_wearing_parts_max.subtract(df_car_wearing_parts_min, fill_value=0))/len(df_car_wearing_parts_max.index)
# Compute total average per wearing part
df_wearing_parts_decrease = df_wearing_parts_decrease.groupby(['year', 'month', 'day']).mean()

# Convert to ElasticSearch Bulk Format
car_json_lines =  df_car_wearing_parts_min.reset_index().to_dict(orient="records")
decrease_json_lines = df_wearing_parts_decrease.reset_index().to_dict(orient="records")

car_elastic = elasticsearch.convert_to_json_elastic(car_json_lines, ["id", "year", "month", "day"])
decrease_elastic = elasticsearch.convert_to_json_elastic(decrease_json_lines, ["year", "month", "day"])

with jsonlines.open(target + "wearing_parts_car_bulk.json", 'w') as writer:
    writer.write_all(car_elastic)
with jsonlines.open(target + "wearing_parts_bulk.json", 'w') as writer:
    writer.write_all(decrease_elastic)
