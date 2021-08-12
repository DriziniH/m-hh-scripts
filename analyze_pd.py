import pandas as pd
import json
import jsonlines 
import os
from src.utility import elasticsearch,io
from datetime import datetime

source = r"C:\Masterarbeit\data\eu\raw\raw"
target = r'C:\Masterarbeit\data\eu\analyzed\\'


pdf = pd.DataFrame(io.read_local_car_data(source, True))

df_grouped = pdf.groupby(['id','year', 'month', 'day'])

df_car_avg = df_grouped.mean()
df_car_max = df_grouped.max()
df_car_pos = pdf[pdf.id == '300b0247-632d-4401-97e7-f86f5fb7e8d3'][['id', 'timestamp', 'lat', 'lon', 'year','month','day']]

# Transform to json lines
car_avg_json_lines = df_car_avg.reset_index().to_dict(orient="records") #[["model", "consumptionMile"]]
car_max_json_lines = df_car_max.reset_index().to_dict(orient="records")
df_car_pos_json_lines = df_car_pos.to_dict(orient="records") 

# Convert to ElasticSearch Bulk Format
car_avg_elastic = elasticsearch.convert_to_json_elastic(
    car_avg_json_lines, ["id", "year", "month", "day"])
car_max_elastic = elasticsearch.convert_to_json_elastic(
    car_max_json_lines, ["id", "year", "month", "day"])
car_pos_elastic = elasticsearch.convert_to_json_elastic(
    df_car_pos_json_lines, ["id", "timestamp"])

with jsonlines.open(target + "car_avg_bulk.json", 'w') as writer:
    writer.write_all(car_avg_elastic)
with jsonlines.open(target + "car_max_bulk.json", 'w') as writer:
    writer.write_all(car_max_elastic)  
with jsonlines.open(target + "car_pos_bulk.json", 'w') as writer:
    writer.write_all(car_pos_elastic)  