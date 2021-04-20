import jsonlines
from src.utility import dict_tools
from src.utility import io
from src.utility import elasticsearch
import pandas as pd

json_file = r'C:\Showcase\Projekt\M-HH-scripts\src\elasticsearch\car-eu.json'

data = []
with jsonlines.open(json_file) as reader:
            for json_line in reader.iter(skip_invalid=True):
                data.append(json_line)

elastic_data = elasticsearch.convert_to_json_elastic(data, ["id","timestamp"])
elasticsearch.upload_bulk_to_es("localhost", 9200, elastic_data, "car")

