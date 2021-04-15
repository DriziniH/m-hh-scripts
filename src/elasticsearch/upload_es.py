import jsonlines
from src.utility import dict_tools
from src.utility import io
from src.utility import elasticsearch
import pandas as pd

#json_file = r'C:\Showcase\Projekt\M-HH-scripts\data\.part-7-0.inprogress-1.be815de0-9ed6-42b3-b110-77d6fd932f66'
json_file = r'C:\Showcase\Projekt\M-HH-scripts\src\elasticsearch\car-eu.json'
parquet_file = r'C:\Showcase\Projekt\M-HH-scripts\data\dm-analysis\car-avg.parquet'

data = []
with jsonlines.open(json_file) as reader:
            for json_line in reader.iter(skip_invalid=True):
                data.append({"index": {"_id": json_line["id"] }})
                data.append(json_line)


es = Elasticsearch(hosts=[{'host': "localhost", 'port': 9200}])
es.bulk(body=data, index="car", pretty=True)
