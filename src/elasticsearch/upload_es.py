import jsonlines
from src.utility import dict_tools
from src.utility import io
from src.utility import elasticsearch
import pandas as pd

#json_file = r'C:\Showcase\Projekt\M-HH-scripts\data\.part-7-0.inprogress-1.be815de0-9ed6-42b3-b110-77d6fd932f66'
json_file = r'C:\Showcase\Projekt\M-HH-scripts\data\car-eu-sample.json'
parquet_file = r'C:\Showcase\Projekt\M-HH-scripts\data\dm-analysis\car-avg.parquet'

pd.read_json
pdf = io.read_file_to_pdf(json_file, "json_lines")
elastic = elasticsearch.convert_to_json_elastic(
    pdf.to_dict(orient='records'), "id", True)
elasticsearch.upload_bulk_to_es("localhost", 9200, elastic, "car") #
