from datetime import datetime
from utility import format_conversion, io
from elasticsearch import Elasticsearch

es = Elasticsearch(hosts=[{'host': 'localhost', 'port': 9200}])

json_file = r"C:\Showcase\Projekt\m-hh-elasticsearch\data\car-eu-sample.json"
destination = r'C:\Showcase\Projekt\m-hh-elasticsearch\data\cars-bulk.json'

data_json = io.read_json_lines(json_file, True)
bulk_json = []

for line in data_json:
    io.write_json_lines(destination, "a", {"index":{"_id":line["id"] + str(line["timestamp"])}})
    io.write_json_lines(destination, "a", line)
    bulk_json.append({"index": {"_id": line["id"] + str(line["timestamp"])}})
    bulk_json.append(line)


# es.bulk(body=bulk_json, index="car", pretty=True)
