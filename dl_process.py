import pandas as pd
import numpy as np
from pyspark.sql import SparkSession
from datetime import datetime
import pyarrow as pa
import pyarrow.parquet as pq
from os import walk

from src.utility import format_conversion
from src.utility import io
from src.utility.logger import logger

import os

# json_file = r"C:\Showcase\Projekt\M-HH-spark\data\car-eu-sample.json"
#json_file = r'C:\Showcase\Projekt\M-HH-scripts\data\flink\raw\.part-7-0.inprogress-1.be815de0-9ed6-42b3-b110-77d6fd932f66'
#json_file = r'C:\Showcase\Projekt\M-HH-scripts\data\flink\waiting-time\part-1-0.inprogress.62edcc47-2e19-4bdf-a6ab-99a97e7c90f9'
flink_path_raw = r'C:\Showcase\Projekt\M-HH-scripts\data\flink\raw'
flink_path_waiting_time = r'C:\Showcase\Projekt\M-HH-scripts\data\flink\waiting-time'
destination = r'C:\Showcase\Projekt\M-HH-scripts\data\car-parquet'

_, _, filenames = next(walk(flink_path_waiting_time))

processed_data = []

try:

    for json_file in filenames:
        data_json = io.read_json_lines(flink_path_waiting_time + "/" + json_file, False)
        for row in data_json:
            row["value"].update({"timestamp": row["timestamp"]})
            row = row["value"]
            row.update({
                "year": datetime.now().year,
                "month": datetime.now().month,
                "day": datetime.now().day,
            })
            processed_data.append(row)

    pdf = pd.DataFrame(processed_data)
    # Read data and create df
    df_table = pa.Table.from_pandas(pdf)
    pq.write_to_dataset(df_table, root_path=destination,
                        partition_cols=["year", "month", "day"])

except Exception as e:
    logger.error(f'Error converting json to pandas: {str(e)}')
    pdf = []
