import json
import pandas as pd
from datetime import datetime

from src.utility import io
from src.utility.logger import logger


def json_lines_to_pdf(path_json, dt = None):    
    """Writes json lines from source path to pandas dataframe with time information

    Args:
        path_json (String): Source path with json data
        dt (datetime)

    Returns:
        pdf (Pandas data frame)
    """
    try:
        data_json = io.read_json_lines(path_json, True)
        if dt:
            for row in data_json:
                row.update({
                    "year": dt.year,
                    "month": dt.month,
                    "day": dt.day,
                })
        pdf = pd.DataFrame(data_json)
    except Exception as e:
        logger.error(f'Error converting json to pandas: {str(e)}')
        return []

    return pdf


def write_csv_to_parquet(path_csv, path_parquet):
    """Reads json data, converts it to pandas dataframe and writes it to destination path as csv

    Args:
        path_csv (String): Source path
        path_parquet (String): Destination path
    """

    df = pd.read_csv(path_csv)
    if df.empty:
        logger.error(f'File <{path_csv}> is empty!')
    if not df.to_parquet(path_parquet):
        logger.error(
            f'Failed to transform csv file <{path_csv}> to parquet!')

    logger.info(f'Succesfully persisted parquet data to <{path_parquet}>')
    return True
