import os
import errno
import pandas as pd
from datetime import datetime
import jsonlines

from src.utility.logger import logger
from src.utility import dict_tools

def read_local_car_data(path, append_time_information):
    def update_time_information(row, timestamp):
        date = datetime.fromtimestamp(timestamp/1000.0)
        row.update({
            "timestamp": timestamp,
            "year": date.year,
            "month": date.month,
            "day": date.day,
        })
        return row


    data = []

    os.chdir(path)
    for file in os.listdir():
        with open(file, 'r') as f:
            for json_line in jsonlines.Reader(f.read().split()).iter(skip_invalid=True):
                row = update_time_information(
                    json_line["value"], json_line["timestamp"])  # json_line (raw)
                data.append(row)
    return data


def create_path(path):
    if not os.path.exists(os.path.dirname(path)):
        try:
            os.makedirs(os.path.dirname(path))
        except OSError as exc:  # Guard against race condition
            if exc.errno != errno.EEXIST:
                logger.error(exc)
                raise
        except Exception as e:
            logger.error(f'Error creating path <{path}> : {str(e)}')
            return False


def write_data(path, mode, data):
    """Creates dirs if not existent and writes data in given mode

    Args:
        path (String): Path to write to
        mode (String): Write mode
        data (?): any file

    Returns:
        Boolean: Write operation successfull or not
    """

    try:
        create_path(path)

        with open(path, mode) as f:
            f.write(data)

    except Exception as e:
        logger.error(e)
        return False

    return True


def read_json_to_dict(path):
    try:
        with open(path) as f:
            return dict_tools.load_json_to_dict(f.read())
    except Exception as e:
        logger.error(f'Error reading json to dict : {str(e)}')
        return {}


def write_json_lines(path, mode, data):
    try:
        create_path(path)

        with jsonlines.open(path, mode=mode) as writer:
            writer.write(data)

    except Exception as e:
        logger.error(f'Error writing json lines to <{path}> : {str(e)}')
        return False

    return True


def read_json_lines(path, flatten=True):
    data = []
    try:
        with jsonlines.open(path) as reader:
            for json_line in reader.iter(skip_invalid=True):
                if flatten:
                    json_line = dict_tools.flatten_json(json_line)
                data.append(json_line)
    except Exception as e:
        logger.error(f'Error reading json lines from <{path}> : {str(e)}')
        return []
    return data


pandas_format_conversion = {
    "csv": pd.read_csv,
    "parquet": pd.read_parquet,
    "json": pd.read_json
}

def read_file_to_pdf(file, file_type):
    if file_type == "json_lines":
        return pandas_format_conversion[file_type](file, lines=True)
    else:
        return pandas_format_conversion[file_type]
