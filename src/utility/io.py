import os
import errno
import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd
from datetime import datetime
import jsonlines

from src.utility.logger import logger
from src.utility import dict_tools

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


def read_json_lines(path,flatten):
    data = []
    try:
        with jsonlines.open(path) as reader:
            for json_line in reader:
                if flatten:
                    json_line = dict_tools.flatten_json(json_line)
                data.append(json_line)
    except Exception as e:
        logger.error(f'Error reading json lines from <{path}> : {str(e)}')
        return []
    return data