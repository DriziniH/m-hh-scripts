import json
import ast

from src.utility.logger import logger

def recursive_extraction(data):
    """Recusivly adds all data and types from data to json string

    Args:
        data (dict): data

    Returns:
        schema [String]: Schema as JSON String
    """
    schema = ""

    for key, value in data.items():

        schema += "{\n"
        schema += f'"name": "{key}",\n'

        if type(value) is dict:
            schema += f'"type" : "{type(value).__name__}",\n'
            schema += f'"fields": [{recursive_extraction(value)}]\n'
        else:
            schema += f'"type" : "{type(value).__name__}"\n'

        # dont add semicolon to last entry
        *_, last = data
        if key != last:
            schema += "},\n"
        else:
            schema += "}\n"

    return schema


def load_json_to_dict(json_string):
    try:
        return json.loads(json_string)
    except Exception as json_e:
        try:
            return ast.literal_eval(json_string)
        except Exception as ast_e:
            logger.warning("JSON ERROR: ", json_e)
            logger.warning("AST ERROR: ", ast_e)
            return {}

def extract_schema(name, data):
    """Takes data from a dict and extracts it data and types to another dict

    Args:
        name (String): Name of schema
        data (dict): data

    Returns:
        dict: schema
    """
    schema = '{\n "name": "'
    schema += name
    schema += '",\n"fields": ['
    schema += recursive_extraction(data)
    schema = schema[:-1]
    schema += "]}"

    try:
        return json.loads(schema)
    except Exception as e:
        logger.error(f'Error extracting schema: {str(e)}')
        return schema


def contains_struct(data):
    for entry in data:
        if type(entry) is dict:
            return True
    return False

def flatten_json(data):
    out = {}

    def flatten(data, name=''):
        if type(data) is dict:
            for key in data:
                flatten(data[key], name + key + '.')
        elif type(data) is list and contains_struct(data):
            i = 0
            for key in data:
                flatten(key, name + str(i) + '.')
                i += 1
        else:
            out[name[:-1]] = data

    flatten(data)
    return out