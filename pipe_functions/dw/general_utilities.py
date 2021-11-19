import pandas as pd
import awswrangler as wr
from datetime import datetime
import json

def get_query_from_file(filepath:str)->str:
    file = open(filepath,'r')
    return file.read()

def formatted_element(element):
    if isinstance(element, str):
        return f"'{element}'"
    elif isinstance(element, datetime):
        return "'{datetime}'".format(datetime = element.isoformat())
    elif isinstance(element, date):
        return "'{date}'".format(date = element.isoformat())
    else:
        return f"{element}"

def simplify_dict_structure(elements_dict:dict):
    new_dict = dict()
    for element in elements_dict.keys():
        new_dict[element] = elements_dict[element]['Value']

    return new_dict

def clean_body(body:str):
    json_body = json.loads(body)
    return pd.DataFrame(json_body['pedido'])

def get_dimensions():
    return [
        'cliente',
        'filial',
        'parceiro',
        'produto'
    ]
