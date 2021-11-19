from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
import pandas as pd
from datetime import datetime, date

def get_query_from_file(filepath:str)->str:
    file = open(filepath,'r')
    return file.read()

def connect_sqlite(path_sqlite:str)->Engine:
    filename_comp = f'sqlite:///{path_sqlite}'
    engine = create_engine(filename_comp)
    return engine

def run_sql_query(query:str, engine:Engine)->pd.DataFrame:
    frame = pd.read_sql(query, con = engine)
    return frame

def get_limit_offset_query(
    query:str,
    limit:int,
    offset:int
)->str:
    adjusted_query = f"""
        {query}
        LIMIT {limit}
        OFFSET {offset}
    """
    return adjusted_query

def formatted_element(element):
    if isinstance(element, str):
        return f"'{element}'"
    elif isinstance(element, datetime):
        return "'{datetime}'".format(datetime = element.isoformat())
    elif isinstance(element, date):
        return "'{date}'".format(date = element.isoformat())
    else:
        return f"{element}"

def get_in_filter_query(
    query:str,
    list_elements:list,
    field:str
):
    formatted_list_elements = ",".join([formatted_element(element) for element in list_elements])
    formatted_query = f"""
        {query}
        WHERE {field} IN ({formatted_list_elements})
    """
    return formatted_query

def in_filter_read_table(
    query:str,
    engine:Engine,
    list_elements:list,
    field:str
):
    formatted_query = get_in_filter_query(query, list_elements = list_elements, field = field)
    frame = run_sql_query(engine = engine, query = formatted_query)
    return frame

def batch_read_table(
    query:str,
    engine:Engine,
    batch_size:int = 400,
    **kwargs
)->pd.DataFrame:
    offset = 0
    query = query.format(**kwargs)
    while True:
        formatted_query = get_limit_offset_query(
            query = query,
            limit = batch_size,
            offset = offset
        )
        frame = run_sql_query(engine = engine, query = formatted_query)
        yield frame

        if frame.shape[0] < batch_size:
            break

        offset += batch_size
def batch_in_filter_read_table(
    query:str,
    engine:Engine,
    list_elements:list,
    field:str,
    unique_max_length:int
):
    batch_start = 0
    while batch_start < len(list_elements):
        last_element = batch_start + unique_max_length
        if last_element > len(list_elements):
            last_element = len(list_elements)

        frame_dim = in_filter_read_table(
            query = query,
            engine = engine,
            list_elements = list_elements[batch_start:last_element],
            field = field
        )
        yield frame_dim
        batch_start += unique_max_length

def gather_dim_frames(
    config_queries:dict,
    extracted_frame:pd.DataFrame,
    queries_dir:str,
    engine:Engine,
    unique_max_length:int = 150000
):
    for dim in config_queries.keys():
        query_dim =  get_query_from_file(f'{queries_dir}/query_{dim}.sql')
        unique_elements = extracted_frame[config_queries[dim]['fact_field']].unique()
        frame_dim = pd.concat(
            [
                frame
                for frame in batch_in_filter_read_table(
                    query = query_dim,
                    engine = engine,
                    list_elements = unique_elements,
                    field = config_queries[dim]['field'],
                    unique_max_length = unique_max_length
                )
            ]
        )
        extracted_frame = extracted_frame.merge(
            right = frame_dim,
            on=config_queries[dim]['fact_field'],
            how='left',
        )
    return extracted_frame
