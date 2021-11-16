import os
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
import pandas as pd

def file_is_csv(filename:str):
    if not isinstance(filename, str):
        return False
    elif filename[-4:] == '.csv':
        return True
    return False

def list_csvs_tables(path_to_csvs:str):
    files = os.listdir(path_to_csvs)
    for file in files:
        if file_is_csv(file):
            yield file

def connect_sqlite(path_sqlite:str):
    filename_comp = f'sqlite:///{path_sqlite}'
    engine = create_engine(filename_comp)
    return engine

def frame_dump_to_sqlite(
    frame:pd.DataFrame,
    engine:Engine,
    tablename:str
):
    frame.to_sql(
        name = tablename,
        con = engine,
        if_exists = 'replace',
        index = False
    )
