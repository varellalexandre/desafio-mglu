from .pipeline_base_de_dados import (
    list_csvs_tables,
    connect_sqlite,
    frame_dump_to_sqlite,
    file_is_csv
)
import os
import pandas as pd

def main_pipeline_base_de_dados(
    path_to_csvs:str = '/raw'
):
    sqlite_filename = os.environ.get('SQL_FILENAME')
    engine = connect_sqlite(sqlite_filename)
    for file in list_csvs_tables(path_to_csvs):
        file_frame = pd.read_csv(f'{path_to_csvs}/{file}', delimiter ='|')
        tablename = file[:-4]
        frame_dump_to_sqlite(
            frame = file_frame,
            engine = engine,
            tablename = tablename
        )

if __name__ == "__main__":
    main()
