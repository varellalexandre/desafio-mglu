import pandas as pd
from general_utilities import simplify_dict_structure, clean_body, get_dimensions, formatted_element,get_query_from_file
from datetime import datetime
import awswrangler as wr
import os

PARTITION_FIELD_SIZE = os.environ.get('PARTITION_FIELD_SIZE',100000)
PATH_S3 =  os.environ.get('PATH_S3','s3://desafiomglu/dw')

def get_id_dimension(row,dimension_name):
    return row[f'id_{dimension_name}']

def treat_messages_without_dims(message):
    atributos = simplify_dict_structure(message['Sns']['MessageAttributes'])
    frame = clean_body(atributos['body'])
    frame['queue_datetime'] = message['Sns']['Timestamp']
    frame['queue_date'] = message['Sns']['Timestamp'][:10]
    dimensions = get_dimensions()
    for dimension in dimensions:
        frame[f'id_{dimension}'] = frame[dimension].apply(get_id_dimension,dimension_name=dimension)
    frame.drop(dimensions, axis=1, inplace=True)
    return frame

def clean_double_dimensions(frame, pk_field):
    if isinstance(frame, list):
        frame = pd.concat(frame)
    return frame.sort_values(
        by='queue_datetime',
        ascending=False
    ).groupby(
        by=pk_field,
        as_index=False
    ).first()

def send_to_athena(
    frame,
    database,
    table_name,
    partition_field,
    pk_field = None
):
    if database not in wr.catalog.databases().values:
        wr.catalog.create_database(database)

    if isinstance(frame, list):
        frame = pd.concat(frame)

    if wr.catalog.does_table_exist(database=database, table=table_name) and (pk_field is not None):
        partition_fields = frame[partition_field].unique()
        query = get_query_from_file(
            'query/query_base.sql'
        ).format(
            table=table_name,
            partition_field=partition_field,
            in_fields=",".join([formatted_element(element) for element in partition_fields])
        )
        on_dim_frame = wr.athena.read_sql_query(query, database=database)
        frame = frame.append(on_dim_frame)

    if pk_field is not None:
        frame = clean_double_dimensions(frame, pk_field = pk_field)
        wr.s3.to_parquet(
            df=frame,
            path=f'{PATH_S3}/{table_name}',
            mode='overwrite_partitions',
            index=None,
            dataset=True,
            partition_cols=[partition_field],
            database=database,
            table=table_name
        )
    else:
        wr.s3.to_parquet(
            df=frame,
            path=f'{PATH_S3}/{table_name}',
            mode='append',
            index=None,
            dataset=True,
            partition_cols=[partition_field],
            database=database,
            table=table_name
        )

def treat_messages_dim(message, dim_name, pk_field):
    atributos = simplify_dict_structure(message['Sns']['MessageAttributes'])
    general_frame = clean_body(atributos['body'])
    dim_frame = pd.DataFrame(general_frame[dim_name].tolist())
    dim_frame['queue_datetime'] = message['Sns']['Timestamp']
    dim_frame['__partition_field'] = (dim_frame[pk_field]/PARTITION_FIELD_SIZE).apply(int)
    return dim_frame
