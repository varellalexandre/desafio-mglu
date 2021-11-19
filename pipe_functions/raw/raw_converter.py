import logging
import pandas as pd
import awswrangler as wr
import os

PATH_S3 =  os.environ.get('PATH_S3','s3://desafiomglu/raw')
def simplify_dict_structure(elements_dict:dict):
    new_dict = dict()
    for element in elements_dict.keys():
        new_dict[element] = elements_dict[element]['Value']

    return new_dict

def upload_raw_parquet_to_athena(
    frame,
    database = 'raw_marketplace',
    table_name = 'raw_messages',
    partition_field = 'queue_date'
):
    if database not in wr.catalog.databases().values:
        wr.catalog.create_database(database)
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

def main(
    event,
    context
):
    list_messages = list()
    for message in event['Records']:
        atributos = simplify_dict_structure(message['Sns']['MessageAttributes'])
        list_messages.append(atributos)
    else:
        frame = pd.DataFrame(list_messages)
        frame['queue_datetime'] = message['Sns']['Timestamp']
        frame['queue_date'] = message['Sns']['Timestamp'][:10]
        upload_raw_parquet_to_athena(frame)
