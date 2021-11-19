import os, sys
pipe_dir = os.path.join(os.path.dirname(__file__),"pipe_functions/dw")
watcher_dir = os.path.join(os.path.dirname(__file__),"watcher")
sys.path.append(pipe_dir)
sys.path.append(watcher_dir)

from pipe_functions import main
from watcher import (
    run_sql_query,
    connect_sqlite,
    get_query_from_file,
    batch_read_table,
    gather_dim_frames,
    format_frame_to_json,
    get_config_queries
)
from datetime import datetime
import json
import logging

def get_records(json_formatted):
    return {
          "Records": [
            {
              "EventSource": "aws:sns",
              "EventVersion": "1.0",
              "EventSubscriptionArn": "arn:aws:sns:sa-east-1:{{{accountId}}}:ExampleTopic",
              "Sns": {
                "Type": "Notification",
                "MessageId": "95df01b4-ee98-5cb9-9903-4c221d41eb5e",
                "TopicArn": "arn:aws:sns:sa-east-1:123456789012:ExampleTopic",
                "Subject": "example subject",
                "Message": "example message",
                "Timestamp": "1970-01-01T00:00:00.000Z",
                "SignatureVersion": "1",
                "Signature": "EXAMPLE",
                "SigningCertUrl": "EXAMPLE",
                "UnsubscribeUrl": "EXAMPLE",
                "MessageAttributes": {
                    "body":{
                        "Type":"String",
                        "Value":json.dumps({'pedido':json_formatted})
                    },
                    "request_datetime":{
                        "Type":"String",
                        "Value":"2021-11-18 08:00:00"
                    },
                    "remote_address":{
                        "Type":"String",
                        "Value":"127.0.0.1"
                    },
                }
              }
            }
          ]
        }

def send_to_dw(
    json_formatted
):
    formatted_event = get_records(json_formatted)
    main(event = formatted_event, context = None)
    print('enviando lote')

def send_to_api(
    formatted_json
):
    send_to_dw(formatted_json)

def run_watcher(
    watcher_dir:str = 'watcher',
    date_from:datetime = datetime.fromisoformat('1900-01-01')
):
    queries_dir = f'{watcher_dir}/queries'
    engine = connect_sqlite('marketplace.sqlite')
    query_fact = get_query_from_file(f'{queries_dir}/query_fact_item_pedido.sql')
    config_queries = get_config_queries()
    for frame in batch_read_table(
        query = query_fact,
        engine = engine,
        date_from = date_from.isoformat()
    ):
        gathered_table = gather_dim_frames(
            config_queries = config_queries,
            extracted_frame = frame,
            queries_dir = queries_dir,
            engine = engine
        )
        formatted_list = format_frame_to_json(gathered_table)
        send_to_api(formatted_list)

def run():
    date_from = datetime.fromisoformat('1900-01-01')
    logging.info(f"Iniciando run do watcher a partir de {date_from}")
    run_watcher(
        date_from = date_from
    )
if __name__ == "__main__":
    run()
