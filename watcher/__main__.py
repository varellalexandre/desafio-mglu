from query_utilities import (
    run_sql_query,
    connect_sqlite,
    get_query_from_file,
    batch_read_table,
    gather_dim_frames
)
from format_utilities import format_frame_to_json
from send_utilities import send_to_api
from datetime import datetime

def get_config_queries():
    return {
        "dim_produto":{
        'field':'produto.id_produto',
        'fact_field':'id_produto'
        },
        "dim_parceiro":{
            'field':'parceiro.id_parceiro',
            'fact_field':'id_parceiro'
        },
        "dim_filial":{
            'field':'filial.id_filial',
            'fact_field':'id_filial'
        },
        "dim_cliente":{
            'field':'cliente.id_cliente',
            'fact_field':'id_cliente'
        }
    }


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

def main():
    date_from = datetime.fromisoformat('1900-01-01')
    logger.info(f"Iniciando run do watcher a partir de {date_from}")
    run_watcher(
        date_from = date_from
    )

if __name__ == "__main__":
    main()
