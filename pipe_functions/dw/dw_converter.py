import logging
from dw_utilities import (
    treat_messages_without_dims,
    treat_messages_dim,
    send_to_athena
)
import os
DATABASE = os.environ.get('DATABASE','dw_marketplace')

def main(
    event,
    context
):
    frames_pedido = list()
    frames_filial = list()
    frames_cliente = list()
    frames_parceiro = list()
    frames_produto = list()
    for message in event['Records']:
        frames_pedido.append(treat_messages_without_dims(message))
        frames_filial.append(
            treat_messages_dim(
                message,
                dim_name = 'filial',
                pk_field = 'id_filial'
            )
        )
        frames_cliente.append(
            treat_messages_dim(
                message,
                dim_name = 'cliente',
                pk_field = 'id_cliente'
            )
        )
        frames_parceiro.append(
            treat_messages_dim(
                message,
                dim_name = 'parceiro',
                pk_field = 'id_parceiro'
            )
        )
        frames_produto.append(
            treat_messages_dim(
                message,
                dim_name = 'produto',
                pk_field = 'id_produto'
            )
        )
    else:
        gathered_pedido = send_to_athena(
            frames_pedido,
            database = DATABASE,
            table_name = 'fact_item_pedido',
            partition_field = 'date_pedido'
        )
        gathered_filial = send_to_athena(
            frames_filial,
            database = DATABASE,
            table_name = 'dim_filial',
            partition_field = '__partition_field',
            pk_field = 'id_filial'
        )
        gathered_cliente = send_to_athena(
            frames_cliente,
            database = DATABASE,
            table_name = 'dim_cliente',
            partition_field = '__partition_field',
            pk_field = 'id_cliente'
        )
        gathered_parceiro = send_to_athena(
            frames_parceiro,
            database = DATABASE,
            table_name = 'dim_parceiro',
            partition_field = '__partition_field',
            pk_field = 'id_parceiro'
        )
        gathered_produto = send_to_athena(
            frames_produto,
            database = DATABASE,
            table_name = 'dim_produto',
            partition_field = '__partition_field',
            pk_field = 'id_produto'
        )
