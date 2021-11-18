import pandas as pd
def row_to_json(row):
    return {
      "id_pedido":row['id_pedido'],
      "dt_pedido":row['dt_pedido'],
      "quantidade":row['quantidade'],
      "vr_unitario":row['vr_unitario'],
      "vr_total_pago":row['vr_total_pago'],
      "produto":{
        "id_produto":row['id_produto'],
        "ds_produto":row['ds_produto'],
        "ds_subcategoria":row['ds_subcategoria'],
        "ds_categoria":row['ds_categoria'],
        "perc_parceiro":row['perc_parceiro']
      },
      "parceiro":{
        "id_parceiro":row['id_parceiro'],
        "nm_parceiro":row['nm_parceiro']
      },
      "cliente":{
        "id_cliente":row['id_cliente'],
        "nm_cliente":row['nm_cliente'],
        "flag_ouro":row['flag_ouro'],
      },
      "filial":{
        "id_filial":row['id_filial'],
        "ds_filial":row['ds_filial'],
        "ds_cidade":row['ds_cidade'],
        "ds_estado":row['ds_estado']
        }
      }

def format_frame_to_json(
    frame:pd.DataFrame
):
    formatted_list = list()
    for index, row in frame.iterrows():
        formatted_list.append(row_to_json(row))
    return formatted_list
