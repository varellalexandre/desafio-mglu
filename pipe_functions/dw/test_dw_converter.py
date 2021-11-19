from dw_converter import main
def get_records():
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
                        "Value":'{"pedido": [{"id_pedido": 46651165713, "dt_pedido": "2021-06-02T00:00:00.000Z", "quantidade": 1, "vr_unitario": 155.99, "vr_total_pago": 155.99, "produto": {"id_produto": 2306710, "ds_produto": "Produto - 0002306710", "ds_subcategoria": "Sub-categoria - 00685", "ds_categoria": "Categoria - 027", "perc_parceiro": 2.0}, "parceiro": {"id_parceiro": 13, "nm_parceiro": "Parceiro Magalu- 13"}, "cliente": {"id_cliente": 130244027, "nm_cliente": "Cliente Magalu - 0130244027", "flag_ouro": 0}, "filial": {"id_filial": 1704, "ds_filial": "Filial - 001704", "ds_cidade": "ACAILANDIA", "ds_estado": "MA"}}, {"id_pedido": 46651165713, "dt_pedido": "2021-06-02T00:00:00.000Z", "quantidade": 1, "vr_unitario": 155.99, "vr_total_pago": 155.99, "produto": {"id_produto": 2306710, "ds_produto": "Produto - 0002306710", "ds_subcategoria": "Sub-categoria - 00685", "ds_categoria": "Categoria - 027", "perc_parceiro": 2.0}, "parceiro": {"id_parceiro": 13, "nm_parceiro": "Parceiro Magalu - 13"}, "cliente": {"id_cliente": 130244027, "nm_cliente": "Cliente Magalu - 0130244027", "flag_ouro": 0}, "filial": {"id_filial": 1704, "ds_filial": "Filial - 001704", "ds_cidade": "ACAILANDIA", "ds_estado": "MA"}}, {"id_pedido": 46651165713, "dt_pedido": "2021-06-02T00:00:00.000Z", "quantidade": 1, "vr_unitario": 155.99, "vr_total_pago": 155.99, "produto": {"id_produto": 2306710, "ds_produto": "Produto - 0002306710", "ds_subcategoria": "Sub-categoria - 00685", "ds_categoria": "Categoria - 027", "perc_parceiro": 2.0}, "parceiro": {"id_parceiro": 13, "nm_parceiro": "Parceiro Magalu - 13"}, "cliente": {"id_cliente": 130244027, "nm_cliente": "Cliente Magalu - 0130244027", "flag_ouro": 0}, "filial": {"id_filial": 1704, "ds_filial": "Filial - 001704", "ds_cidade": "ACAILANDIA", "ds_estado": "MA"}}, {"id_pedido": 46651165713, "dt_pedido": "2021-06-02T00:00:00.000Z", "quantidade": 1, "vr_unitario": 155.99, "vr_total_pago": 155.99, "produto": {"id_produto": 2306710, "ds_produto": "Produto - 0002306710", "ds_subcategoria": "Sub-categoria - 00685", "ds_categoria": "Categoria - 027", "perc_parceiro": 2.0}, "parceiro": {"id_parceiro": 13, "nm_parceiro": "Parceiro Magalu - 13"}, "cliente": {"id_cliente": 130244027, "nm_cliente": "Cliente Magalu - 0130244027", "flag_ouro": 0}, "filial": {"id_filial": 1704, "ds_filial": "Filial - 001704", "ds_cidade": "ACAILANDIA", "ds_estado": "MA"}}]}'
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


def test_main():
    records = get_records()
    main(event = records, context = None)


if __name__ == '__main__':
    test_main()
