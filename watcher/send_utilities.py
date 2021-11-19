import requests
import json

def send_to_api(
    formatted_json,
    url:str = 'http://127.0.0.1:3212/pedido'
):
    payload = {
        "pedido":formatted_json
    }
    headers = {
      "Content-Type": "application/json"
    }

    response = requests.post(url, headers=headers, json=payload)
    print('pedido enviado')
