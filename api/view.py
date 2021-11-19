from config import (
    app,
    request,
    jsonify,
    datetime,
    TOPIC_ARN
)
from queue_utilities import send_info_to_queue,format_json
import json

@app.route('/pedido', methods = ['POST'])
def run_pedido():
    request_time = datetime.now()
    ip = request.remote_addr
    if request.is_json is False:
        return jsonify({"message":"error: not json body format", "success":False}), 400

    content = request.get_json()
    if 'pedido' not in content.keys():
        return jsonify({"message":"error: missing 'pedido' in request body", "success":False}), 400

    content_str = json.dumps(content)
    formatted_json = format_json(
        body = content_str,
        request_datetime = request_time.isoformat(),
        remote_address = ip
    )
    send_info_to_queue(
        topic_arn = TOPIC_ARN,
        titulo = 'Pedido',
        attributes = formatted_json,
        message = 'request de pedido'
    )
    return jsonify({"message":"sucess", "success":True}), 200
