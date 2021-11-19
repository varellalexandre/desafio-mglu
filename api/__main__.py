from config import app
from view import run_pedido

def run():
    from gevent.pywsgi import WSGIServer

    http_server = WSGIServer(('0.0.0.0', 3212), app)
    http_server.serve_forever()


if __name__ == '__main__':
    run()
