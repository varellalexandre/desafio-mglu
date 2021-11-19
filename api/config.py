from flask import (
	Flask,
	render_template,
	redirect,
	url_for,
	request,
	session,
	send_from_directory,
	send_file,
	jsonify,
	make_response
)
import os
from flask_cors import CORS
from datetime import datetime,timedelta

app = Flask(__name__)
app.config['SEND_FILE_MAX_AGE_DEFAULT'] = timedelta(
	days=int(os.environ.get('CACHE',360))
)
CORS(app,resources={r"/*": {"origins": "*"}})

TOPIC_ARN = os.environ.get('TOPIC_ARN','arn:aws:sns:sa-east-1:469457704300:webhook_pedidos')
app.config['SECRET_KEY'] = os.environ.get('SECRET',None)
