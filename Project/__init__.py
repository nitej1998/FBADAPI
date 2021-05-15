import socket

from flask import Flask
from flask_cors import CORS

ip = socket.gethostbyname(socket.gethostname())
app = Flask(__name__)

app.config['SECRET_KEY'] = "FB AD POC"
app.config['JSON_SORT_KEYS'] = False
app.config['DEBUG'] = True
app.config['THREADED'] = True

app.config['SERVER_NAME'] = str(ip) + ":5000"
cors = CORS(app)
from Project import views
