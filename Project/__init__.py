import socket

from flask import Flask
from flask_cors import CORS
from flask_mail import Mail, Message

ip = socket.gethostbyname(socket.gethostname())
app = Flask(__name__)
mail= Mail(app)

app.config['SECRET_KEY'] = "FB AD POC"
app.config['JSON_SORT_KEYS'] = False
app.config['DEBUG'] = True
app.config['THREADED'] = True
app.config['MAIL_SERVER']='smtp.gmail.com'
app.config['MAIL_PORT'] = 465
app.config['MAIL_USERNAME'] = 'yourId@gmail.com'
app.config['MAIL_PASSWORD'] = '*****'
app.config['MAIL_USE_TLS'] = False
app.config['MAIL_USE_SSL'] = True
mail = Mail(app)
# app.config['SERVER_NAME'] = str(ip) + ":5000"
cors = CORS(app)
from Project import views
