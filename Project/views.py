# from Project import app
from Project import app
from flask import jsonify,request,g,make_response,send_from_directory
from datetime import datetime,timedelta
from .logger import logger,get_time
from .models import DB,session_dic
from .advisment import dashboardfilter

import traceback
import os
import json

@app.before_request
def before_request():
    """ logs Request and establish a connection with database configured"""
    
    logger.info('Request Method: %s', request.method)
    logger.info('Request Route: %s', request.path)
    logger.info('Request Inputs: %s', request.args)
    logger.info('connecting to database...')
    g.db = DB()
        
@app.after_request
def after_request(response):
    """ Logging after every request. and closing database connection """
    # This avoids the duplication of registry in the log,
    # since that 500 is already logged via @app.errorhandler.
    # logger.info('Response : %s', response)   
    
    response.headers['Content-Length'] = None
    response.headers['Content-Type'] = None
    response.headers['Vary'] = None
    
    logger.info('Responce Status: %s',response.status)
    logger.info('Responce Headers: %s',response.headers)
    logger.info('Responce Data: %s',response.get_data())
    
    if response.status_code != 500:
        ts = datetime.now().strftime('[%Y-%b-%d %H:%M]')
        logger.error('%s %s %s %s %s %s',
                      ts,
                      request.remote_addr,
                      request.method,
                      request.scheme,
                      request.full_path,
                      response.status)
    
    if g.db is not None:
        print ('closing connection...')
        # g.db.close()
    
    return response

@app.errorhandler(Exception)
def exceptions(e):
    """ Logging after every Exception. """
    logger.error("Type of expection:")
    template = "An exception of type {0} occurred. Arguments:\n{1!r}"
    message = template.format(type(e).__name__, e.args)
    logger.error(message)
    ts = datetime.now().strftime('[%Y-%b-%d %H:%M]')
    tb = traceback.format_exc()
    logger.error('%s %s %s %s %s 5xx INTERNAL SERVER ERROR\n%s',
                  ts,
                  request.remote_addr,
                  request.method,
                  request.scheme,
                  request.full_path,
                  tb)
    return ("Internal Server Error", 500)

@app.route('/favicon.ico') 
def favicon(): 
    # return send_from_directory(os.path.join(app.root_path, 'static'), 'favicon.ico', mimetype='image/vnd.microsoft.icon')
    return jsonify("dont use")

@app.route("/")
def default():
    return jsonify("Every thing is working fine")

@app.route("/login",methods=["GET", "POST"])
def login():
    data = request.form
    data = data.to_dict()
    data = json.loads(data['file'])
    query = "EXEC VerifyLogin @UserEmail = ?,@Password = ?"
    values = (data["UserEmail"],data["Password"])
    responce_dic = g.db.execute(query,values,as_dic = True)
    return jsonify(responce_dic)

@app.route("/create-advisement-inputs",methods=["GET", "POST"])
def get_create_ad_inputs():
    responce_dic = {}
    responce_dic["Location"] = session_dic["Location"]
    responce_dic["Advertiser"] = session_dic["Advertiser"]
    responce_dic["AdCategory"] = session_dic["AdCategory"]
    responce_dic["FbKeyWord"] = session_dic["FbKeyWord"]
    responce_dic["FbStatus"] = session_dic["FbStatus"]
    return jsonify(responce_dic)

@app.route("/create-advisement",methods=["GET", "POST"])
def create_advisement():
    data = request.form
    data = data.to_dict()
    data = json.loads(data['file'])
    query = "EXEC InsertAdvisement @UserId = ?,@AdName = ?,@AdId = ?,@AdvertiserId = ?,@LocationId = ?,@AdCategoryId = ?,@FbKeyWordId = ?,@StatusId = ?,@UniqueAtId = ?,@UniqueAtCreative = ?"
    values = (data["UserId"],data["AdName"],data["AdId"],data["AdvertiserId"],data["LocationId"],data["AdCategoryId"],data["FbKeyWordId"],data["StatusId"],data["UniqueAtId"],data["UniqueAtCreative"])
    responce_dic = g.db.execute(query,values,as_dic = True)
    return jsonify(responce_dic)

@app.route("/ongoing-team-advisement",methods=["GET","POST"])
def ongoing_team_advisement():
    data = request.form
    data = data.to_dict()
    data = json.loads(data['file'])
    responce_dic = dashboardfilter(g.db,data,1,0,False,True,True)
    return jsonify(responce_dic)
    
@app.route("/completed-team-advisement",methods=["GET","POST"])
def completed_team_advisement():
    data = request.form
    data = data.to_dict()
    data = json.loads(data['file'])
    responce_dic = dashboardfilter(g.db,data,1,1,False,True,True)
    return jsonify(responce_dic)

		
