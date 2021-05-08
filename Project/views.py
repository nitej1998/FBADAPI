# from Project import app
from Project import app
from flask import jsonify,request,g,make_response,send_from_directory
from datetime import datetime,timedelta
from .logger import logger,get_time
from .models import DB,session_dic
from .advisment import dashboardfilter,scheduling_page_insertion

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
        logger.info('closing connection...')
        g.db.close()
        logger.info('connection closed')

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
    if responce_dic["IsCreated"] == 1:
        logger.info(f"New Advisement created by user {data['UserId']} and Aid is {responce_dic}")
        return get_advisement(dic = {"AId":responce_dic['AId']})
    else:
        logger.info(f"New Advisement Creation failed :( ")
        return jsonify(False)

@app.route("/schedule-advisement",methods=["GET", "POST"])
def schedule_advisement():
    data = request.form
    data = data.to_dict()
    data = json.loads(data['file'])
    scheduling_page_insertion(data,g.db)
    return get_advisement(dic = {"AId":data['AId']})

@app.route("/get-advisement",methods=["GET","POST"])
def get_advisement(dic = {}):
    responce_dic = {"Create Advisement":{},"Schedule Advisement":{}}
    if dic != {}:
        values = (dic["AId"],)
    else:
        data = request.form
        data = data.to_dict()
        data = json.loads(data['file'])
        values = (data["AId"],)

    query = "SELECT * from ViewAd where AId = ?"
    advisement_dic = g.db.execute(query,values,as_dic = True)
    responce_dic["AId"] = advisement_dic["AId"]  
    responce_dic["ProcessNumber"] = advisement_dic["ProcessNumber"]  

    responce_dic["Create Advisement"] = {
                                            "AdName":advisement_dic["AdName"],
                                            "AdId":advisement_dic["AdId"],
                                            "Advertiser":advisement_dic["Advertiser"],
                                            "Location":advisement_dic["Location"],
                                            "AdCategory":advisement_dic["AdCategory"],
                                            "FbKeyWord":advisement_dic["FbKeyWord"],
                                            "Status":advisement_dic["Status"],
                                            "UniqueAtCreative":advisement_dic["UniqueAtCreative"],
                                            "UniqueAtId":advisement_dic["UniqueAtId"]}
    
    if int(advisement_dic["ProcessNumber"] > 1):
        responce_dic["Schedule Advisement"]["Data"] = {
                                        "ScheduleMethod":advisement_dic["ScheduleMethod"],
                                        "RecurringMethod":advisement_dic["RecurringMethod"],
                                        "SD":advisement_dic["SD"],
                                        "ED":advisement_dic["ED"],
                                        "SM":advisement_dic["SM"],
                                        "EM":advisement_dic["EM"],
                                        "SY":advisement_dic["SY"],
                                        "EY":advisement_dic["EY"],
                                        "YearType":advisement_dic["YearType"]}
        responce_dic["Schedule Advisement"]["DropDown"] = {"Years":session_dic["Years"],"Months":session_dic["Months"]}
    else:
        responce_dic["Schedule Advisement"]["Data"] = {}
        responce_dic["Schedule Advisement"]["DropDown"] = {"Years":session_dic["Years"],"Months":session_dic["Months"]}
    
    return jsonify(responce_dic)

@app.route("/ongoing-team-advisement",methods=["GET","POST"])
def ongoing_team_advisement():
    # data = request.form
    # data = data.to_dict()
    # data = json.loads(data['file'])
    responce_dic = dashboardfilter(g.db,{},1,0,False,True,True)
    return jsonify(responce_dic)
    
@app.route("/completed-team-advisement",methods=["GET","POST"])
def completed_team_advisement():
    # data = request.form
    # data = data.to_dict()
    # data = json.loads(data['file'])
    responce_dic = dashboardfilter(g.db,{},1,1,False,True,True)
    return jsonify(responce_dic)

@app.route("/mark-as-complete-advisement",methods=["GET", "POST"])
def mark_as_complete_advisement():
    data = request.form
    data = data.to_dict()
    data = json.loads(data['file'])
    query = "EXEC UpdateMarkAsCompleteAdvisement @UserId = ?,@AId = ?"
    values = (data["UserId"],data["AId"])
    g.db.update(query,values,as_dic = True)
    logger.info("")
    return dashboardfilter(g.db,data,1,0,False,True,True) # need to add data

@app.route("/prioritize-advisement",methods=["GET", "POST"])
def prioritize_advisement():
    data = request.form
    data = data.to_dict()
    data = json.loads(data['file'])
    query = "EXEC UpdatePrioritizeAdvisement @UserId = ?,@AId = ?"
    values = (data["UserId"],data["AId"])
    g.db.update(query,values,as_dic = True)
    logger.info("")
    return dashboardfilter(g.db,data,1,0,False,True,True) # need to add data

