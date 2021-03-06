# from Project import app
from logging import exception
from Project import app
from flask import jsonify, request, g, make_response, send_from_directory
from datetime import datetime, timedelta,date
from .logger import logger, get_time
from .models import DB, session_dic
from .advisment import dashboardfilter, scheduling_page_insertion

import traceback
import os
import json
import schedule
import time


@app.before_request
def before_request():
    """ logs Request and establish a connection with database configured"""

    logger.info('Request Method:  %s', request.method)
    logger.info('Request Route:  %s', request.path)
    logger.info('Request Inputs:  %s', request.args)
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

    logger.info('Responce Status:  %s', response.status)
    logger.info('Responce Headers:  %s', response.headers)
    # logger.info('Responce Data:  %s', response.get_data())

    if response.status_code != 500:
        ts = datetime.now().strftime('[%Y-%b-%d %H: %M]')
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
    logger.error("Type of expection: ")
    template = "An exception of type {0} occurred. Arguments: \n{1!r}"
    message = template.format(type(e).__name__, e.args)
    logger.error(message)
    ts = datetime.now().strftime('[%Y-%b-%d %H: %M]')
    tb = traceback.format_exc()
    logger.error('%s %s %s %s %s 5xx INTERNAL SERVER ERROR\n%s',
                 ts,
                 request.remote_addr,
                 request.method,
                 request.scheme,
                 request.full_path,
                 tb)
    return ("Internal Server Error", 500)


# scheduling Database SP to invoke every day at a some specified time 
def auto_schedule():
    try:
        db = DB()
        logger.info('auto scheduler activated')
        query = "select LiveDate from Configuration"
        old_date = db.execute(query,as_dic=True)
        old_date = old_date['LiveDate']
        old_date = 	datetime.strftime(old_date, '%Y-%m-%d')

        od = old_date.split('-')
        nd = str(date.today()).split('-')
        if od[0] == nd[0]:
            year = False
        else:
            year = True
        if od[1] == nd[1]:
            month = False
            quater = False
        else:
            month = True
            if month in ['1','5','9']:
                quater = True
            else:
                quater = False
        if od[2] == od[2]:
            day = False
        else:
            day = True

        query = "UPDATE [Configuration] set [LiveDate] = ?,TriggerWeekly = ?,TriggerMonthly = ?"
        values = (date,month,year,quater)
        db.update(query,values)


        query = "EXEC [TriggerAutoSchedule]"
        db.update(query)
        logger.info('auto scheduler completed')   
    except Exception as e:
        logger.info('auto scheduler failed')
        logger.error(str(e))
        # msg = Message('Hello', sender = 'yourId@gmail.com', recipients = ['someone1@gmail.com'])
        # msg.body = "Hello Flask message sent from Flask-Mail"
        # mail.send(msg)


schedule.every().day.at("01:00").do(auto_schedule,'It is 01:00')

# while True:
#     schedule.run_pending()
#     time.sleep(60) # wait one minute


@app.route('/favicon.ico')
def favicon():
    return jsonify("dont use")


@app.route("/")
def default():
    return jsonify("Every thing is working fine")


@app.route("/login", methods=["GET", "POST"])
def login():
    data = request.form
    data = data.to_dict()
    data = json.loads(data['file'])
    query = "EXEC VerifyLogin @UserEmail = ?, @Password = ?"
    values = (data["UserEmail"], data["Password"])
    responce_dic = g.db.execute(query, values, as_dic=True)
    logger.info('Responce Data:  %s', responce_dic)
    return jsonify(responce_dic)


@app.route("/create-advisement-inputs", methods=["GET", "POST"])
def get_create_ad_inputs():
    responce_dic = {}
    query = "EXEC GetAdvertiser"
    responce_dic["Advertiser"] = g.db.execute(query)
    responce_dic["Location"] = session_dic["Location"]
    responce_dic["AdCategory"] = session_dic["AdCategory"]
    responce_dic["FbStatus"] = session_dic["FbStatus"]
    logger.info('Responce Data:  %s', responce_dic)
    return jsonify(responce_dic)


@app.route("/create-advisement", methods=["GET", "POST"])
def create_advisement():
    data = request.form
    data = data.to_dict()
    data = json.loads(data['file'])

    query = "EXEC InsertAdvisement @UserId = ?, @AdName = ?, @AdId = ?, @Advertiser = ?, @LocationId = ?, @AdCategoryId = ?, @FbKeyWordId = ?, @StatusId = ?, @UniqueAtId = ?, @UniqueAtCreative = ?, @FbKeyWord = ?"
    values = (data["UserId"], data["AdName"], data["AdId"], data["AdvertiserId"], data["LocationId"], data["AdCategoryId"], 1, data["StatusId"], data["UniqueAtId"], data["UniqueAtCreative"], data["FbKeyWordId"])
    responce_dic = g.db.execute(query, values, as_dic=True)
    if responce_dic["IsCreated"] == 1:
        logger.info(f"New Advisement created by user {data['UserId']} and Aid is {responce_dic}")
        responce_dic = get_advisement(dic={"AId": responce_dic['AId']})
        logger.info('Responce Data:  %s', responce_dic)
        return responce_dic
    else:
        logger.info(f"New Advisement Creation failed : ( ")
        return jsonify(False)


@app.route("/schedule-advisement", methods=["GET", "POST"])
def schedule_advisement():
    data = request.form
    data = data.to_dict()
    data = json.loads(data['file'])
    scheduling_page_insertion(data, g.db)
    responce_dic = get_advisement(dic={"AId": data['AId']})
    logger.info('Responce Data:  %s', responce_dic)
    return responce_dic


@app.route("/get-advisement", methods=["GET", "POST"])
def get_advisement(dic={}):
    responce_dic = {"Create Advisement": {}, "Schedule Advisement": {}}
    if dic != {}:
        values = (dic["AId"], )
    else:
        data = request.form
        data = data.to_dict()
        data = json.loads(data['file'])
        values = (data["AId"], )

    query = "SELECT * from ViewAd where AId = ?"
    advisement_dic = g.db.execute(query, values, as_dic=True)
    responce_dic["AId"] = advisement_dic["AId"]
    responce_dic["ProcessNumber"] = advisement_dic["ProcessNumber"]
    if responce_dic["ProcessNumber"] == 1:
        responce_dic["UiProcess"] = 2
    elif responce_dic["ProcessNumber"] > 1:
        responce_dic["UiProcess"] = 2
    else:
        responce_dic["UiProcess"] = 1

    responce_dic["Create Advisement"] = {
                                            "AdName": advisement_dic["AdName"],
                                            "AdId": advisement_dic["AdId"],
                                            "Advertiser": advisement_dic["Advertiser"],
                                            "Location": advisement_dic["Location"],
                                            "AdCategory": advisement_dic["AdCategory"],
                                            "FbKeyWord": advisement_dic["FbKeyWord"],
                                            "Status": advisement_dic["Status"],
                                            "UniqueAtCreative": advisement_dic["UniqueAtCreative"],
                                            "UniqueAtId": advisement_dic["UniqueAtId"],
                                            "LocationId": advisement_dic["LocationId"],
                                            "StatusId": advisement_dic["StatusId"],
                                            "AdCategoryId": advisement_dic["AdCategoryId"],
                                        }

    if int(advisement_dic["ProcessNumber"] > 1):
        responce_dic["Schedule Advisement"]["Data"] = {
                                        "ScheduleMethod": advisement_dic["ScheduleMethod"],
                                        "RecurringMethod": advisement_dic["RecurringMethod"],
                                        "SD": str(advisement_dic["SD"].strftime("%Y-%m-%d")),
                                        "ED": str(advisement_dic["ED"].strftime("%Y-%m-%d")),
                                        "SM": advisement_dic["SM"],
                                        "EM": advisement_dic["EM"],
                                        "SY": advisement_dic["SY"],
                                        "EY": advisement_dic["EY"],
                                        "YearType": advisement_dic["YearType"]}
        responce_dic["Schedule Advisement"]["DropDown"] = {"Years": session_dic["Years"], "Months": session_dic["Months"]}
    else:
        responce_dic["Schedule Advisement"]["Data"] = {}
        responce_dic["Schedule Advisement"]["DropDown"] = {"Years": session_dic["Years"], "Months": session_dic["Months"]}
    logger.info('Responce Data:  %s', responce_dic)
    return jsonify(responce_dic)


@app.route("/ongoing-team-advisement", methods=["GET", "POST"])
def ongoing_team_advisement():
    """ returns list on ongoing advisements """
    data = request.form
    data = data.to_dict()
    data = json.loads(data['file'])
    responce_dic = dashboardfilter(g.db, data, 1, 0, False, True, True)
    logger.info('Responce Data:  %s', responce_dic)
    return jsonify(responce_dic)


@app.route("/completed-team-advisement", methods=["GET", "POST"])
def completed_team_advisement():
    """ returns list on completed advisements """
    data = request.form
    data = data.to_dict()
    data = json.loads(data['file'])
    responce_dic = dashboardfilter(g.db, data, 1, 1, False, True, True)
    logger.info('Responce Data:  %s', responce_dic)
    return jsonify(responce_dic)


@app.route("/mark-as-complete-advisement", methods=["GET", "POST"])
def mark_as_complete_advisement():
    """ marks provided advisement as completed in application Q and returns list on ongoing advisements """
    data = request.form
    data = data.to_dict()
    data = json.loads(data['file'])
    query = "EXEC UpdateMarkAsCompleteAdvisement @UserId = ?, @AId = ?"
    values = (data["userid"], data["AId"])
    g.db.update(query, values)
    logger.info("")
    responce_dic = dashboardfilter(g.db, {}, 1, 0, False, True, True)
    logger.info('Responce Data:  %s', responce_dic)
    return jsonify(responce_dic)


@app.route("/prioritize-advisement", methods=["GET", "POST"])
def prioritize_advisement():
    """ prioritize provided advisement in RPA Q and returns list on ongoing advisements """
    data = request.form
    data = data.to_dict()
    data = json.loads(data['file'])
    query = "EXEC UpdatePrioritizeAdvisement @UserId = ?, @AId = ?"
    values = (data["UserId"], data["AId"])
    g.db.update(query, values)
    logger.info("")
    responce_dic = dashboardfilter(g.db, {}, 1, 0, False, True, True)
    logger.info('Responce Data:  %s', responce_dic)
    return jsonify(responce_dic)


@app.route('/get-file-names', methods=["GET", "POST"])
def file_names():
    """
    Input:  AId

    Description:  will provided all files attached for that plan

    Output:  object consist of file name and file path
    """
    data = request.form
    data = data.to_dict()
    data = json.loads(data['file'])
    query = "Exec [GetFilePathsForAdvisement] @AId=?"
    values = (data["AId"], )

    myresult = g.db.execute(query, params=values)
    file_dic = {}
    """ converting all slashs in file path """
    for file_path in myresult:
        for i in file_path:
            if file_path[i] is not ' ' and file_path[i] is not None:
                paths = file_path[i].split(', ')
                for j in paths:
                    path = j.replace('/', '\\')
                    file_name = j.replace('/', '\\').split("\\")[-1]
                    file_dic[file_name] = path
            else:
                pass
    logger.info('Responce Data:  %s', file_dic)
    return jsonify(file_dic)


@app.route('/download-file', methods=["GET", "POST"])
def download_file():
    """ used to download files from FBAD application """
    data = request.form
    data = data.to_dict()
    data = json.loads(data['file'])
    return jsonify("will add")
