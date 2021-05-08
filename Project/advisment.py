import pandas as pd
from datetime import datetime,timedelta

from .views import session_dic

def dashboardfilter(db,dic,module,record_status,for_user = False,advertiser_needed = False,location_needed = False):
    """ will extract data from DB as per the filter applied

    Args:
        dic ([dic]): object consist of applied filters 
        module ([int]): 1:planning,2:scheduling,3:tracking,4:reporting
        record_status ([int]): 0:Ongoing,1:Completed
        for_user (bool, optional): if for a specific user userid.Defaults to False.
        client_needed(bool, optional): if True will return client dropdown also 
        brand_needed(bool, optional): if True will return brand dropdown also 

    Returns:
        [type]: [description]
    """

    filter_adid = False
    filter_adname = False

    dic = {k.lower(): v for k, v in dic.items()}
    # userid = dic["userid"]
    # dic.pop('isdefault')
    # if for_user == False:
    #     dic.pop('userid')
    result_dic = {}
    if 'startdate'  in dic:
        datefilter = True
        start_date =datetime.strptime(dic['startdate'],'%Y-%m-%d')
        end_date = datetime.strptime(dic['enddate'],'%Y-%m-%d')
        dic.pop('startdate')
        dic.pop('enddate')
    else:
        datefilter = False
    if 'sessionid' in dic:
        dic.pop('sessionid')

    
    if "advertiserid" in dic:			
        advertiser_name = session_dic["AdvertiserId"][dic['advertiserid']]
        dic['advertisername'] =   f'"{advertiser_name}"'  
        dic.pop('advertiserid')

    if "locationid" in dic:			
        location_name = session_dic["LocationId"][dic['locationid']]
        dic['locationname'] = f'"{location_name}"'
        dic.pop('locationid')

    if "adid" in dic:
        filter_adid = True
        adid = dic['adid']
        dic.pop("adid")

    if "adname" in dic:
        filter_adname = True
        adname = dic['adname']
        dic.pop("adname")

    if module == 1:
        if record_status == 0:
            query = 'EXEC GetOngoingadvisement'	
        else:
            query = 'EXEC GetCompletedadvisement'

    df = db.execute(query,as_dataframe=True)
    original_columns = list(df.columns)
    if df.empty == False:
        df["StartDate"] = pd.to_datetime(df["StartDate"],format = '%d/%m/%Y %H:%M')
        df["StartDate"] = df["StartDate"].astype(str)
        new = df["StartDate"].str.split(":", n = 2, expand = True)
        df["StartDate"] = new[0].str.cat(new[1], sep =":")
        
        if record_status != 0:
            df["EndDate"] = pd.to_datetime(df["EndDate"],format = '%d/%m/%Y %H:%M')
            df["EndDate"] = df["EndDate"].astype(str)
            new = df["EndDate"].str.split(":", n = 2, expand = True)
            df["EndDate"] = new[0].str.cat(new[1], sep =":")

        df.columns= df.columns.str.lower()
        filter_query = ' & '.join(['{}=={}'.format(k, v) for k, v in dic.items()])
        
        if filter_query != '':
            df = df.query(filter_query)
        
        if filter_adid == True:
            df = df[df['campaignid'].str.contains(adid)]

        if filter_adname == True:
            df = df[df['campaignname'].str.contains(adname)]
                
        if datefilter == True:
            df = df[df["startdate"].isin(pd.date_range(start_date, end_date))]		
                            
    df.columns = original_columns
    data = df.to_dict(orient='records') 
    result_dic["Records"] = data

    if location_needed == True:
        result_dic["Location"] = session_dic["Location"]
    if advertiser_needed == True:
        result_dic["Advertiser"] = session_dic["Advertiser"]

    return result_dic

def scheduling_page_insertion(dic,db):
    """update a report as per the inputs provided in Scheduling tab at UI level
        schedulemethod --> 1 then it is one time schedule
        schedulemethod --> 2 then it is recurring schdule

        yt --> 1 calender year
        yt --> 2 financial year
    Args:
        dic ([dic]): consist of all input peramaters required to create a schedule for report
    """
    # rpa_process = 0
    aid = dic['AId']
    onetimevalue = dic["OneTimeValue"]
    counter = 0
    uniqueid = str(aid) + '_0'
    
    if onetimevalue == True:
        query = "INSERT INTO SchedulingAd([AId],[ScheduleMethod],[SD],[ED],[IsRPAProcessed],[Counter],[UniqueId],[CreatedBy],[ModifiedBy]) VALUES(?,?,?,?,?,?,?,?,?)"
        values = (dic['AId'],"OneTime",dic['SD'],dic['ED'],0,counter,uniqueid,dic['UserId'],dic['UserId'])
    else:
        if dic["RecurringMethod"] in ("daily","week"):
            query = "INSERT INTO SchedulingAd([AId],[ScheduleMethod],[RecurringMethod],[SD],[ED],[IsRPAProcessed],[Counter],[UniqueId],[CreatedBy],[ModifiedBy]) VALUES(?,?,?,?,?,?,?,?,?,?)"
            values = (dic['AId'],"Recurring",dic["RecurringMethod"],dic['SD'],dic['ED'],0,counter,uniqueid,dic['UserId'],dic['UserId'])

        elif dic["RecurringMethod"] == "month":
            sd = dic["SY"] + '-' + str(session_dic["Months"].index(dic["SM"]) +1).zfill(2) + '-' + '01'
            em = str(session_dic["Months"].index(dic["EM"]) +1).zfill(2)
            year = int(dic["EY"])
            if int(em) in (1,3,5,7,8,10,12):
                ed = '31'
            elif int(em) in (4,6,9,11):
                ed = '30'
            elif (year % 4) == 0:
                if (year % 100) == 0:
                    if (year % 400) == 0:
                        ed = '29'
                    else:
                        ed = '28'
                else:
                    ed = '29'
            else:
                ed = '28'

            ed = dic["EY"] + '-' + em + '-' + ed
            
            query = "INSERT INTO SchedulingAd([AId],[ScheduleMethod],[RecurringMethod],[SD],[ED],[SM],[EM],[SY],[EY],[IsRPAProcessed],[Counter],[UniqueId],[CreatedBy],[ModifiedBy]) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
            values = (dic['AId'],"Recurring",dic["RecurringMethod"],sd,ed,dic['SM'],dic['EM'],dic['SY'],dic['EY'],0,counter,uniqueid,dic['UserId'],dic['UserId'])

        elif dic["RecurringMethod"] == "quarter":
            m = session_dic["Months"].index(dic["SM"]) +1
            if m in (1,2,3):
                m = '01'
            elif m in (4,5,6):
                m = '04'
            elif m in (7,8,9):
                m = '07'
            elif m in (10,11,12):
                m = '10'
            sd = dic["SY"] + '-' + m + '-' + '01'

            m = session_dic["Months"].index(dic["EM"]) +1
            if m in (1,3):
                m,d = '01','31'
            elif m == 2:
                m,d = '01','28'
            elif m in (4,6):
                m,d = '04','30'
            elif m == 5:
                m,d = '04','31'
            elif m in (7,8):
                m,d = '07','31'
            elif m == 9:
                m,d = '09','30'
            elif m in (10,12):
                m,d = '10','31'
            elif m == 11:
                m,d = '10','30'
            ed = dic["EY"] + '-' + m + '-' + d

            query = "INSERT INTO SchedulingAd([AId],[ScheduleMethod],[RecurringMethod],[SD],[ED],[SM],[EM],[SY],[EY],[IsRPAProcessed],[Counter],[UniqueId],[CreatedBy],[ModifiedBy]) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
            values = (dic['AId'],"Recurring",dic["RecurringMethod"],sd,ed,dic['SM'],dic['EM'],dic['SY'],dic['EY'],0,counter,uniqueid,dic['UserId'],dic['UserId'])

        elif dic["RecurringMethod"] == "year":
            if dic["YT"] == "calendar":
                sd = dic["SY"] + '-' + '01'+ '-' + '01'
                ed = dic["EY"] + '-' + '12'+ '-' + '31'
            elif dic["YT"] == "financial":
                sd = dic["SY"] + '-' + '04' + '-' + '01'
                ed = dic["EY"] + '-' + '03' + '-' + '30'

            query = "INSERT INTO SchedulingAd([AId],[ScheduleMethod],[RecurringMethod],[SD],[ED],[SY],[EY],[YearType],[IsRPAProcessed],[Counter],[UniqueId],[CreatedBy],[ModifiedBy]) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)"
            values = (dic['AId'],"Recurring",dic["RecurringMethod"],sd,ed,dic['SY'],dic['EY'],dic["YT"],0,counter,uniqueid,dic['UserId'],dic['UserId'])

    db.update(query,values)

    query = "UPDATE Ad SET Modifiedby = ?,ModifiedDate = Getdate(), [ProcessNumber] = CASE when  ProcessNumber = 1 then 2 else ProcessNumber end where Id = ?"
    values = (dic['UserId'],aid)
    db.update(query,values)
