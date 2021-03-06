import os
import base64
import shutil
import pandas as pd
from datetime import datetime, timedelta

from .views import session_dic
from .logger import logger, config_dic

# from .azure import AZURE, getListOfFiles, create_directory_local, share_name


def dashboardfilter(db, dic, module, record_status, for_user=False, advertiser_needed=False, location_needed=False):
    """ will extract data from DB as per the filter applied

    Args:
        dic ([dic]): object consist of applied filters
        module ([int]): 1:planning, 2:scheduling, 3:tracking, 4:reporting
        record_status ([int]): 0:Ongoing, 1:Completed
        for_user (bool, optional): if for a specific user userid.Defaults to False.
        client_needed(bool, optional): if True will return client dropdown also
        brand_needed(bool, optional): if True will return brand dropdown also

    Returns:
        [type]: [description]
    """

    filter_adid = False
    filter_adname = False
    dic = {k.lower(): v for k, v in dic.items()}
    userid = dic["userid"]
    dic.pop('isdefault')
    dic.pop('userid')

    # if for_user == False:
    #     dic.pop('userid')
    result_dic = {}
    if 'startdate' in dic:
        datefilter = True
        start_date = datetime.strptime(dic['startdate'], '%Y-%m-%d')
        end_date = datetime.strptime(dic['enddate'], '%Y-%m-%d')
        dic.pop('startdate')
        dic.pop('enddate')
    else:
        datefilter = False
    if 'sessionid' in dic:
        dic.pop('sessionid')

    if "advertiserid" in dic:
        query = "select [Value] from [Advertiser] where id = ?"
        values = (dic['advertiserid'],)
        advertiser_name = db.execute(query, values, as_dic=True)
        advertiser_name = advertiser_name["Value"]
        dic['advertiser'] = f'"{advertiser_name}"'
        dic.pop('advertiserid')

    if "locationid" in dic:
        query = "select [Value] from [Location] where id = ?"
        values = (dic['locationid'],)
        location_name = db.execute(query, values, as_dic=True)
        location_name = location_name["Value"]
        dic['location'] = f'"{location_name}"'
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
    df = db.execute(query, as_dataframe=True)
    original_columns = list(df.columns)
    if df.empty is False:
        df["StartDate"] = pd.to_datetime(df["StartDate"], format='%d/%m/%Y %H:%M')
        df["StartDate"] = df["StartDate"].astype(str)
        new = df["StartDate"].str.split(":", n=2, expand=True)
        df["StartDate"] = new[0].str.cat(new[1], sep=":")
        if record_status != 0:
            df["EndDate"] = pd.to_datetime(df["EndDate"], format='%d/%m/%Y %H:%M')
            df["EndDate"] = df["EndDate"].astype(str)
            new = df["EndDate"].str.split(":", n=2, expand=True)
            df["EndDate"] = new[0].str.cat(new[1], sep=":")

        df.columns = df.columns.str.lower()
        filter_query = ' & '.join(['{}=={}'.format(k, v) for k, v in dic.items()])
        if filter_query != '':
            df = df.query(filter_query)
        if filter_adid:
            df = df[df['adid'].str.contains(adid)]
        if filter_adname:
            df = df[df['adname'].str.contains(adname)]
        if datefilter:
            df = df[df["startdate"].isin(pd.date_range(start_date, end_date))]
    df.columns = original_columns
    data = df.to_dict(orient='records')
    result_dic["Records"] = data
    if location_needed:
        result_dic["Location"] = session_dic["Location"]
    if advertiser_needed:
        query = "EXEC GetAdvertiser"
        result_dic["Advertiser"] = db.execute(query)
    return result_dic


def scheduling_page_insertion(dic, db):
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

    if onetimevalue:
        query = "INSERT INTO SchedulingAd([AId], [ScheduleMethod], [SD], [ED], [IsRPAProcessed], [Counter], [UniqueId], [CreatedBy], [ModifiedBy]) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)"
        values = (dic['AId'], "OneTime", dic['SD'], dic['ED'], 0, counter, uniqueid, dic['UserId'], dic['UserId'])
    else:
        query = "SELECT ProcessNumber from Ad where id = ?"
        values = (dic['AId'],)
        process_number = db.execute(query, values, as_dic=True)
        process_number = int(process_number["ProcessNumber"])
        if dic["RecurringMethod"] in ("daily", "week"):
            if process_number == 1:
                query = "INSERT INTO SchedulingAd([AId], [ScheduleMethod], [RecurringMethod], [SD], [ED], [IsRPAProcessed], [Counter], [UniqueId], [CreatedBy], [ModifiedBy]) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
                values = (dic['AId'], "Recurring", dic["RecurringMethod"], dic['SD'], dic['ED'], 0, counter, uniqueid, dic['UserId'], dic['UserId'])
            else:
                query = "UPDATE SchedulingAd set [ED] = ?, [ModifiedBy] = ?, [ModifiedDate] = GetDate()) where AId = ?"
                values = (dic['ED'], dic['UserId'], dic['AId'])

        elif dic["RecurringMethod"] == "month":
            sd = dic["SY"] + '-' + str(session_dic["Months"].index(dic["SM"]) + 1).zfill(2) + '-' + '01'
            em = str(session_dic["Months"].index(dic["EM"]) + 1).zfill(2)
            year = int(dic["EY"])
            if int(em) in (1, 3, 5, 7, 8, 10, 12):
                ed = '31'
            elif int(em) in (4, 6, 9, 11):
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

            if process_number == 1:
                query = "INSERT INTO SchedulingAd([AId], [ScheduleMethod], [RecurringMethod], [SD], [ED], [SM], [EM], [SY], [EY], [IsRPAProcessed], [Counter], [UniqueId], [CreatedBy], [ModifiedBy]) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
                values = (dic['AId'], "Recurring", dic["RecurringMethod"], sd, ed, dic['SM'], dic['EM'], dic['SY'], dic['EY'], 0, counter, uniqueid, dic['UserId'], dic['UserId'])
            else:
                query = "UPDATE SchedulingAd set [ED] = ?, [EM] = ?, [EY] = ?, [ModifiedBy] = ?, [ModifiedDate] = GetDate()) where AId = ?"
                values = (ed, dic['EM'], dic['EY'], dic['UserId'], dic['AId'])

        # elif dic["RecurringMethod"] == "quarter":
        #     m = session_dic["Months"].index(dic["SM"]) + 1
        #     if m in (1, 2, 3):
        #         m = '01'
        #     elif m in (4, 5, 6):
        #         m = '04'
        #     elif m in (7, 8, 9):
        #         m = '07'
        #     elif m in (10, 11, 12):
        #         m = '10'
        #     sd = dic["SY"] + '-' + m + '-' + '01'

        #     m = session_dic["Months"].index(dic["EM"]) + 1
        #     if m in (1, 3):
        #         m, d = '01', '31'
        #     elif m == 2:
        #         m, d = '01', '28'
        #     elif m in (4, 6):
        #         m, d = '04', '30'
        #     elif m == 5:
        #         m, d = '04', '31'
        #     elif m in (7, 8):
        #         m, d = '07', '31'
        #     elif m == 9:
        #         m, d = '09', '30'
        #     elif m in (10, 12):
        #         m, d = '10', '31'
        #     elif m == 11:
        #         m, d = '10', '30'
        #     ed = dic["EY"] + '-' + m + '-' + d

        #     query = "INSERT INTO SchedulingAd([AId], [ScheduleMethod], [RecurringMethod], [SD], [ED], [SM], [EM], [SY], [EY], [IsRPAProcessed], [Counter], [UniqueId], [CreatedBy], [ModifiedBy]) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
        #     values = (dic['AId'], "Recurring", dic["RecurringMethod"], sd, ed, dic['SM'], dic['EM'], dic['SY'], dic['EY'], 0, counter, uniqueid, dic['UserId'], dic['UserId'])

        # elif dic["RecurringMethod"] == "year":
        #     if dic["YT"] == "calendar":
        #         sd = dic["SY"] + '-' + '01' + '-' + '01'
        #         ed = dic["EY"] + '-' + '12' + '-' + '31'
        #     elif dic["YT"] == "financial":
        #         sd = dic["SY"] + '-' + '04' + '-' + '01'
        #         ed = dic["EY"] + '-' + '03' + '-' + '30'

        #     query = "INSERT INTO SchedulingAd([AId], [ScheduleMethod], [RecurringMethod], [SD], [ED], [SY], [EY], [YearType], [IsRPAProcessed], [Counter], [UniqueId], [CreatedBy], [ModifiedBy]) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
        #     values = (dic['AId'], "Recurring", dic["RecurringMethod"], sd, ed, dic['SY'], dic['EY'], dic["YT"], 0, counter, uniqueid, dic['UserId'], dic['UserId'])

    db.update(query, values)
    if process_number == 1:
        query = "UPDATE Ad SET Modifiedby = ?, ModifiedDate = Getdate(), [ProcessNumber] = CASE when  ProcessNumber = 1 then 2 else ProcessNumber end where Id = ?"
        values = (dic['UserId'], aid)
        db.update(query, values)
        query = "exec UpdateAdvisementStatus @AId = ?, @UniqueId = 1120, @Comments = 'Scheduling Advisement Created', @CreatedBy = ?"
        values = (aid, dic['UserId'])
        db.update(query, values)
    else:
        query = "exec UpdateAdvisementStatus @AId = ?, @UniqueId = 1160, @Comments = 'Scheduling Advisement Update', @CreatedBy = ?"
        values = (aid, dic['UserId'])
        db.update(query, values)


def download_file(data, share_name=config_dic["ShareName"], mode=1):
    """ used to download files from flow application

    Args:
        data (dic): code data related to the operation
        share_name (str, optional): share name in Azure storage. Defaults to config_dic["ShareName"].
        mode (int, optional): mode specifies the set of operations need to be performed . Defaults to 1.
                                mode = 1 --> provides blob data of requested file
                                mode = 2 --> provides local file path of requested file
                                mode = 3 --> provides blob data of requested local file path
    """

    az = AZURE()
    if "file_path" in data:
        file_paths = data["file_path"]
    elif "FilePath" in data:
        file_paths = data["FilePath"]

    """ if there is only one file to download it will convert file to blob data. If request is for more file then will zip all file and convert it to blob  """
    if len(file_paths) > 1:

        """ download request for more than one file """
        logger.info("download request for more than one file")
        listOfFiles = getListOfFiles(str(config_dic["Tempmultiplefilefolder"])+'\\')

        """ deleting old files in local folder """
        if listOfFiles:
            for i in listOfFiles:
                os.remove(i)
                logger.info(f"deleted old file: {i}")

        """ downloading requested files from azure to local folder """
        for i in file_paths:
            temp_path = i.split(':')[-1].split('\\')
            del temp_path[0]
            temp_path_1 = '/'.join(temp_path)
            file_name = temp_path_1.split('/')[-1]
            del temp_path[-1]
            dir_name = '/'.join(temp_path)
            local_path = config_dic["Tempmultiplefilefolder"]
            az.download_file_azure(share_name, dir_name, file_name, local_path)
            logger.info(f"Downloaded azure file {dir_name} + {file_name} to local path {local_path} ")

        """ deleting old zips in local folder """
        listOfFiles = getListOfFiles(str(config_dic["Tempzipfolder"]+'\\'))
        if (listOfFiles):
            for i in listOfFiles:
                os.remove(i)
                logger.info(f"deleted old zip: {i}")

        output_filename = str(config_dic["Tempzipmultiplefilefolder"])
        dir_name = str(config_dic["Tempmultiplefilefolder"])
        shutil.make_archive(output_filename, 'zip', dir_name)

        """ zipping files in the folder and blob conversion"""
        with open(output_filename+str('.zip'), 'rb') as fo:
            blob = base64.b64encode(fo.read())
            fo.close()
        return (blob)

    else:
        """ request to download single file """
        logger.info("request to download single file")
        listOfFiles = getListOfFiles(config_dic["Tempsinglefile"]+'\\')

        """ deleting old files in local folder """
        if listOfFiles:
            for i in listOfFiles:
                os.remove(i)
                logger.info(f"deleted old file: {i}")

        local_path = config_dic["Tempsinglefile"]

        if mode == 3:
            """ blob conversion"""
            data = open(file_paths[0], "rb").read()
            blob = base64.b64encode(data)
            return blob
        else:
            temp_path = file_paths[0].split(':')[-1].split('\\')
            del temp_path[0]
            temp_path_1 = '/'.join(temp_path)
            file_name = temp_path_1.split('/')[-1]
            del temp_path[-1]
            dir_name = '/'.join(temp_path)

            """ downloading requested files from azure to local folder """
            az.download_file_azure(share_name, dir_name, file_name, local_path)

            if mode == 1:
                """ blob conversion"""
                data = open(local_path + '\\' + str(file_name), "rb").read()
                blob = base64.b64encode(data)
                return blob
            else:
                return (local_path + '\\' + str(file_name))
