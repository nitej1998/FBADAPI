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
    userid = dic["userid"]
    dic.pop('isdefault')
    if for_user == False:
        dic.pop('userid')
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
            query = 'EXEC GetOngoingadvisement @UserId = ?'	
        else:
            query = 'EXEC GetCompletedadvisement @UserId = ?'

    values = (userid,)

    df = db.execute(query,as_dataframe=True,params=values)
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
