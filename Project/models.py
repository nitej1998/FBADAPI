import pandas as pd
import pyodbc 
import sqlalchemy
import logging

from sqlalchemy import create_engine, exc
from time import time
from .logger import config_dic,logger
from datetime import datetime,timedelta,date
from dateutil.rrule import rrule, MONTHLY

class DB(object):
    def __init__(self, database = config_dic["DataBase"], server=config_dic["Server"], user=config_dic["User"], password=config_dic["Password"], driver = config_dic["Driver"]):
        """
        Initialization of databse object.

        Args:
            database (str): The database to connect to.
            server (str): server name where database is hosted. 
            user (str): Username of MsSQL server.
            password (str): Password of MsSQL server. 
        """

        self.SERVER = server
        self.USER = user
        self.PASSWORD = password
        self.DATABASE = database
        self.DRIVER = driver


        logging.info(f'Servers: {self.SERVER}')
        logging.info(f'Database: {self.DATABASE}')

        self.connect()

    def connect(self, max_retry=5):
        """ Will create a pointer to access database

        Args:
            max_retry (int, optional): Max number of time API is allowed is try to connect . Defaults to 5.

        Returns:
            [pointer]: Data base connection pointer
        """
        retry = 1

        try:
            start = time()
            logging.debug(f'Making connection to `{self.DATABASE}`...')
            # format: 'mssql+pyodbc://user:password@server/database?driver='
            config = f'mssql+pyodbc://{self.USER}:{self.PASSWORD}@{self.SERVER}/{self.DATABASE}?driver={self.DRIVER}'
            self.db_ = create_engine(config, connect_args={'connect_timeout': 2}, pool_recycle=300)
            logging.info(f'Engine created for `{self.DATABASE}`')
            while retry <= max_retry:
                try:
                    self.engine = self.db_.connect()
                    logging.info(f'Connection established succesfully to `{self.DATABASE}`! ({round(time() - start, 2)} secs to connect)')
                    break
                except Exception as e:
                    logging.warning(f'Connection failed. Retrying... ({retry}) [{e}]')
                    retry += 1
                    self.db_.dispose()
        except:
            raise Exception('Something went wrong while connecting. Check trace.')

    def execute(self, query,params = None,conveter = [],as_dataframe = False,as_dic = False,set_none = True,as_list = False):
        """ Will execute an query of return type

        Args:
            query ([str]): query yet to execute
            params(list, optional): Peramaters for place holder in query. Default to None  
            conveter (list, optional): will convert data format to '%d/%m/%Y %H:%M' for requested columns . Defaults to empty list [].
            as_dataframe (bool, optional): If True return result as Data frame. Defaults to False.
            as_dic (bool, optional): If True return result as dictonary. Defaults to False.

        Raises:
            pyodbc.ProgrammingError: If connection is not established raises connection error 

        Returns:
            result of query bases on requested format
        """
        try:
            logging.info(f'Query: {query}')
            try:
                if params != None:
                    logging.info(f'Peramaters: {str(params)}')
                    df = pd.read_sql(query,self.engine,params=params)
                else:
                    df = pd.read_sql(query,self.engine)
            except exc.ResourceClosedError:
                logging.warning('Query does not have any value to return.')
                return True
            except exc.IntegrityError as e:
                logging.warning(f'Integrity Error - {e}')
                return None
            except:
                logging.exception('Something went wrong executing query. Check trace.')
                return False

            if set_none == True:
                df = df.where(pd.notnull(df), None)
            
            for i in conveter:
                try:
                    df[i] = pd.to_datetime(df[i],format = '%d/%m/%Y %H:%M')
                    df[i] = df[i].astype(str)
                    new = df[i].str.split(":", n = 2, expand = True)
                    df[i] = new[0].str.cat(new[1], sep =":")
                except KeyError :
                    pass
                except ValueError :
                    df[i] = pd.to_datetime(df[i],format = '%Y-%m-%d')
                    df[i] = df[i].astype(str)

            if as_dataframe == True:
                data = df
                logging.info('Result: It is data frame')
            elif as_dic ==  True:
                try:
                    data = df.to_dict(orient='records')[0]
                except:
                    data = {}
                logging.info(f'Result: {str(data)}')
            elif as_list == True:
                data = list(df[df.columns[0]])
                logging.info(f'Result: {str(data)}')
            else:
                data = df.to_dict(orient='records')
                logging.info('Result: It is List of dic')
            return data
        
        except pyodbc.ProgrammingError as e:
            if 'No results.  Previous SQL was not a query.' in str(e):
                logging.info('Provided SQL query has no return type.Check trace.')
            elif 'Not connected' in str(e):
                logging.info("Data base connection issue using out dated curser")
            else:
                logging.info('Something went wrong executing query. Check trace.')
            logging.debug(str(e))
            self.error = str(e)
            self.status = False
            
        except Exception as e :
            logging.warning('Something went wrong executing query. Check trace.')
            logging.debug(str(e))
            self.status = False
            self.error = str(e)
            return False
    
    def update(self,query,values = None):
        """ execute an query of no return type

        Args:
            query ([str]): query yet to execute

        Returns:
            [bool]: True for successful execution, False for failure case 
        """
        try:
            logging.info(f'Query: {query}')
            if values != None:
                logging.info(f'Values: {values}')
                self.execute(query,values)
            else:
                self.execute(query)
                print(1)
            return True
        except Exception as e :
            logging.warning('Something went wrong executing query. Check trace.')
            logging.warning('Data base expection {}'.format(e))
            self.status = False
            self.error = str(e)
            return False
    
# creating a session dic which will have all required data that need to be pulled from data base all the time 
# as a result which helps to reduce no of hits to data bass and increse speed of the request also 
logger.info('connecting to database for session dic creation...')
db = DB()
session_dic = {}
query = "EXEC GetLocation"
session_dic["Location"] = db.execute(query)
query = "EXEC GetAdvertiser"
session_dic["Advertiser"] = db.execute(query)
query = "EXEC GetAdCategory"
session_dic["AdCategory"] = db.execute(query)
query = "EXEC GetFbKeyWord"
session_dic["FbKeyWord"] = db.execute(query)
query = "EXEC GetStatus"
session_dic["FbStatus"] = db.execute(query)
logger.info('Session dic creation completed :)')
logger.info('Disconnecting database object utilized for session cereation')
# need to add
logger.info('dataabse disconnected')
