import pandas as pd
import pyodbc

from .logger import config_dic, logger
from datetime import datetime, timedelta

session = {}


class DB(object):
    """
    DB class create an bridge between API and SQL Data base
    to perform CURD operation
    """

    def __init__(self, database=config_dic["Envirolment"]):
        """
        Initialization of Database object.

        Args:
            databse (str): The database to connect to.
        """
        self.connected = False
        self.conn = None
        self.DATABASE = database
        self.connect()
        self.status = True
        self.error = None

    def connect(self, database=None, max_retry=5):
        """ Will create a pointer to access database

        Args:
            database ([str], optional): The database to connect.
            max_retry (int, optional): Max number of time API is allowed is try to connect. Defaults to 5.

        Returns:
            [pointer]: Data base connection pointer
        """
        try:
            retry = 1
            if database is None:
                database = self.DATABASE
            logger.info(f'Making database connection to `{self.DATABASE}`...')
            while(retry < max_retry):
                try:
                    database_connection_string = config_dic[database]
                    self.conn = pyodbc.connect(database_connection_string)
                    self.conn.autocommit = True
                    self.connected = True
                    logger.info(f'Connection established for `{self.DATABASE}` database')
                    break
                except Exception as e:
                    logger.warning(f'Connection failed. Retrying... ({retry}) [{e}]')
                    retry += 1

        except Exception as e:
            logger.warning(f'Something went wrong while connecting. Check Logs...')
            logger.warning('Error while connecting: {}'.format(str(e)))
            self.error = str(e)
            return False

    def close(self):
        """ Will close the data base connection (pointer)
        """
        if self.conn:
            self.conn.close()
            self.conn = None
            self.connected = False

    def execute(self, query, params=None, conveter=[], as_dataframe=False, as_dic=False, set_none=True, as_list=False):
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
            logger.info(f'Query: {query}')

            if self.connected is False:
                raise pyodbc.ProgrammingError("Not connected")

            if params is not None:
                logger.info(f'Peramaters: {str(params)}')
                df = pd.read_sql(query, self.conn, params=params)
            else:
                df = pd.read_sql(query, self.conn)

            if set_none is True:
                df = df.where(pd.notnull(df), None)

            for i in conveter:
                try:
                    df[i] = pd.to_datetime(df[i], format='%d/%m/%Y %H:%M')
                    df[i] = df[i].astype(str)
                    new = df[i].str.split(":", n=2, expand=True)
                    df[i] = new[0].str.cat(new[1], sep=":")
                except KeyError:
                    pass
                except ValueError:
                    df[i] = pd.to_datetime(df[i], format='%Y-%m-%d')
                    df[i] = df[i].astype(str)

            if as_dataframe is True:
                data = df
                logger.info('Result: It is data frame')
            elif as_dic is True:
                try:
                    data = df.to_dict(orient='records')[0]
                except:
                    data = {}
                logger.info(f'Result: {str(data)}')
            elif as_list is True:
                data = list(df[df.columns[0]])
                logger.info(f'Result: {str(data)}')
            else:
                data = df.to_dict(orient='records')
                logger.info('Result: It is List of dic')
            return data

        except pyodbc.ProgrammingError as e:
            if 'No results.  Previous SQL was not a query.' in str(e):
                logger.info('Provided SQL query has no return type.Check trace.')
            elif 'Not connected' in str(e):
                logger.info("Data base connection issue using out dated curser")
            else:
                logger.info('Something went wrong executing query. Check trace.')
            logger.debug(str(e))
            self.conn.rollback()
            self.error = str(e)
            self.status = False

        except Exception as e:
            logger.warning('Something went wrong executing query. Check trace.')
            logger.debug(str(e))
            self.conn.rollback()
            self.status = False
            self.error = str(e)
            return False

    def update(self, query, values=None):
        """ execute an query of no return type

        Args:
            query ([str]): query yet to execute

        Returns:
            [bool]: True for successful execution, False for failure case
        """
        try:
            logger.info(f'Query: {query}')
            if values is not None:
                logger.info(f'Values: {values}')
                self.conn.cursor().execute(query, values)
            else:
                self.conn.cursor().execute(query)
            try:
                result = self.conn.cursor().fetchone()
                status = result[0][0]
                if status == 0:
                    self.status = False
                    self.conn.rollback()
                self.conn.commit()
                return True
            except pyodbc.ProgrammingError as e:
                if 'No results.  Previous SQL was not a query.' in str(e):
                    return True
        except Exception as e:
            logger.warning('Something went wrong executing query. Check trace.')
            logger.warning('Data base expection {}'.format(e))
            self.conn.rollback()
            self.status = False
            self.error = str(e)
            return False


logger.info('connecting to database for session dic creation...')
db = DB()
session_dic = {}
query = "EXEC GetLocation"
session_dic["Location"] = db.execute(query)
query = "EXEC GetAdCategory"
session_dic["AdCategory"] = db.execute(query)
query = "EXEC GetStatus"
session_dic["FbStatus"] = db.execute(query)

session_dic["Years"] = [2021, 2022, 2023, 2024, 2025, 2026, 2027, 2028, 2029, 2030]
session_dic["Months"] = ["January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December"]

logger.info('Session dic creation completed :)')
logger.info('Disconnecting database object utilized for session cereation')
db.close()
logger.info('dataabse disconnected')
