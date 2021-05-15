from datetime import datetime

import os
import logging
import json
import pytz

conf_file_path = str(os.getcwd())+'\\Configuration.json'


def get_file_object(file_path):
    """ converts given file in to dictionary"""
    with open(file_path, 'r') as fo:
        path_dict = json.loads(fo.read())
    return path_dict


def get_time():
    """ returns date time as per the given time zone"""
    format = "%Y-%m-%d %H:%M:%S"
    now_utc = datetime.now(pytz.timezone('Asia/Kolkata'))
    p = datetime.strptime(now_utc.strftime(format), '%Y-%m-%d %H:%M:%S')
    return (p)


# creating config dic
config_dic = get_file_object(conf_file_path)

# -------- Logging Function --------------


def logging_handler():
    """
    Expects no parameters

    returns: logger object
    """
    log_filename = "WM_Flow_Logs_" + str(datetime.now().strftime('%d-%m-%Y')) + '.log'
    log_file_path = str(os.getcwd())+f'\\Log files\\{log_filename}'
    log_format = '%(asctime)s- %(levelname)-8s- %(filename)s- %(funcName)s- %(lineno)s- %(message)s'

    logger = logging.getLogger()
    level = config_dic["LoggingLevel"]
    if level == "CRITICAL":
        logger.setLevel(logging.CRITICAL)
    elif level == "ERROR":
        logger.setLevel(logging.ERROR)
    elif level == "WARNING":
        logger.setLevel(logging.WARNING)
    elif level == "INFO":
        logger.setLevel(logging.INFO)
    elif level == "DEBUG":
        logger.setLevel(logging.DEBUG)
    elif level == "NOTSET":
        logger.setLevel(logging.NOTSET)
    else:
        logger.setLevel(logging.DEBUG)

    formatter = logging.Formatter(log_format, datefmt=str(get_time()))

    file_handler = logging.FileHandler(log_file_path)
    file_handler.setFormatter(formatter)
    stream_handler = logging.StreamHandler()

    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)
    return logger


logger = logging_handler()
