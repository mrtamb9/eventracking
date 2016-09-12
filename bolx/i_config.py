# -*- coding: utf-8 -*-
"""
Created on Fri Oct 30 09:24:20 2015

@author: bolx
"""
import sys
sys.path.insert(0, '../')
import resources
import os

"Folder contain data files"
DATA_FOLDER = resources.DATA_FOLDER + "/bolx"

"Folder contain log files"
LOG_FOLDER = resources.LOG_FOLDER + "/bolx"


FILE_DOMAIN_XPATH = os.getcwd()+ '/domain_xpath'
FILE_DOCS_GET_ERROR = LOG_FOLDER + 'docs_error'
FILE_LOG = LOG_FOLDER + 'crawl_image.log'
IMG_TABLE = 'NewsDb.news_image'
IMG_ID = 'news_id'
IMG_URL = 'url_image'
IMG_STATE = 'state'
STATE_NO_IMG = 0
STATE_OK = 1
STATE_GET_AGAIN = 2
STATE_ERROR = 3
TIME_OUT_GET = 10
TIME_OUT_REGET = 20
REAL_TIME_EVENT_TABLE = resources.EVENT_CURRENT_TABLE
REAL_TIME_EVENT_ID = resources.EVENT_ID
REAL_TIME_EVENT_ARTICCLE_IDS = resources.EVENT_ARTICCLE_IDS
EVENT_TABLE= resources.EVENT_TABLE