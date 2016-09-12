# -*- coding: utf-8 -*-
"""
Created on Fri Jan 29 15:33:01 2016

@author: bolx
"""
from flask import Flask
from flask import request

import sys, logging
import util

# sys.path.insert(0, 'support_service')
import support_service.support_service_handle as support

# sys.path.insert(0, 'statistic')
import statistic.statistics_service_handles as statistics
import statistic.statistics_time_delta_service_handle as time_late

# sys.path.insert(0, 'new_view_detection')
import new_view_detection.new_detection_service_handle as detect_view

app = Flask(__name__)
log_file = 'logs/bolx_service.log'
logger = logging.getLogger("bolx service")
logger.setLevel(logging.INFO)
# util.set_log_file(logger, log_file)


@app.route('/')
def hello_world():
    return '<h1>Welcome to bolx service!</h1><p></p>'


@app.route('/get_home_page', methods=['GET'])
def get_home_page():
    logger.info('================   Support: Get home page   =================')
    return support.get_home_page(request.args)


@app.route('/get_event_data', methods=['GET'])
def get_event_data():
    logger.info('================   Support: get event data   =================')
    return support.get_event_data(request.args)


@app.route('/user_report')
def get_result():
    logger.info('================   Support: receive user data   =================')
    return support.store_result(request.args)


"""========================  Statistic service  ============================"""


@app.route('/get_domains')
def get_domains():
    logger.info('\n================   Statistics: Get all domain   =================')
    return statistics.get_home_page()


@app.route('/get_count', methods=['GET'])
def get_statistic():
    logger.info('\n================   Statistics: Get statistics   =================')
    return statistics.get_statistic(request.args)


@app.route('/get_loss', methods=['GET'])
def get_loss():
    logger.info('\n================   Statistics: Get loss   =================')
    return statistics.get_domain_loss(request.args)


@app.route('/get_time_late', methods=['GET'])
def get_statistic_time_late():
    logger.info('\n================   Statistics: Get time late avg   =================')
    return time_late.get_late_time(request.args)


@app.route('/get_domain_slow_more', methods=['GET'])
def get_slow_more():
    logger.info('\n================   Statistics: Get slow   =================')
    return time_late.get_slow_more(request.args)


@app.route('/get_late_ratio', methods=['GET'])
def get_statistic_late_ratio():
    logger.info('\n================   Statistics: Get late ratio   =================')
    return time_late.get_ratio_late(request.args)


@app.route('/get_slow', methods=['GET'])
def get_slow():
    logger.info('\n================   Statistics: Get slow   =================')
    return time_late.get_slow(request.args)


"""========================================================================="""


@app.route('/detect_view_1', methods=['GET'])
def get_detect_1():
    # logger.info()
    try:
        html = detect_view.point_view(request.args)
    except Exception as e:
        print e.message
        return 'some error'
    #print html
    return html

if __name__ == '__main__':
    util.set_log_console(logger)
    app.run(host='localhost', port=5005)
