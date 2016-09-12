import sys, online_dao, dao, online_resources, static_resources, collections, news_domain_statistics
import numpy as np
import json, visualize_hier_sub, util, event_dao, resources, trend_util

# import sub_util
sys.path.insert(0, 'news_visualize')
from flask import Flask, render_template
from flask import request

app = Flask(__name__)
TREND_LENGTH_THRESHOLD = 2
CATID_PAR = 'catid'
CATID_ALL = -1


@app.route('/')
def hello_world():
    return render_template('index.html')


@app.route('/test', methods=['GET'])
def test():
    name = request.args.get('name')
    return name


@app.route('/get_event', methods=['GET'])
def get_event():
    date1 = request.args.get('date1')
    print 'date1 = %s' % date1
    date2 = request.args.get('date2')
    print 'date2 = %s' % date2
    app.logger.info('get event: %s and %s' % (date1, date2))
    catid = CATID_ALL
    if CATID_PAR in request.args:
        catid = int(request.args.get(CATID_PAR))
    return handle_event(date1, date2, catid)


def handle_event(date1, date2, catid):
    today = util.get_today_str()
    # if (date1 == date2 and date2 == today):
    #    return get_today_event()
    rows = event_dao.get_event_in_range(date1, date2, catid=catid)
    today_events = []
    comp = util.compare_dates(date2, today)
    if (comp == util.GREATER or comp == util.EQUAL):
        today_events = online_dao.get_real_time_events(catid)
    data = dict()
    for row in rows:
        trend_util.handle_event_object(row)
        date = row[resources.EVENT_DATE]
        if date not in data:
            data[date] = [row]
        else:
            data[date].append(row)
    if (len(today_events) > 0):
        for event in today_events:
            trend_util.handle_event_object(event)
            date = event[resources.EVENT_DATE]
            trend_id = event[resources.EVENT_TREND_ID]
            if (trend_id == -1):
                event[resources.EVENT_TREND_ID] = event[resources.EVENT_ID]
            if date not in data:
                data[date] = [event]
            else:
                data[date].append(event)
    for date in data:
        events = data[date]  # sort events by sizes
        events = sort_events(events)
        data[date] = events
    return json.dumps(data, sort_keys=True)


@app.route('/get_trend', methods=['GET'])
def get_trend():
    date1 = request.args.get('date1')
    date2 = request.args.get('date2')
    app.logger.info('get trends between  %s,%s' % (date1, date2))
    catid = CATID_ALL
    if CATID_PAR in request.args:
        catid = int(request.args.get(CATID_PAR))
    return handle_trend(date1, date2, catid)


@app.route('/get_urls', methods=['GET'])
def get_a_urls():
    aids = request.args.get('ids')
    return get_article_urls(aids)


def get_article_urls(article_ids):
    aids = util.str_to_int_array(article_ids, ',')
    rows = event_dao.get_article_urls(aids)
    # create a map
    result = dict()
    for i in range(len(rows)):
        row = rows[i]
        temp = event_dao.get_article_url_infor(row)
        result[temp[dao.ID]] = temp
    final_list = []
    for i in range(len(aids)):
        final_list.append(result[aids[i]])
    return json.dumps(final_list)


def get_trend_img_url(trend_id):
    trend_img = trend_util.get_trend_top_img_path(trend_id)
    trend_img = trend_util.get_img_url(trend_img)
    return trend_img


def handle_trend(date1, date2, catid):
    rows = event_dao.get_trend_between_day(date1, date2, TREND_LENGTH_THRESHOLD, catid)
    for row in rows:
        row[resources.TREND_START_DATE] = str(row[resources.TREND_START_DATE])
        row[resources.TREND_LAST_DATE] = str(row[resources.TREND_LAST_DATE])
        trend_img = get_trend_img_url(row[resources.TREND_ID])
        row[resources.TREND_IMG] = trend_img
        del row[resources.TREND_CENTER]
    return json.dumps(rows)


@app.route('/get_trend_by_id', methods=['GET'])
def get_trend_by_id():
    trend_id_str = request.args.get('trend_id')
    trend_id = int(trend_id_str)
    return get_trend_infor(trend_id)


def get_trend_infor(trend_id):
    trend = event_dao.fetch_trend_by_id(trend_id)
    c_event_ids = online_dao.get_event_id_list_in_trend(trend_id)
    del trend[resources.TREND_CENTER]
    trend[resources.TREND_IMG] = get_trend_img_url(trend_id)
    event_list = trend[resources.TREND_EVENT_IDS]
    event_ids = util.str_to_int_array(event_list, ',')
    events = []
    leng = len(c_event_ids) - 1
    if (leng >= 0):
        for i in range(len(c_event_ids)):
            event = online_dao.get_event_by_id(c_event_ids[leng - i])
            trend_util.handle_event_object(event)
            events.append(event)
    leng = len(event_ids) - 1
    trend[resources.TREND_EVENT_NUM] += len(c_event_ids)
    for i in range(len(event_ids)):
        event = event_dao.get_event_by_id(event_ids[leng - i])
        trend_util.handle_event_object(event)
        events.append(event)
    trend[resources.TREND_EVENT_IDS] = events
    trend[resources.TREND_START_DATE] = str(trend[resources.TREND_START_DATE])
    trend[resources.TREND_LAST_DATE] = str(trend[resources.TREND_LAST_DATE])
    return json.dumps(trend)


def sort_events(events):
    sizes = []
    for e in events:
        sizes.append(e[resources.EVENT_SIZE])
    sizes_array = np.array(sizes)
    agrsort = np.argsort(sizes_array)
    result = []
    leng = len(events)
    for i in range(len(agrsort)):
        result.append(events[agrsort[leng - 1 - i]])
    return result


@app.route('/save_feedback', methods=['GET'])
def save_feedback():
    message = request.args.get('message')
    event_id = request.args.get('event_id')
    print 'event_id: %s' % event_id
    trend_id = request.args.get('trend_id')
    print 'trend_id = %s' % trend_id
    print 'message = %s' % message
    try:
        event_id_num = int(event_id)
        trend_id_num = int(trend_id)
        feed_back(message, event_id_num, trend_id_num)
        return 'message has been sent to Mr Khai'
    except:
        return 'failed to send feedback'


def feed_back(message, event_id, trend_id):
    today = util.get_today_str()
    event_dao.save_message(message, today, event_id, trend_id)


@app.route('/get_events_by_ids', methods=['GET'])
def get_event_by_ids():
    idstr = request.args.get('ids')
    # return idstr
    ids = util.str_to_int_array(idstr, ',')
    print idstr
    return get_events_by_id(ids)


@app.route('/get_real_time_events', methods=['GET'])
def get_real_time_trend():
    print 'ok?'
    if CATID_PAR in request.args:
        catid_filter = int(request.args.get('catid'))
    else:
        catid_filter = CATID_ALL
    print 'start'
    # catid_filter =-1
    result = collections.OrderedDict()
    today = util.get_today_str()
    (events, trend_ids) = online_dao.get_current_events(today, catid_filter)
    events = sort_events(events)
    result[today] = events
    yesterday = util.get_yesterday_str()
    print 'yesterday'
    y_events = get_events_on_day_with_trend(yesterday, trend_ids, catid_filter)
    y_events = sort_events(y_events)
    print 'final json'
    result[yesterday] = y_events
    return json.dumps(result)


@app.route('/get_trend_on_day', methods=['GET'])
def get_trends_on_day():
    day = request.args.get('date')
    if CATID_PAR in request.args:
        catid_filter = int(request.args.get('catid'))
    else:
        catid_filter = CATID_ALL
    current_trend_ids = online_dao.get_current_trend_ids()
    events = get_events_on_day_with_trend(day, current_trend_ids, catid_filter)
    events = sort_events(events)
    result = dict()
    result[day] = events
    return json.dumps(result)


def get_events_on_day_with_trend(day, except_trend_ids=None, catid_filter=-1):
    print 'except: %s' % (str(except_trend_ids))
    if (except_trend_ids):
        trends = event_dao.get_trend_in_day_except(day, except_trend_ids, catid_filter)
    else:
        trends = event_dao.get_trend_by_last_update(day, catid_filter)
    events = []
    cat_map = static_resources.CATMAP
    for trend in trends:
        event_list_str = trend[resources.TREND_EVENT_IDS]
        event_list = util.str_to_int_array(event_list_str, ',')
        (series, total) = online_dao.get_scores_from_events(event_list, online_resources.EVENT_DISPLAY_NUM)
        last_event = event_dao.get_event_by_id(event_list[-1])
        hour_ob = dict()
        hour_ob['series'] = series
        hour_ob['total'] = total
        last_event[online_resources.HOUR_SERIES] = hour_ob

        last_event['offset'] = 0
        last_event['trend_id'] = trend[resources.TREND_ID]
        catid = resources.EVENT_CATID
        last_event[catid] = cat_map[last_event[catid]]
        trend_util.handle_event_object(last_event)
        events.append(last_event)
    return events


@app.route('/get_categories', methods=['GET'])
def get_categories():
    return json.dumps(static_resources.CATMAP)


@app.route('/get_trend_urls', methods=['GET'])
def get_trend_urls():
    trend_id = int(request.args.get('trend_id'))
    print trend_id
    event_offset = int(request.args.get('event_offset'))
    return handle_trend_urls(trend_id, event_offset)


def handle_trend_urls(trend_id, event_offset):
    (result, date, leng, event_id) = event_dao.get_url_in_trend(trend_id, event_offset)
    if (result):
        temp = dict()
        temp['date'] = date
        temp['urls'] = result
        temp['event_id'] = event_id
        temp['event_num'] = leng
        return json.dumps(temp)
    else:
        return ''


@app.route('/get_domains', methods=['GET'])
def get_domain_map():
    return json.dumps(static_resources.DOMAIN_TO_ID)


DOC_COUNT_STAT = 1
ERROR_DOC_COUNT = 2
AVERGE_GAP_TIME = 3

DETECT_TIME = 1
TOTAL_DOC_COUNT = 2


@app.route('/get_statistics', methods=['GET'])
def get_doc_count_statistics():
    date1 = request.args.get('date1')
    date2 = request.args.get('date2')
    selected_domain_str = request.args.get('domains')
    stat_type = int(request.args.get('type'))
    if selected_domain_str == '-1':
        domain_ids = static_resources.ID_TO_DOMAIN.keys()
    else:
        domain_ids = util.str_to_int_array(selected_domain_str, ',')
    if (stat_type == DOC_COUNT_STAT):
        data = news_domain_statistics.get_stat_doc_count(date1, date2, domain_ids)
    elif (stat_type == ERROR_DOC_COUNT):
        data = news_domain_statistics.get_error_create_time_count(date1, date2, domain_ids)
    elif (stat_type == AVERGE_GAP_TIME):
        data = news_domain_statistics.get_averg_gap(date1, date2, domain_ids)
    return json.dumps(data)


@app.route('/get_detect_event_statistics', methods=['GET'])
def get_detect_time():
    date1 = request.args.get('date1')
    date2 = request.args.get('date2')
    stat_type = int(request.args.get('type'))
    if (stat_type == DETECT_TIME):
        print 'detect time'
        data = news_domain_statistics.get_detect_time(date1, date2)
    elif (stat_type == TOTAL_DOC_COUNT):
        print 'doc_count'
        data = news_domain_statistics.get_total_doc_count(date1, date2)
    return json.dumps(data)


def get_events_by_id(event_ids):
    events = event_dao.get_event_by_ids(event_ids, resources.EVENT_TABLE)
    if len(events) == 0:
        events = []
    events2 = event_dao.get_event_by_ids(event_ids, resources.EVENT_CURRENT_TABLE)
    for i in range(len(events2)):
        events.append(events2[i])
    data = dict()
    for event in events:
        trend_util.handle_event_object(event)
        data[event[resources.EVENT_ID]] = event
    pair = []
    leng = len(event_ids)
    date_dict = dict()
    for i in range(leng):
        eid = event_ids[leng - i - 1]
        key = data[eid][resources.EVENT_DATE]
        if (key not in date_dict):
            date_dict[key] = 1
            pair.append((key, data[eid]))
        else:
            order = date_dict[key]
            date_dict[key] += 1
            for i in range(order):
                key += ' '
            pair.append((key, data[eid]))
    result = collections.OrderedDict(pair)
    return json.dumps(result)


def get_table_name(table_id):
    if table_id == -1:
        return 'new_sub_trend_temp'
    if table_id == 0:
        return 'new_sub_trend_temp'
    if table_id == 1:
        return 'new_sub_v3'
    if table_id == 2:
        return 'new_sub_v4'
    if table_id == 3:
        return 'new_sub_v5'
    if table_id == 4:
        return 'sub_event_binh1'
    if table_id == 5:
        return 'sub_event_binh'


def get_hier_table(table_id):
    if table_id == 0:
        return 'hier_sub_v1'
    if table_id == 1:
        return 'hier_sub_v2'
    return 'hier_sub_v1'


@app.route('/get_hier_sub_events', methods=['GET'])
def get_hierarchical_sub():
    print 'gointo hier'
    trend_id = int(request.args.get('trend_id'))
    table = request.args.get('table_id')
    number = int(request.args.get('day_num'))
    print 'trend_id = %d, number = %d, table = %s' % (trend_id, number, table)
    res_str = visualize_hier_sub.gen_html(trend_id, table, number)
    return res_str


@app.route('/get_group_sub_events2', methods=['GET'])
def get_group_sub2():
    print 'gointo hier'
    trend_id = int(request.args.get('trend_id'))
    table_id = request.args.get('table_id')
    table = get_hier_table(table_id)
    print 'trend_id = %d, table = %s' % (trend_id, table)
    res_obj = visualize_hier_sub.get_group_sub_first_time2(trend_id, table, 3)
    res_str = json.dumps(res_obj)
    return res_str


@app.route('/get_more_group_sub_events2', methods=['GET'])
def get_more_group_sub2():
    print 'gointo hier'
    trend_id = int(request.args.get('trend_id'))
    table_id = int(request.args.get('table_id'))
    max_date = request.args.get('load_date')
    table = get_hier_table(table_id)
    print 'trend_id = %d, table = %s, load_date = %s' % (trend_id, table, max_date)
    res_obj = visualize_hier_sub.get_group_sub_with_max_date2(trend_id, table, max_date)
    res_str = json.dumps(res_obj)
    return res_str


@app.route('/get_articles_sub_trend', methods=['GET'])
def get_artilce_of_sub():
    aids = request.args.get('ids')
    print 'process: %s' % str(aids)
    # return get_article_urls_with_visualize(aids)
    return get_article_urls(aids)


if __name__ == '__main__':
    app.run(host=resources.HOST, port=resources.PORT)
# handle_event('2015-08-03','2015-09-04',-1)
# obstr = get_article_urls_with_visualize('63144953')
# print obstr
