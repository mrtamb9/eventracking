import datetime
import sys

import pymysql

import i_config
import i_get_image as get_image
import i_util

# from bolx import config

sys.path.insert(0, '../')
import event_dao, resources, dao, util


def get_real_time_event():
    query = 'SELECT %s, %s FROM %s' % (
        i_config.REAL_TIME_EVENT_ID, i_config.REAL_TIME_EVENT_ARTICCLE_IDS, i_config.REAL_TIME_EVENT_TABLE)
    conn = dao.get_connection()
    cur = conn.cursor(pymysql.cursors.DictCursor)
    cur.execute(query)
    rows = cur.fetchall()
    dao.free_connection(conn, cur)
    return rows


def process_real_time():
    events = get_real_time_event()
    for event in events:
        get_image.logger.info('process event: %s' % (event[i_config.REAL_TIME_EVENT_ID]))
        sids = util.get_full_article_ids(event[i_config.REAL_TIME_EVENT_ARTICCLE_IDS])
        ids = get_ids_not_exist(sids)
        if len(ids) == 0:
            continue
        imgs = get_image.get_img_with_ids(ids)
        conn = dao.get_connection()
        cur = dao.get_cursor(conn)
        for i in ids:
            insert_img(cur, i, imgs[i])
        conn.commit()
        dao.free_connection(conn, cur)
    pass


def get_ids_not_exist(ids):
    conn = dao.get_connection()
    cur = dao.get_cursor(conn)
    query = u'SELECT %s FROM %s WHERE %s IN (%s)' % (
        i_config.IMG_ID, i_config.IMG_TABLE, i_config.IMG_ID, i_util.ids_to_str(ids))
    cur.execute(query)
    rows = cur.fetchall()
    dao.free_connection(conn, cur)
    exist_ids = [row[0] for row in rows]
    result = [i for i in ids if i not in exist_ids]
    return result


def get_ids_img_null(ids):
    conn = dao.get_connection()
    cur = dao.get_cursor(conn)
    query = u'SELECT %s FROM %s WHERE %s IN (%s) and %s is null' % (
        i_config.IMG_ID, i_config.IMG_TABLE, i_config.IMG_ID, i_util.ids_to_str(ids), i_config.IMG_URL)
    cur.execute(query)
    rows = cur.fetchall()
    dao.free_connection(conn, cur)
    exist_ids = [row[0] for row in rows]
    return exist_ids


def insert_img(cur, news_id, url_image):
    query = u'INSERT INTO {0} ({4}, {5}, {6}) VALUES ({1},"{2}", {3});'.format(i_config.IMG_TABLE, news_id,
                                                                               url_image[1], url_image[0],
                                                                               i_config.IMG_ID, i_config.IMG_URL,
                                                                               i_config.IMG_STATE)
    if url_image[1] is None:
        query = u'INSERT INTO {0} ({3}, {4}, {5}) VALUES ({1},null, {2});'.format(i_config.IMG_TABLE, news_id,
                                                                                  url_image[0], i_config.IMG_ID,
                                                                                  i_config.IMG_URL, i_config.IMG_STATE)
    try:
        cur.execute(query)
    except:
        get_image.logger.info('MySQL ERROR: %s in id: %s' % (sys.exc_info()[0], news_id))


def insert_replace_img(cur, news_id, url_image):
    query = u'INSERT INTO {0} ({4}, {5}, {6}) VALUES ({1},"{2}", {3}) ON DUPLICATE KEY UPDATE {5} = "{2}", {6} = {3};'.format \
        (i_config.IMG_TABLE, news_id, url_image[1], url_image[0], i_config.IMG_ID, i_config.IMG_URL, i_config.IMG_STATE)
    if url_image[1] is None:
        query = u'INSERT INTO {0} ({3}, {4}, {5}) VALUES ({1},null, {2}) ON DUPLICATE KEY UPDATE {4} = null, {5} = {2};'.format \
            (i_config.IMG_TABLE, news_id, url_image[0], i_config.IMG_ID, i_config.IMG_URL, i_config.IMG_STATE)
    cur.execute(query)


def process(day1, day2):
    events = event_dao.get_event_in_range(day1, day2)
    for event in events:
        get_image.logger.info('process event: %s,\tday: %s' % (event[resources.EVENT_ID], event[resources.EVENT_DATE]))
        sids = util.str_to_int_array(util.convert_dic_article_to_list(event[resources.EVENT_ARTICCLE_IDS]), ',')
        ids = get_ids_not_exist(sids)
        if len(ids) == 0:
            continue
        imgs = get_image.get_img_with_ids(ids)
        conn = dao.get_connection()
        cur = dao.get_cursor(conn)
        for i in ids:
            insert_img(cur, i, imgs[i])
        conn.commit()
        dao.free_connection(conn, cur)


def process_reget(day1, day2, only_null=True):
    events = event_dao.get_event_in_range(day1, day2)
    for event in events:
        get_image.logger.info('process event: %s,\tday: %s' % (event[resources.EVENT_ID], event[resources.EVENT_DATE]))
        sids = util.str_to_int_array(util.convert_dic_article_to_list(event[resources.EVENT_ARTICCLE_IDS]), ',')
        if only_null:
            ids = get_ids_img_null(sids)
        else:
            ids = sids
        if len(ids) == 0:
            continue
        imgs = get_image.get_img_with_ids(ids)
        conn = dao.get_connection()
        cur = dao.get_cursor(conn)
        for i in ids:
            insert_replace_img(cur, i, imgs[i])
        conn.commit()
        dao.free_connection(conn, cur)


"""
usage:
python run_find_img.py real             chay realtime
python run_find_img.py next             chay next date
python run_find_img.py date             chay moi voi day date
python run_find_img.py date1 date2      chay moi tu date1 toi date2
python run_find_img.py date1 date2 -r   chay lai voi nhung anh null tu date1 toi date2
python run_find_img.py date1 date2 -a   chay lai tat ca tu date1 toi date2
"""


def main():
    if (len(sys.argv) < 2):
        print 'usage: python run_find_img.py date/real/max'
        return
    date1 = sys.argv[1]
    if date1 == 'real':
        print "Run realtime"
        process_real_time()
    elif date1 == 'next':
        max_date = event_dao.get_max_date(i_config.EVENT_TABLE)
        print 'process max day: %s' % max_date
        process(max_date, max_date)
    # elif (date1 == 'next'):
    #     current_day = event_dao.get_max_date(resources.EVENT_TABLE)
    #     next_day = util.get_ahead_day(current_day, 1)
    #     print 'process next day: %s' % next_day
    #     process(next_day, next_day)
    else:
        if len(sys.argv) == 2:
            print 'process date: %s' % (date1)
            process(date1, date1)
        else:
            if len(sys.argv) == 3:
                print 'process date: %s %s' % (date1, sys.argv[2])
                process(date1, sys.argv[2])
                return
            else:
                if sys.argv[3] == '-r':
                    print 'process reget image: %s %s' % (date1, sys.argv[2])
                    process_reget(date1, sys.argv[2])
                    return
                if sys.argv[3] == '-a':
                    print 'process reget all image: %s %s' % (date1, sys.argv[2])
                    process_reget(date1, sys.argv[2], False)
                    return


if __name__ == '__main__':
    process_real_time()
    # get_image.logger.info('\n====================== News Session ======================\n')
    # start = datetime.datetime.now()
    # main()
    # # re_get_docs_nullimg()
    # # process_real_time()
    # #    import datetime
    # #    start = datetime.datetime.now()
    # #    day1 = '2015-12-01'
    # #    day2 = '2015-12-04'
    # #    process(day1, day2)
    # #    process_reget(day1, day2)
    # #    print datetime.datetime.now() - start
    # get_image.logger.info('\nFinish process: runtime: %s\n' % (datetime.datetime.now() - start))
