# -*- coding: utf-8 -*-
"""
Created on Fri Dec 11 14:43:20 2015

@author: bolx
"""

import sys

import pymysql

import i_util

# sys.path.insert(0, '../')
import resources, dao


def get_all_trend():
    conn = dao.get_connection()
    cur = conn.cursor(pymysql.cursors.DictCursor)
    query = 'SELECT * FROM %s' % resources.TREND_TABLE
    cur.execute(query)
    rows = cur.fetchall()
    dao.free_connection(conn, cur)
    return rows


def get_trends_by_last_date(last_date):
    query = 'SELECT * FROM %s WHERE (%s = \'%s\')' % (resources.TREND_TABLE, resources.TREND_LAST_DATE, last_date)
    # print 'query = ', query
    conn = dao.get_connection()
    cur = conn.cursor(pymysql.cursors.DictCursor)
    cur.execute(query)
    rows = cur.fetchall()
    dao.free_connection(conn, cur)
    return rows


def get_all_trend_event_ids():
    rows = get_all_trend()
    trend_event_ids = dict()
    for row in rows:
        trend_id = row[resources.TREND_ID]
        event_ids_str = row[resources.TREND_EVENT_IDS]
        event_ids = [int(i) for i in event_ids_str.split(',') if len(i) > 0]
        trend_event_ids[trend_id] = event_ids
    return trend_event_ids


def get_event(event_ids=None, date=None):
    # query = 'SELECT * FROM %s '
    if event_ids is None:
        if date is None:
            query = 'SELECT * FROM %s' % resources.EVENT_TABLE
        else:
            query = 'SELECT * FROM %s WHERE %s = %s' % (resources.EVENT_TABLE, resources.EVENT_DATE, date)
    else:
        if date is None:
            query = 'SELECT * FROM %s WHERE %s in (%s)' % (
                resources.EVENT_TABLE, resources.EVENT_ID, i_util.ids_to_str(event_ids))
        else:
            query = 'SELECT * FROM %s WHERE %s in (%s) AND %s = %s' % (
                resources.EVENT_TABLE, resources.EVENT_ID, i_util.ids_to_str(event_ids), resources.EVENT_DATE, date)
    conn = dao.get_connection()
    cur = conn.cursor(pymysql.cursors.DictCursor)
    cur.execute(query)
    rows = cur.fetchall()
    dao.free_connection(conn, cur)
    return rows


def get_tag_date_events_between_days(event_ids, nday):
    query = 'SELECT %s, %s, %s FROM %s WHERE %s in (%s)' % (resources.EVENT_TAG, resources.EVENT_DATE,
                                                            resources.EVENT_SIZE, resources.EVENT_TABLE,
                                                            resources.EVENT_ID, i_util.ids_to_str(event_ids))
    conn = dao.get_connection()
    cur = conn.cursor(pymysql.cursors.DictCursor)
    cur.execute(query)
    rows = cur.fetchall()
    rows.reverse()
    dao.free_connection(conn, cur)
    days = sorted(set([row[resources.EVENT_DATE].strftime(i_util.DATE_FORMAT) for row in rows]), reverse=True)
    if len(days) > nday:
        days = days[:nday]
    tags = []
    dates = []
    sizes = []
    for row in rows:
        day = row[resources.EVENT_DATE].strftime(i_util.DATE_FORMAT)
        size = row[resources.EVENT_SIZE]
        if day not in days:
            continue
        if row[resources.EVENT_TAG] is None:
            tag = []
        else:
            tag = row[resources.EVENT_TAG].split(',')
            tag = [t.strip() for t in tag]
        tags.append(tag)
        dates.append(day)
        sizes.append(size)
    return tags, dates, sizes


def get_img_event(event_id):
    query = 'SELECT %s FROM %s WHERE %s = %s' % (
    resources.EVENT_IMG2, resources.EVENT_TABLE, resources.EVENT_ID, event_id)
    conn = dao.get_connection()
    cur = conn.cursor(pymysql.cursors.DictCursor)
    cur.execute(query)
    row = cur.fetchone()
    return row[resources.EVENT_IMG2]


def get_event_ids_date_end_trends():
    rows = get_all_trend()
    trend_date_events = dict()
    for row in rows:
        trend_id = row[resources.TREND_ID]
        event_ids_str = row[resources.TREND_EVENT_IDS]
        event_ids = [int(i) for i in event_ids_str.split(',') if len(i) > 0]
        end_date = row[resources.TREND_LAST_DATE].strftime(i_util.DATE_FORMAT)
        trend_date_events[trend_id] = (event_ids, end_date)
    return trend_date_events


""" Lay cac event id va date cua cac tren co last date = date_end """


def get_event_ids_trends_by_date_end(date_end):
    rows = get_trends_by_last_date(date_end)
    #  print rows
    trend_date_events = dict()
    for row in rows:
        trend_id = row[resources.TREND_ID]
        event_ids_str = row[resources.TREND_EVENT_IDS]
        event_ids = [int(i) for i in event_ids_str.split(',') if len(i) > 0]
        end_date = row[resources.TREND_LAST_DATE].strftime(i_util.DATE_FORMAT)
        trend_date_events[trend_id] = (event_ids, end_date)
    return trend_date_events


def fill_tags_trend(trend_id, tags):
    tag = i_util.ids_to_str(tags).replace('\'', '\\\'')
    query = 'UPDATE %s SET %s=\'%s\' WHERE %s = %s' % (
        resources.TREND_TABLE, resources.TREND_TAG, tag, resources.TREND_ID, trend_id)
    # print query
    # print 'Update tag = \'%s\' at trend %s'%(n_util.ids_to_str(tags), trend_id)
    conn = dao.get_connection()
    cur = dao.get_cursor(conn)
    cur.execute(query)
    conn.commit()
    dao.free_connection(conn, cur)
    pass


def fill_image_event(event_id, img_url):
    tag = img_url.replace('\'', '\\\'')
    query = 'UPDATE %s SET %s=\'%s\' WHERE %s = %s' % (
        resources.EVENT_TABLE, resources.EVENT_IMG2, tag, resources.EVENT_ID, event_id)
    # print query
    print 'Update img = \'%s\' at event %s' % (tag, event_id)
    conn = dao.get_connection()
    cur = dao.get_cursor(conn)
    cur.execute(query)
    conn.commit()
    dao.free_connection(conn, cur)


def fill_image_trend(trend_id, img_url):
    tag = img_url.replace('\'', '\\\'')
    query = 'UPDATE %s SET %s=\'%s\' WHERE %s = %s' % (
        resources.TREND_TABLE, resources.TREND_IMG, tag, resources.TREND_ID, trend_id)
    print 'Update img = \'%s\' at trend %s' % (tag, trend_id)
    conn = dao.get_connection()
    cur = dao.get_cursor(conn)
    cur.execute(query)
    conn.commit()
    dao.free_connection(conn, cur)

# print get_event_ids_date_end_trend(10)
