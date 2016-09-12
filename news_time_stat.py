TABLE_NAME = 'news_time_stat'
ID = 'id'
GET_TIME =  'get_time'
PROCESS_TIME = 'process_time'
import dao,util
import pymysql
def insert_record(process_time_dic):
    aids = process_time_dic.keys()
    time_dic = get_get_time_of_articles(aids)
    conn = dao.get_connection()
    cur = conn.cursor()
    for aid in aids:
        query = 'INSERT IGNORE INTO %s(%s,%s,%s) VALUES (%d,\'%s\',\'%s\')'%(TABLE_NAME,ID,GET_TIME,PROCESS_TIME,aid,time_dic[aid],process_time_dic[aid])
        print query
        cur.execute(query)
    conn.commit()
    dao.free_connection(conn,cur)

def get_get_time_of_articles(aids):
    wstr = util.get_in_where_str(aids)
    query = 'SELECT %s,%s FROM %s WHERE %s in (%s)'%(dao.ID,dao.GET_TIME,dao.NEWS_TABLE,dao.ID,wstr)
    conn = dao.get_connection()
    cur = conn.cursor(pymysql.cursors.DictCursor)
    cur.execute(query)
    rows = cur.fetchall()
    result = dict()
    for row in rows:
        result[row[dao.ID]] = str(row[dao.GET_TIME])
    return result
    dao.free_connection(conn,cur)

import datetime
def test_insert():
    aids = [62393784, 62393745, 62393658, 62393699]
    process_time_dic = dict()
    for aid in aids:
        process_time_dic[aid] = str(datetime.datetime.now())
    insert_record(process_time_dic)

#test_insert()
        