import sub_resources,dao,util,pymysql,logging,resources
TABLE_NAME = 'new_sub_trend2'
EVENT_ID = 'event_id'
GROUP_ID = 'group_id'
logger = logging.getLogger("h_sub_util.py")
logger.setLevel(logging.INFO)
util.set_log_file(logger,resources.LOG_FOLDER+'/h_sub_util.log','w')
def create_insert_query(sub_event,table = TABLE_NAME):
    #sub_event[sub_resources.CENTER] = 'center'
    #sub_event[sub_resources.EN_CENTER] = 'encenter'
    #preprocess_sub_event(sub_event)
    article_dic_str = str(sub_event[sub_resources.ARTICLES])
    article_dic_str = article_dic_str.replace('\'','\\\'')
    query = 'replace into %s(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) values (%d,\'%s\',\'%s\',\'%s\',%d,\'%s\',%d,%f,%d,%d)'\
    %(table,\
    sub_resources.ID,sub_resources.ARTICLES,\
    sub_resources.LAST_DATE,sub_resources.START_DATE,\
    sub_resources.TOTAL_ARTICLE_NUM,sub_resources.TAG,sub_resources.TREND_ID,sub_resources.COHERENCE,sub_resources.TYPICAL_ART,EVENT_ID,\
    sub_event[sub_resources.ID],article_dic_str,\
    sub_event[sub_resources.LAST_DATE],sub_event[sub_resources.START_DATE],\
    sub_event[sub_resources.TOTAL_ARTICLE_NUM],sub_event[sub_resources.TAG],sub_event[sub_resources.TREND_ID],\
    sub_event[sub_resources.COHERENCE],sub_event[sub_resources.TYPICAL_ART],sub_event[EVENT_ID])
    return query

def create_insert_query_wo_event_id(sub_event,table = TABLE_NAME):
    #sub_event[sub_resources.CENTER] = 'center'
    #sub_event[sub_resources.EN_CENTER] = 'encenter'
    #preprocess_sub_event(sub_event)
    article_dic_str = str(sub_event[sub_resources.ARTICLES])
    article_dic_str = article_dic_str.replace('\'','\\\'')
    query = 'replace into %s(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) values (%d,\'%s\',\'%s\',\'%s\',%d,\'%s\',%d,%f,%d,%d)'\
    %(table,\
    sub_resources.ID,sub_resources.ARTICLES,\
    sub_resources.LAST_DATE,sub_resources.START_DATE,\
    sub_resources.TOTAL_ARTICLE_NUM,sub_resources.TAG,sub_resources.TREND_ID,sub_resources.COHERENCE,sub_resources.TYPICAL_ART,GROUP_ID,\
    sub_event[sub_resources.ID],article_dic_str,\
    sub_event[sub_resources.LAST_DATE],sub_event[sub_resources.START_DATE],\
    sub_event[sub_resources.TOTAL_ARTICLE_NUM],sub_event[sub_resources.TAG],sub_event[sub_resources.TREND_ID],\
    sub_event[sub_resources.COHERENCE],sub_event[sub_resources.TYPICAL_ART],sub_event[GROUP_ID])
    return query

def insert_sub_event_wo_event_id(sub_events,table):
    conn = dao.get_connection()
    cur = conn.cursor()
    for sub_event in sub_events:
        query = create_insert_query_wo_event_id(sub_event,table)
        logger.info(query)
        cur.execute(query)
    conn.commit()
    dao.free_connection(conn,cur)

def insert_sub_event(sub_events,table = TABLE_NAME):
    conn = dao.get_connection()
    cur = conn.cursor()
    for sub_event in sub_events:
        query = create_insert_query(sub_event)
        print query
        cur.execute(query)
    conn.commit()
    dao.free_connection(conn,cur)

def delete_sub_with_ids(ids,table=TABLE_NAME):
    conn = dao.get_connection()
    cur = conn.cursor()
    wstr = util.get_in_where_str(ids)
    query = 'DELETE FROM %s WHERE %s in (%s)'%(table,sub_resources.ID,wstr)
    cur.execute(query)
    conn.commit()
    dao.free_connection(conn,cur)

def delete_sub_of_event(event_id):
    conn = dao.get_connection()
    cur = conn.cursor()
    query = 'DELETE FROM %s WHERE %s = %d'%(TABLE_NAME,EVENT_ID,event_id)
    print query
    cur.execute(query)
    conn.commit()
    dao.free_connection(conn,cur)

def update_sub_event(sub_events,table):
    remove_ids = []
    for sub in sub_events:
        remove_ids.append(sub[sub_resources.ID])
    delete_sub_with_ids(remove_ids,table)
    insert_sub_event(sub_events,table)

def remove_real_trend_today(table):
    query = 'delete from %s where %s > 5000000'%(table,sub_resources.TREND_ID)
    conn = dao.get_connection()
    cur = conn.cursor()
    cur.execute(query)
    conn.commit()
    dao.free_connection(conn,cur)
    

def get_sub_util_by_ids(sub_ids,table = TABLE_NAME):
    in_wstr = util.get_in_where_str(sub_ids)
    query = 'select * from %s where (%s in (%s))'%(table,sub_resources.ID,in_wstr)
    conn = dao.get_connection()
    cur = conn.cursor(pymysql.cursors.DictCursor)
    cur.execute(query)
    rows = cur.fetchall()
    dao.free_connection(conn,cur)
    return rows

def get_max_group_id(trend_id,table):
    query = 'select max(%s) from %s where (%s = %d)'%(GROUP_ID,table,sub_resources.TREND_ID,trend_id)
    print query
    conn = dao.get_connection()
    cur = conn.cursor()
    cur.execute(query)
    max_gid_row = cur.fetchone()
    max_gid = 0
    if max_gid_row[0]:
        max_gid = int(max_gid_row[0])
    dao.free_connection(conn,cur)
    return max_gid

def get_sub_records(trend_id,date1,date2,table):
    query = 'select * from %s where (%s = %d and %s >= \'%s 00:00:00\' and %s <= \'%s 23:59:59\') order by %s desc'%(table,sub_resources.TREND_ID,trend_id,\
    sub_resources.START_DATE,date1,sub_resources.START_DATE,date2,sub_resources.START_DATE)
    print query
    conn = dao.get_connection()
    cur = conn.cursor(pymysql.cursors.DictCursor)
    cur.execute(query)
    rows = cur.fetchall()
    dao.free_connection(conn,cur)
    return rows

def get_sub_records_all_trend_by_last_date(date1,date2,table):
    query = 'select * from %s where (%s >= \'%s 00:00:00\' and %s <= \'%s 23:59:59\') order by %s desc'%(table,\
    sub_resources.LAST_DATE,date1,sub_resources.LAST_DATE,date2,sub_resources.LAST_DATE)
    print query
    conn = dao.get_connection()
    cur = conn.cursor(pymysql.cursors.DictCursor)
    cur.execute(query)
    rows = cur.fetchall()
    dao.free_connection(conn,cur)
    return rows

def get_max_date(trend_id,table):
    query = 'select max(%s) from %s where %s = %d '%(sub_resources.LAST_DATE,table,sub_resources.TREND_ID,trend_id)
    print query
    conn = dao.get_connection()
    cur = conn.cursor()
    cur.execute(query)
    row = cur.fetchone()
    dao.free_connection(conn,cur)
    return row[0]

def get_max_before_date(trend_id,table,max_date):
    query = 'select max(%s) from %s where %s = %d and %s <= \'%s 23:59:59\' '%(sub_resources.LAST_DATE,table,\
    sub_resources.TREND_ID,trend_id,sub_resources.LAST_DATE,max_date)
    print query
    conn = dao.get_connection()
    cur = conn.cursor()
    cur.execute(query)
    row = cur.fetchone()
    dao.free_connection(conn,cur)
    return row[0]

def get_sub_from_today(table,trend_id,date):
    #today = util.get_today_str()
    low_datetime = date+' 00:00:00'
    high_datetime = date + ' 23:59:59'
    query = 'select * from %s where %s <= \'%s\' and %s >=\'%s\' and %s = %d'%(table,sub_resources.LAST_DATE,\
    high_datetime,sub_resources.LAST_DATE,low_datetime,sub_resources.TREND_ID,trend_id)
    print query
    conn = dao.get_connection()
    cur = conn.cursor(pymysql.cursors.DictCursor)
    cur.execute(query)
    rows = cur.fetchall()
    dao.free_connection(conn,cur)
    return rows

def get_all_sub_from_today(table,date):
    #today = util.get_today_str()
    low_datetime = date+' 00:00:00'
    high_datetime = date + ' 23:59:59'
    query = 'select * from %s where %s <= \'%s\' and %s >=\'%s\''%(table,sub_resources.LAST_DATE,\
    high_datetime,sub_resources.LAST_DATE,low_datetime)
    print query
    conn = dao.get_connection()
    cur = conn.cursor(pymysql.cursors.DictCursor)
    cur.execute(query)
    rows = cur.fetchall()
    dao.free_connection(conn,cur)
    return rows

def get_sub_id_group_id(table,sub_ids):
    wstr = util.get_in_where_str(sub_ids)
    query = 'select * from %s where %s in (%s)'%(table,sub_resources.ID,wstr)
    conn = dao.get_connection()
    cur = conn.cursor(pymysql.cursors.DictCursor)
    cur.execute(query)
    rows = cur.fetchall()
    dao.free_connection(conn,cur)
    result = dict()
    for row in rows:
        result[int(row[sub_resources.ID])] = int(row[GROUP_ID])
    return result

#get_sub_from_today('hier_sub_v3',123)
#result = get_sub_id_group_id('hier_sub_v2',[73385600])
#result = get_max_group_id(17327,'hier_sub_v2')
#print result