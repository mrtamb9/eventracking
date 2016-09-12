import dao
import pymysql
import resources
import util
import logging,news_img
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
#util.set_log_file(logger,resources.LOG_FOLDER+'/log_sql.log')

def create_insert_new_trend_query(trend,trend_table= resources.TREND_TABLE):
    query = 'INSERT INTO %s(%s,%s,%s,%s,%s,%s) VALUES(\'%s\',\'%s\',\'%s\',%d,%d,%d)'%(trend_table,\
    resources.TREND_START_DATE,resources.TREND_LAST_DATE,resources.TREND_EVENT_IDS,resources.TREND_SIZE,resources.TREND_EVENT_NUM,resources.TREND_CATID,\
    trend[resources.TREND_START_DATE],trend[resources.TREND_LAST_DATE],trend[resources.TREND_EVENT_IDS],trend[resources.TREND_SIZE],\
    trend[resources.TREND_EVENT_NUM],trend[resources.TREND_CATID])
    return query
def insert_new_trend(trend,trend_table=resources.TREND_TABLE):
    conn = dao.get_connection()
    cur = conn.cursor()
    query = create_insert_new_trend_query(trend,trend_table)
    cur.execute(query)
    conn.commit()
    trend_id = cur.lastrowid
    dao.free_connection(conn,cur)    
    return trend_id

def get_min_datetime(aids):
    wstr = get_in_where_str(aids)
    query = 'select min(%s) from %s where %s in (%s)'%(dao.CREATE_TIME,dao.NEWS_TABLE,dao.ID,wstr)
    #print query
    conn = dao.get_connection()
    cur = conn.cursor()
    cur.execute(query)
    row = cur.fetchone()
    dao.free_connection(conn,cur)
    return row[0]

def get_max_datetime(aids):
    wstr = get_in_where_str(aids)
    query = 'select max(%s) from %s where %s in (%s)'%(dao.CREATE_TIME,dao.NEWS_TABLE,dao.ID,wstr)
    #print query
    conn = dao.get_connection()
    cur = conn.cursor()
    cur.execute(query)
    row = cur.fetchone()
    dao.free_connection(conn,cur)
    return row[0]

def fetch_all_events(day,table=resources.EVENT_TABLE):
    conn = dao.get_connection()
    cur = conn.cursor(pymysql.cursors.DictCursor)
    query = create_query_all_events(day,table)
    logger.info('execute: %s'%query)
    cur.execute(query)
    rows = cur.fetchall()
    dao.free_connection(conn,cur)
    for row in rows:
        center_str = row[resources.EVENT_CENTER]
        center = util.str_to_dict(center_str)
        row[resources.EVENT_CENTER] = center
        row[resources.EVENT_DATE] = str(row[resources.EVENT_DATE])
    return rows

def create_query_all_events(day,table=resources.EVENT_TABLE):
    res = 'SELECT %s,%s,%s,%s,%s FROM %s WHERE (%s=\'%s\')'%(resources.EVENT_DATE,\
    resources.EVENT_CENTER,resources.EVENT_SIZE,resources.EVENT_ID,resources.EVENT_CATID,\
    table,resources.EVENT_DATE,day)
    return res

def create_query_all_trends(day,interval,table= resources.TREND_TABLE):
    old_day = util.get_past_day(day,interval)
    res = 'SELECT * FROM %s WHERE (%s <= \'%s\' ) AND (%s > \'%s\')'%(\
    table, resources.TREND_LAST_DATE,day,resources.TREND_LAST_DATE,old_day)
    return res

def fetch_trend_by_id(trend_id,table=resources.TREND_TABLE ):
    query = 'SELECT * FROM %s WHERE (%s = %d)'%(table,resources.TREND_ID,trend_id)
    conn = dao.get_connection()
    cur = conn.cursor(pymysql.cursors.DictCursor)
    logger.info('execute: %s'%query)
    cur.execute(query)
    row = cur.fetchone()
    dao.free_connection(conn,cur)
    return row

def get_article_urls(aids):
    wstr = get_in_where_str(aids)
    query = 'SELECT * FROM %s WHERE (%s in (%s)) order by %s desc'%(dao.NEWS_TABLE,dao.ID,wstr,dao.CREATE_TIME)
    conn = dao.get_connection()
    cur = conn.cursor(pymysql.cursors.DictCursor)
    logger.info('execute: %s'%query)
    cur.execute(query)
    rows = cur.fetchall()
    dao.free_connection(conn,cur)
    news_imgs = get_image_urls(aids)
    url_map = get_img_map_id_2_imgurl(news_imgs)
    for row in rows:
        row_id = row[dao.ID]
        if (row_id in url_map):
            row[news_img.IMG_URL] = url_map[row_id]
        else:
            row[news_img.IMG_URL] = None
    return rows

def get_img_map_id_2_imgurl(rows):
    result = dict()
    for row in rows:
        aid = row[news_img.NEWS_ID]
        url = row[news_img.IMG_URL]
        result[aid]= url
    return result

def get_in_where_str(id_list):
    res = ''
    leng = len(id_list)
    for i in range(leng-1):
        res += '%d,'%id_list[i]
    res += '%d'%id_list[leng-1]
    return res
def get_trends_by_ids(trend_ids):
    wherestr = get_in_where_str(trend_ids)
    query = 'select * from %s where %s in (%s)'%(resources.TREND_TABLE,resources.TREND_ID,wherestr)
    conn = dao.get_connection()
    cur = conn.cursor(pymysql.cursors.DictCursor)
    cur.execute(query)
    rows = cur.fetchall()
    dao.free_connection(conn,cur)
    return rows

def get_event_by_id(event_id,table = resources.EVENT_TABLE):
    query = 'SELECT * FROM %s WHERE (%s=%d)'%(table,resources.EVENT_ID,event_id)
    conn = dao.get_connection()
    cur = conn.cursor(pymysql.cursors.DictCursor)
    logger.info('execute: %s'%query)
    cur.execute(query)
    row = cur.fetchone()
    dao.free_connection(conn,cur)
    return row
def get_event_in_range(day1,day2,table = resources.EVENT_TABLE,catid = -1):
    if (catid == -1):
        query = 'SELECT * FROM %s WHERE (%s >=\'%s\') and (%s <= \'%s\')'%(table,resources.EVENT_DATE,\
        day1,resources.EVENT_DATE,day2)
    else:
        query = 'SELECT * FROM %s WHERE (%s >=\'%s\') and (%s <= \'%s\') and (%s = %d)'%(table,resources.EVENT_DATE,\
        day1,resources.EVENT_DATE,day2,\
        resources.EVENT_CATID,catid)
    conn = dao.get_connection()
    cur = conn.cursor(pymysql.cursors.DictCursor)
    print query
    logger.info('execute: %s'%query)
    cur.execute(query)
    rows = cur.fetchall()
    dao.free_connection(conn,cur)
    return rows

def fetch_trends(start_day, trend_length):
    query = 'SELECT * FROM %s WHERE (%s <= \'%s\') and (%s > %d) order by %s desc'%(resources.TREND_TABLE,resources.TREND_START_DATE,start_day,\
    resources.TREND_EVENT_NUM,trend_length,resources.TREND_LAST_DATE)
    conn = dao.get_connection()
    cur = conn.cursor(pymysql.cursors.DictCursor)
    cur.execute(query)
    rows = cur.fetchall()
    dao.free_connection(conn,cur)
    return rows

def get_articles_by_list_ids(art_ids,domain):
    idstr = ''
    for i in range(len(art_ids)-1):
        aid = art_ids[i]
        idstr += '%d,'%aid
    aid = art_ids[len(art_ids)-1]
    idstr += '%d'%aid
    query = 'SELECT * FROM %s where (%s in (%s)) and (%s = \'%s\') order by %s'%(\
    dao.NEWS_TABLE,dao.ID,idstr,dao.DOMAIN,domain,dao.CREATE_TIME)
    if (domain == 'all'):
        query = 'SELECT * FROM %s where (%s in (%s)) order by %s'%(\
    dao.NEWS_TABLE,dao.ID,idstr,dao.CREATE_TIME)
    conn = dao.get_connection()
    cur = conn.cursor(pymysql.cursors.DictCursor)
    cur.execute(query)
    rows = cur.fetchall()
    dao.free_connection(conn,cur)
    return rows

def fetch_all_trends(day,interval,table= resources.TREND_TABLE):       
    conn = dao.get_connection()
    cur = conn.cursor(pymysql.cursors.DictCursor)
    query = create_query_all_trends(day,interval,table)
    print query
    logger.info('execute: %s'%query)
    cur.execute(query)
    rows = cur.fetchall()
    dao.free_connection(conn,cur)
    for row in rows:
        date_str = str(row[resources.TREND_LAST_DATE])
        row[resources.TREND_LAST_DATE] = date_str
    return rows

def update_trend(trend,table=resources.TREND_TABLE):
    query = 'UPDATE %s SET %s=\'%s\',%s=\'%s\',%s=%d, %s=%d, %s=%d WHERE (%s = %d)'%(table,\
    resources.TREND_LAST_DATE,trend[resources.TREND_LAST_DATE],\
    resources.TREND_EVENT_IDS,trend[resources.TREND_EVENT_IDS],\
    resources.TREND_SIZE,trend[resources.TREND_SIZE],\
    resources.TREND_EVENT_NUM,trend[resources.TREND_EVENT_NUM],\
    resources.TREND_CATID,trend[resources.TREND_CATID],\
    resources.TREND_ID,trend[resources.TREND_ID])
    print query
    conn = dao.get_connection()
    cur = conn.cursor()
    cur.execute(query)
    conn.commit()
    dao.free_connection(conn,cur)
def get_event_by_id2(event_id):
    """
    each row is a dictionary with key as name of column
    """
    query = 'SELECT * from %s where %s = %d'%(resources.EVENT_TABLE,resources.EVENT_ID,event_id)
    conn = dao.get_connection()
    cur = conn.cursor(pymysql.cursors.DictCursor)
    cur.execute(query)
    row = cur.fetchone()
    return row
def get_all_trend_id(threshold=resources.VISUAL_THRESHOLD_TREND):
    query = 'SELECT %s FROM %s where %s >= %d'%(resources.TREND_ID,\
    resources.TREND_TABLE,resources.TREND_EVENT_NUM,threshold)
    conn = dao.get_connection()
    cur = conn.cursor()
    cur.execute(query)
    rows = cur.fetchall()
    trend_list = []
    for row in rows:
        trend_list.append(int(row[0]))
    dao.free_connection(conn,cur)
    return trend_list
def remove_events(day,delete_sign='='):
    query = 'delete from %s where (%s %s \'%s\')'%(resources.EVENT_TABLE,resources.EVENT_DATE,delete_sign,day)
    conn = dao.get_connection()
    cur = conn.cursor()
    cur.execute(query)
    conn.commit()
    dao.free_connection(conn,cur)
    logger.info('execute: %s'%query)
def remove_trend(day):
    query = 'delete from %s where (%s >= \'%s\')'%(resources.TREND_TABLE,resources.TREND_LAST_DATE,day)
    conn = dao.get_connection()
    cur = conn.cursor()
    cur.execute(query)
    conn.commit()
    dao.free_connection(conn,cur)
    logger.info('execute: %s'%query)
def delete_all_trend():
    query = 'delete from %s'%resources.TREND_TABLE
    conn = dao.get_connection()
    cur = conn.cursor()
    cur.execute(query)
    conn.commit()
    dao.free_connection(conn,cur)
    logger.info('execute: %s'%query)

def delete_all_events():
    query = 'delete from %s'%resources.EVENT_TABLE
    conn = dao.get_connection()
    cur = conn.cursor()
    cur.execute(query)
    conn.commit()
    dao.free_connection(conn,cur)
    logger.info('execute: %s'%query)

def delete_all_record_in_table(table):
    query  = 'delete from %s'%table
    conn = dao.get_connection()
    cur = conn.cursor()
    cur.execute(query)
    conn.commit()
    dao.free_connection(conn,cur)
    print 'complete deleting table: %s'%table

def delete_records_with_ids(ids,table):
    wstr = util.get_in_where_str(ids)
    query = 'delete from %s where id in (%s)'%(table,wstr)
    conn = dao.get_connection()
    cur = conn.cursor()
    cur.execute(query)
    conn.commit()
    dao.free_connection(conn,cur)
    print 'complete deleting records with ids: %s table%s'%(wstr,table)

def get_event_trend_id_pair(table):
    query = 'select %s,%s from %s'%(resources.EVENT_ID,resources.EVENT_TREND_ID,table)
    conn = dao.get_connection()
    cur = conn.cursor(pymysql.cursors.DictCursor)
    cur.execute(query)
    rows = cur.fetchall()
    dao.free_connection(conn,cur)
    result = dict()
    for row in rows:
        result[row[resources.EVENT_ID]] = row[resources.TREND_ID]
    return result

def save_message(mess,date,event_id,trend_id):
    query = 'insert into %s(%s,%s,%s,%s) values(\'%s\',\'%s\',%d,%d)'%\
    (resources.FEEDBACK_TABLE,resources.FEEDBACK_MESSAGE,resources.FEEDBACK_DATE,resources.FEEDBACK_EVENT_ID,resources.FEEDBACK_TREND_ID,\
    mess,date,event_id,trend_id)
    conn = dao.get_connection()
    cur = conn.cursor()
    cur.execute(query)
    conn.commit()
    dao.free_connection(conn,cur)
def get_event_by_ids(ids,table = resources.EVENT_TABLE):
    idstr = ''
    leng = len(ids)
    for index in range(leng -1):
        idstr += '%d,'%ids[index]
    idstr += '%d'%ids[leng-1]
    query = 'SELECT * FROM %s WHERE %s in (%s)'%(table,resources.EVENT_ID,idstr)
    conn = dao.get_connection()
    cur = conn.cursor(pymysql.cursors.DictCursor)
    cur.execute(query)
    rows = cur.fetchall()
    dao.free_connection(conn,cur)
    return rows
def get_trend_between_day(day1,day2,leng_threshold,size_threshold=20,catid=-1):
    print 'catid = %d'%catid
    if (catid == -1):
        query = 'SELECT * FROM %s WHERE (not (%s < \'%s\' or %s > \'%s\')) and (%s >= %d) and (%s > %d)'%(resources.TREND_TABLE,\
        resources.TREND_LAST_DATE,day1,\
        resources.TREND_START_DATE,day2,\
        resources.TREND_EVENT_NUM,leng_threshold,resources.TREND_SIZE,size_threshold)
    else:
        query = 'SELECT * FROM %s WHERE (not (%s < \'%s\' or %s > \'%s\')) and (%s >= %d) and (%s > %d) and (%s = %d)'%(resources.TREND_TABLE,\
        resources.TREND_LAST_DATE,day1,\
        resources.TREND_START_DATE,day2,\
        resources.TREND_EVENT_NUM,leng_threshold,resources.TREND_SIZE,size_threshold,\
        resources.TREND_CATID,catid)
    conn = dao.get_connection()
    cur = conn.cursor(pymysql.cursors.DictCursor)
    cur.execute(query)
    rows = cur.fetchall()
    dao.free_connection(conn,cur)
    return rows
    
def create_insert_events_query(event_id,date,article_ids,center,size,img_path,coherence,catid,table_name = resources.EVENT_TABLE):
    query = 'INSERT INTO %s(%s,%s,%s,%s,%s,%s,%s,%s)\
    VALUES(\'%s\',\'%s\',\'%s\',%d,%d,\'%s\',%f,%d)'%\
    (table_name,\
    resources.EVENT_DATE,resources.EVENT_CENTER,resources.EVENT_ARTICCLE_IDS,resources.EVENT_SIZE,\
    resources.EVENT_ID,resources.EVENT_TOPWORD_IMG,resources.EVENT_COHERENCE,resources.EVENT_CATID
    ,date,center,article_ids,size,event_id,img_path,coherence,int(catid))
    return query

def get_trend_ids_on_day(day,trend_table= resources.TREND_TABLE):
    query = 'select %s from %s where %s = \'%s\''%(resources.TREND_ID,trend_table,resources.TREND_LAST_DATE,day)
    conn = dao.get_connection()
    cur = conn.cursor()
    cur.execute(query)
    rows = cur.fetchall()
    dao.free_connection(conn,cur)
    trend_ids = []
    for row in rows:
        trend_ids.append(int(row[0]))
    return trend_ids
    
def get_all_categories():
    query = 'select * from news_category'
    conn = dao.get_connection()
    cur = conn.cursor()
    cur.execute(query)
    rows = cur.fetchall()
    dao.free_connection(conn,cur)
    cat_map = dict()
    for row in rows:
        cat_map[row[0]] = row[1]
    return cat_map

def get_image_urls(aids):
    wstr = get_in_where_str(aids)
    query = 'select * from %s where %s in (%s)'%(news_img.TABLE_NAME,news_img.NEWS_ID,wstr)
    conn = dao.get_connection()
    cur = conn.cursor(pymysql.cursors.DictCursor)
    cur.execute(query)
    rows = cur.fetchall()
    dao.free_connection(conn,cur)
    return rows
    
def get_article_url_infor(row):
    temp= dict()
    temp[dao.URL] = row[dao.URL]
    temp[dao.TITLE] = row[dao.TITLE]
    temp[dao.ID] = row[dao.ID]
    if (news_img.IMG_URL in row):
        temp[news_img.IMG_URL] = row[news_img.IMG_URL]
    temp[dao.DESCRIPTION] = row[dao.DESCRIPTION]
    temp[dao.CREATE_TIME] = str(row[dao.CREATE_TIME])
    return temp
def get_url_in_trend(trend_id,event_offset):
    trend = fetch_trend_by_id(trend_id)
    event_list_str = trend[resources.TREND_EVENT_IDS]
    event_list = util.str_to_int_array(event_list_str,',')
    leng = len(event_list)
    if (leng - event_offset-1 >= 0):
        event_id = event_list[leng - event_offset-1]
        event = get_event_by_id(event_id)
        event_art_str = event[resources.EVENT_ARTICCLE_IDS]
        aids_str = util.convert_dic_article_to_list(event_art_str)
        aids = util.str_to_int_array(aids_str,',')
        arts = get_article_urls(aids)
        result = dict()
        for art in arts:
            temp = get_article_url_infor(art)
            result[temp[dao.ID]] = temp
        size = len(arts)
        final_list = []
        for i in range(size):
            final_list.append(result[aids[i]])
        return (final_list,str(event[resources.EVENT_DATE]),leng,event_id)
    return (None,None,leng,event_id)
def get_trend_in_day_except(day,trend_ids,catid_filter = -1):
    print 'catid_filter = %d'%catid_filter
    wherestr = get_in_where_str(trend_ids)
    if (catid_filter == -1):
        query = 'select * from %s where (%s = \'%s\') and (%s not in (%s) )'%(resources.TREND_TABLE,resources.TREND_LAST_DATE,\
        day,resources.TREND_ID,wherestr)
    else:
        query = 'select * from %s where (%s = \'%s\') and (%s not in (%s) ) and (%s = %d)'%(resources.TREND_TABLE,resources.TREND_LAST_DATE,\
        day,resources.TREND_ID,wherestr,resources.TREND_CATID,catid_filter)
    
    conn = dao.get_connection()
    cur = conn.cursor(pymysql.cursors.DictCursor)
    cur.execute(query)
    print query
    rows = cur.fetchall()
    dao.free_connection(conn,cur)
    return rows
def get_trend_by_last_update(day,catid = -1):
    if (catid == -1):
        query = 'select * from %s where (%s = \'%s\')'%(resources.TREND_TABLE,resources.TREND_LAST_DATE,day)
    else:
        query = 'select * from %s where (%s = \'%s\') and (%s = %d)'%(resources.TREND_TABLE,\
        resources.TREND_LAST_DATE,day,resources.TREND_CATID,catid)
    conn = dao.get_connection()
    cur = conn.cursor(pymysql.cursors.DictCursor)
    cur.execute(query)
    rows = cur.fetchall()
    dao.free_connection(conn,cur)
    return rows

def get_max_date(table=resources.EVENT_TABLE):
    query = 'select max(%s) from %s'%(resources.EVENT_DATE,table)
    conn = dao.get_connection()
    cur = conn.cursor()
    cur.execute(query)
    rows = cur.fetchall()
    dao.free_connection(conn,cur)
    row = rows[0]
    print row
    return str(row[0])

def insert_event_trend(event_id,trend_id,event_table = resources.EVENT_TABLE):
    query = 'update %s SET %s = %d WHERE %s = %d'%(event_table,resources.EVENT_TREND_ID,trend_id,resources.EVENT_ID,event_id)
    conn = dao.get_connection()
    cur = conn.cursor()
    cur.execute(query)
    conn.commit()
    dao.free_connection(conn,cur)

def update_trend_id_for_events(event_trend_pairs,event_table = resources.EVENT_TABLE, trend_table = resources.TREND_TABLE):
    conn = dao.get_connection()
    cur = conn.cursor()
    for pair in event_trend_pairs:
        (event_id,trend_id) = pair
        query = 'update %s SET %s = %d WHERE %s = %d'%(event_table,resources.EVENT_TREND_ID,trend_id,resources.EVENT_ID,event_id)
        cur.execute(query)
    conn.commit()
    dao.free_connection(conn,cur)

def insert_events_trend(event_ids,trend_id,event_table=resources.EVENT_TABLE):
    conn = dao.get_connection()
    cur = conn.cursor()
    whstr = get_in_where_str(event_ids)
    query = 'update %s SET %s = %d WHERE %s in (%s)'%(event_table,resources.EVENT_TREND_ID,trend_id,resources.EVENT_ID,whstr)
    cur.execute(query)
    conn.commit()
    dao.free_connection(conn,cur)

def get_all_trendids_and_event_ids(trend_table =  resources.TREND_TABLE):
    query = 'select %s,%s from %s '%(resources.TREND_ID,resources.TREND_EVENT_IDS,trend_table)
    conn = dao.get_connection()
    cur = conn.cursor(pymysql.cursors.DictCursor)
    cur.execute(query)
    rows = cur.fetchall()
    dao.free_connection(conn,cur)
    result  = dict()
    for row in rows:
        ids_str = row[resources.TREND_EVENT_IDS]
        ids = util.str_to_int_array(ids_str,',')
        result[row[resources.TREND_ID]] = ids
    return result

def get_single_trendids_and_event_ids(date,table= resources.TREND_TABLE):
    """
    get trends containing only one event
    """
    query = 'select %s,%s from %s where %s = 1 and %s = \'%s\''%(resources.TREND_ID,resources.TREND_EVENT_IDS,table,\
    resources.TREND_EVENT_NUM,resources.TREND_LAST_DATE,date)
    conn = dao.get_connection()
    cur = conn.cursor(pymysql.cursors.DictCursor)
    cur.execute(query)
    rows = cur.fetchall()
    dao.free_connection(conn,cur)
    result  = dict()
    for row in rows:
        ids_str = row[resources.TREND_EVENT_IDS]
        ids = util.str_to_int_array(ids_str,',')
        result[row[resources.TREND_ID]] = ids
    return result
def get_all_event_ids(table = resources.EVENT_TABLE):
    query = 'select %s from %s'%(resources.EVENT_ID,table)
    conn = dao.get_connection()
    cur = conn.cursor()
    cur.execute(query)
    rows = cur.fetchall()
    dao.free_connection(conn,cur)
    result = []
    for row in rows:
        result.append(int(row[0]))
    return result
def get_all_event_id_on_day(date,table = resources.EVENT_TABLE):
    query = 'select %s from %s where %s = \'%s\''%(resources.EVENT_ID,table,resources.EVENT_DATE,date)
    conn = dao.get_connection()
    cur = conn.cursor()
    cur.execute(query)
    rows = cur.fetchall()
    dao.free_connection(conn,cur)
    result = []
    for row in rows:
        result.append(int(row[0]))
    return result
def update_hash_tag_for_events(tag_dic,table = resources.EVENT_TABLE):
    conn = dao.get_connection()
    cur = conn.cursor()
    for event_id in tag_dic:
        query = 'update %s set %s = \'%s\' where %s = %d'%(table,resources.EVENT_TAG,tag_dic[event_id],resources.EVENT_ID,event_id)
        cur.execute(query)
    conn.commit()
    dao.free_connection(conn,cur)

def get_date_time_of_articles(aids):
    wstr = get_in_where_str(aids)
    query = 'select %s,%s from %s where %s in (%s)'%(dao.ID,dao.CREATE_TIME,dao.NEWS_TABLE,dao.ID,wstr)
    conn = dao.get_connection()
    cur = conn.cursor(pymysql.cursors.DictCursor)
    cur.execute(query)
    rows = cur.fetchall()
    dao.free_connection(conn,cur)
    result = dict()
    for row in rows:
        aid = row[dao.ID]
        date_time = row[dao.CREATE_TIME]
        result[aid] = date_time
    return result

def get_trend_update_after_date(date,table=resources.TREND_TABLE):
    query = 'select * from %s where %s >= \'%s\''%(table,resources.TREND_LAST_DATE,date)
    conn = dao.get_connection()
    cur = conn.cursor(pymysql.cursors.DictCursor)
    cur.execute(query)
    rows = cur.fetchall()
    dao.free_connection(conn,cur)
    return rows
    

def get_max_last_date(table= resources.TREND_TABLE):
    query = 'select max(%s) from %s '%(resources.TREND_LAST_DATE,table)
    conn = dao.get_connection()
    cur = conn.cursor()
    cur.execute(query)
    rows = cur.fetchall()
    dao.free_connection(conn,cur)
    row = rows[0]
    print row
    return str(row[0])

def get_tag_of_trend(trend_id,table= resources.TREND_TABLE):
    query = 'select %s from %s where %s = %d'%(resources.TREND_TAG,table,resources.TREND_ID,trend_id)
    conn = dao.get_connection()
    cur = conn.cursor()
    cur.execute(query)
    row = cur.fetchone()
    return row[0]
    
def get_tag_of_event(event_id,table= resources.EVENT_TABLE):
    query = 'select %s from %s where %s = %d'%(resources.EVENT_TAG,table,resources.EVENT_ID,event_id)
    conn = dao.get_connection()
    cur = conn.cursor()
    cur.execute(query)
    row = cur.fetchone()
    return row[0]
    
def read_articles_in_date(day,max_id=0):
    date_day = day + ' 00:00:00'
    date_next_day = day + ' 23:59:59'
    query = 'SELECT * \
    FROM %s WHERE (%s between \'%s\' and \'%s\') and (%s < %s) and (%s > %d)'%(\
    dao.NEWS_TABLE,dao.CREATE_TIME,date_day,date_next_day,dao.CREATE_TIME,dao.GET_TIME,\
    dao.ID,max_id)
    print query
    conn = dao.get_connection()
    cur = conn.cursor(pymysql.cursors.DictCursor)
    cur.execute(query)
    rows = cur.fetchall()
    dao.free_connection(conn,cur)
    return rows
def migrate_from_table2_to_table1(table1,table2):
    query = 'INSERT INTO %s SELECT * FROM %s'%(table1,table2)
    conn = dao.get_connection()
    cur = conn.cursor()
    cur.execute(query)
    conn.commit()
    dao.free_connection(conn,cur)