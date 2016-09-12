import dao,resources,pymysql,online_resources,util,event_dao,detec_trend,static_resources,trend_util
from sets import Set
def get_real_time_events(cat_filter=-1):
    if (cat_filter == -1):
        query = 'select * from %s'%(resources.EVENT_CURRENT_TABLE)
    else:
        query = 'select * from %s where  %s = %d'%(resources.EVENT_CURRENT_TABLE,resources.EVENT_CATID,cat_filter)
    conn = dao.get_connection()
    cur = conn.cursor(pymysql.cursors.DictCursor)
    cur.execute(query)
    rows = cur.fetchall()
    dao.free_connection(conn,cur)
    return rows

def get_trend_current():
    query = 'select %s from %s'%(resources.EVENT_TREND_ID,resources.EVENT_CURRENT_TABLE)
    conn = dao.get_connection()
    cur = conn.cursor(pymysql.cursors.DictCursor)
    cur.execute(query)
    rows = cur.fetchall()
    dao.free_connection(conn,cur)
    result  = []
    for row in rows:
        result.append(row[resources.EVENT_TREND_ID])
    return result

def create_insert_events_query(event_id,date,article_ids,center,size,img_path,coherence,catid,trend_id,table_name = resources.EVENT_CURRENT_TABLE):
    query = 'INSERT INTO %s(%s,%s,%s,%s,%s,%s,%s,%s,%s)\
    VALUES(\'%s\',\'%s\',\'%s\',%d,%d,\'%s\',%f,%d,%d)'%\
    (table_name,\
    resources.EVENT_DATE,resources.EVENT_CENTER,resources.EVENT_ARTICCLE_IDS,resources.EVENT_SIZE,\
    resources.EVENT_ID,resources.EVENT_TOPWORD_IMG,resources.EVENT_COHERENCE,resources.EVENT_CATID,resources.EVENT_TREND_ID
    ,date,center,article_ids,size,event_id,img_path,coherence,int(catid),trend_id)
    return query

def connect_current_event_with_trend(day):
    events = get_real_time_events()
    for event in events:
        event[resources.EVENT_CENTER] = util.str_to_dict(event[resources.EVENT_CENTER])
    pre_day = util.get_past_day(day,online_resources.WINDOW_SIZE)
    trends = event_dao.get_trend_between_day(day,pre_day,1)
    centers = dict()
    sizes = dict()
    for trend in trends:
        centers[trend[resources.TREND_ID]] = util.str_to_dict(trend[resources.TREND_CENTER])
        sizes[trend[resources.TREND_ID]] = trend[resources.TREND_SIZE]
    for event in events:
        (trend_id,score) = detec_trend.connect_events(centers,sizes,event[resources.EVENT_CENTER],event[resources.EVENT_SIZE])
        if (trend_id):
            event[online_resources.ATTACHED_TREND] = trend_id
        else:
            event[online_resources.ATTACHED_TREND] = -1

def get_article_series_by_hours(aids,day,max_hours=None,inter=1):
    news = event_dao.get_article_urls(aids)
    hours = dict()
    max_range = 23
    if (max_hours):
        max_range = max_hours
    for i in range(max_range+1):
        hours[i] = 0
    for new in news:
        hour = new[dao.CREATE_TIME].hour
        if hour in hours:
            hours[hour] += 1
        else:
            print 'max_hour: %d and hour = %d'%(max_range,hour)
            print '========================article %d has create_time > current'%new[dao.ID]
    result = []
    for i in range(max_range+1):
        ob = dict()
        key = '%s %02d:00:00'%(day,i)
        value = hours[i]
        ob[key] = value
        result.append(ob)
    return result
def get_scores_from_events(event_ids,recent_num):
    chosen_event_ids = event_ids
    if (len(event_ids) > recent_num):
        chosen_event_ids = []
        last = len(event_ids)
        for i in range(last-recent_num,last):
            chosen_event_ids.append(event_ids[i])
    hours_dic = dict()
    sum_dic = dict()
    days = []
    for event_id in chosen_event_ids:
        event = event_dao.get_event_by_id(event_id)
        art_dic_str = event[resources.EVENT_ARTICCLE_IDS]
        full_articles_id = util.get_full_article_ids(art_dic_str)
        event_str = str(event[resources.EVENT_DATE])
        hours = get_article_series_by_hours(full_articles_id,event_str)
        
        if (event_str not in hours_dic):
            hours_dic[event_str] = hours
            days.append(event_str)
            sum_dic[event_str] = len(full_articles_id)
        else:
            pre_hour = hours_dic[event_str]
            hours_dic[event_str] = aggregate_hours(pre_hour,hours)
            sum_dic[event_str] += len(full_articles_id)
    result = []
    total = []
    for i in range(len(days)):
        day = days[i]
        for el in hours_dic[day]:
            result.append(el)
        ob = dict()
        ob[day] = sum_dic[day]
        total.append(ob)
    return (result,total)
        
def aggregate_hours(h1,h2):
    result = []
    for i in range(len(h1)):
        item1 = h1[i]
        item2 = h2[i]
        item = dict()
        key = item1.keys()[0]
        item[key] = item1[key]+item2[key]
        result.append(item)
    return result
def get_current_events(day,cat_filter = -1):
    events = get_real_time_events(cat_filter)
    trend_ids = []
    cat_map = static_resources.CATMAP
    current_hour = util.get_current_date().hour
    for event in events:
        art_dic_str = event[resources.EVENT_ARTICCLE_IDS]
        full_articles_id = util.get_full_article_ids(art_dic_str)
        series = get_article_series_by_hours(full_articles_id,day,current_hour)
        total_ob = []
        day_total = dict()
        day_total[day] = len(full_articles_id)
        total_ob.append(day_total)
        hour_ob = dict()
        if (event[resources.EVENT_TREND_ID] != -1):
            trend = event_dao.fetch_trend_by_id(event[resources.EVENT_TREND_ID])
            event_ids = util.str_to_int_array(trend[resources.TREND_EVENT_IDS],',')
            (pre_series,pre_total) = get_scores_from_events(event_ids,online_resources.EVENT_DISPLAY_NUM)
            for el in series:
                pre_series.append(el)
            for t in total_ob:
                pre_total.append(t)
            hour_ob['series'] = pre_series
            hour_ob['total'] = pre_total
            event[online_resources.HOUR_SERIES] = hour_ob
            event['offset'] = -1
            trend_ids.append(event[resources.EVENT_TREND_ID])
        else:
            hour_ob['series'] = series
            hour_ob['total']= total_ob
            event[online_resources.HOUR_SERIES] = hour_ob
            event['offset'] = -1
        catid = resources.EVENT_CATID
        event[catid] = cat_map[event[catid]] 
        trend_util.handle_event_object(event)
    return (events,trend_ids)

def get_current_trend_ids(catid = -1):
    if catid == -1:
        query = 'select distinct %s from %s where (%s > 0)'%(resources.EVENT_TREND_ID,\
        resources.EVENT_CURRENT_TABLE,resources.EVENT_TREND_ID)
    else:
        query = 'select %s from %s where (%s > 0) and (%s=%d)'%(resources.EVENT_TREND_ID,\
        resources.EVENT_CURRENT_TABLE,resources.EVENT_TREND_ID,resources.EVENT_CATID,catid)
    conn = dao.get_connection()
    cur = conn.cursor()
    cur.execute(query)
    rows = cur.fetchall()
    dao.free_connection(conn,cur)
    result = []
    for row in rows:
        result.append(row[0])
    return result

def get_events_in_trend(trend_id):
    query = 'select * from %s where %s = %d'%(resources.EVENT_CURRENT_TABLE,resources.EVENT_TREND_ID,trend_id)
    conn = dao.get_connection()    
    cur = conn.cursor(pymysql.cursors.DictCursor)
    cur.execute(query)
    rows = cur.fetchall()
    dao.free_connection(conn,cur)
    return rows

def get_event_id_list_in_trend(trend_id):
    query = 'select %s from %s where %s = %d'%(resources.EVENT_ID,resources.EVENT_CURRENT_TABLE,resources.EVENT_TREND_ID,trend_id)
    print query
    conn = dao.get_connection()
    cur = conn.cursor(pymysql.cursors.DictCursor)
    cur.execute(query)
    rows = cur.fetchall()
    dao.free_connection(conn,cur)
    ids = []
    for row in rows:
        ids.append(row[resources.EVENT_ID])
    return ids

def get_event_by_id(event_id):
    return event_dao.get_event_by_id(event_id,resources.EVENT_CURRENT_TABLE)

def get_current_event_wo_trend(catid=-1):
    if catid == -1:
        query = 'select * from %s where %s = -1'%(resources.EVENT_CURRENT_TABLE,resources.EVENT_TREND_ID)
    else:
        query = 'select * from %s where %s = -1 and %s = %d'%(resources.EVENT_CURRENT_TABLE,\
        resources.EVENT_TREND_ID,resources.EVENT_CATID,catid)
    conn = dao.get_connection()
    cur = conn.cursor(pymysql.cursors.DictCursor)
    cur.execute(query)
    rows = cur.fetchall()
    dao.free_connection(conn,cur)
    return rows
def get_event_ids(table=resources.EVENT_CURRENT_TABLE):
    query = 'select %s from %s '%(resources.EVENT_ID,table)
    conn = dao.get_connection()
    cur = conn.cursor(pymysql.cursors.DictCursor)
    cur.execute(query)
    rows = cur.fetchall()
    dao.free_connection(conn,cur)
    ids = Set()
    for row in rows:
        ids.add(row[resources.EVENT_ID])
    return ids

def get_day_onreal_time():
    query = 'select max(%s) from %s'%(resources.EVENT_DATE,resources.EVENT_CURRENT_TABLE)
    conn = dao.get_connection()
    cur = conn.cursor()
    cur.execute(query)
    row = cur.fetchone()
    dao.free_connection(conn,cur)
    if (row[0]):
        return str(row[0])
    return None

#get_event_id_list_in_trend(2989)