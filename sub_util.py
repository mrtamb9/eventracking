import resources,sub_resources,util,trend_util,dao,pymysql,event_dao,online_dao,news_img
from sets import Set
def get_sub_trend_folder(trend_id,sub_trend_id):
    trend_folder = trend_util.get_trend_folder(trend_id)
    return trend_folder+'/'+sub_resources.TREND_DECOMPOS_FOLDER+'/%d'%sub_trend_id

def get_sub_trend_img_path(trend_id,sub_trend_id):
    sub_folder = get_sub_trend_folder(trend_id,sub_trend_id)
    return sub_folder+'/img.png'

def get_sub_trend_outside_folder(trend_id):
    trend_folder = trend_util.get_trend_folder(trend_id)
    return trend_folder+'/'+sub_resources.TREND_DECOMPOS_FOLDER

def get_sub_trend_top_text_path(trend_id,sub_trend_id):
    sub_folder = get_sub_trend_folder(trend_id,sub_trend_id)
    return sub_folder+'/top.txt'

def get_sub_trend_descripton_path(trend_id):
    trend_folder = trend_util.get_trend_folder(trend_id)
    return trend_folder + '/'+sub_resources.TREND_DECOMPOS_FOLDER+'/description.html'

def get_sub_event_context_center_path(trend_id,sub_id):
    sub_trend_folder = get_sub_trend_folder(trend_id,sub_id)
    return sub_trend_folder + '/context.dat'

def get_sub_event_entity_center_path(trend_id,sub_id):
    sub_trend_folder = get_sub_trend_folder(trend_id,sub_id)
    return sub_trend_folder + '/entity.dat'

def get_black_domain_set():
    f = open(sub_resources.BLACK_DOMAIN_PATH,'r')
    result = Set()
    for line in f:
        temp = line.strip()
        if len(temp) > 2:
            result.add(temp)
    f.close()
    return result

def create_insert_query(sub_event,table = sub_resources.TABLE_NAME):
    #sub_event[sub_resources.CENTER] = 'center'
    #sub_event[sub_resources.EN_CENTER] = 'encenter'
    preprocess_sub_event(sub_event)
    query = 'insert into %s(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) values (%d,\'%s\',\'%s\',\'%s\',\'%s\',%d,\'%s\',%d,\'%s\',%f,%d)'\
    %(table,\
    sub_resources.ID,sub_resources.ARTICLES,\
    sub_resources.IMG,sub_resources.LAST_DATE,sub_resources.START_DATE,\
    sub_resources.TOTAL_ARTICLE_NUM,sub_resources.TAG,sub_resources.TREND_ID,sub_resources.TITLE,sub_resources.COHERENCE,sub_resources.TYPICAL_ART,\
    sub_event[sub_resources.ID],sub_event[sub_resources.ARTICLES],\
    sub_event[sub_resources.IMG],sub_event[sub_resources.LAST_DATE],sub_event[sub_resources.START_DATE],\
    sub_event[sub_resources.TOTAL_ARTICLE_NUM],sub_event[sub_resources.TAG],sub_event[sub_resources.TREND_ID],sub_event[sub_resources.TITLE],\
    sub_event[sub_resources.COHERENCE],sub_event[sub_resources.TYPICAL_ART])
    return query

def preprocess_sub_event(sub_event):
    art = sub_event[sub_resources.ARTICLES]
    sub_event[sub_resources.ARTICLES] = art.replace('\'','\\\'')
    if sub_event[sub_resources.TITLE]:
        sub_event[sub_resources.TITLE] = sub_event[sub_resources.TITLE].replace('\'','"')
    else:
        sub_event[sub_resources.TITLE] = ''
    if sub_event[sub_resources.TAG]:
        sub_event[sub_resources.TAG] = sub_event[sub_resources.TAG].replace('\'','"')
    else:
        sub_event[sub_resources.TAG] = ''

def create_update_query(sub_trend,table = sub_resources.TABLE_NAME):
    preprocess_sub_event(sub_trend)
    query = 'update %s set %s=\'%s\',%s=\'%s\',%s=\'%s\',%s=%d,%s=\'%s\',%s=%d,%s=\'%s\',%s=%f, %s =%d where %s = %d'%(table,\
    sub_resources.ARTICLES,sub_trend[sub_resources.ARTICLES],\
    sub_resources.IMG,sub_trend[sub_resources.IMG],\
    sub_resources.LAST_DATE,sub_trend[sub_resources.LAST_DATE],\
    sub_resources.TOTAL_ARTICLE_NUM,sub_trend[sub_resources.TOTAL_ARTICLE_NUM],\
    sub_resources.TAG,sub_trend[sub_resources.TAG],\
    sub_resources.TREND_ID,sub_trend[sub_resources.TREND_ID],\
    sub_resources.TITLE,sub_trend[sub_resources.TITLE],\
    sub_resources.COHERENCE,sub_trend[sub_resources.COHERENCE],\
    sub_resources.TYPICAL_ART,sub_trend[sub_resources.TYPICAL_ART],\
    sub_resources.ID,sub_trend[sub_resources.ID])
    return query

def insert_sub_events(sub_events,table=sub_resources.TABLE_NAME):
    conn = dao.get_connection()
    cur = conn.cursor()
    for sub_event in sub_events:
            query = create_insert_query(sub_event,table)
            if (query):
                cur.execute(query)
    conn.commit()
    dao.free_connection(conn,cur)
def get_sub_event_of_trend(trend_id,date,pre_interval):
    pre_day = util.get_past_day(date,pre_interval)
    pre_day += ' 00:00:00'
    query = 'select * from %s where (%s = %d) and (%s >= \'%s\')'%(sub_resources.TABLE_NAME,sub_resources.TREND_ID,trend_id,\
    sub_resources.LAST_DATE,pre_day)
    print query
    conn = dao.get_connection()
    cur = conn.cursor(pymysql.cursors.DictCursor)
    cur.execute(query)
    rows = cur.fetchall()
    dao.free_connection(conn,cur)    
    return rows

def get_trend_release_path(trend_id):
    trend_folder = trend_util.get_trend_folder(trend_id)
    return trend_folder + '/'+sub_resources.TREND_DECOMPOS_FOLDER+'/release.html'
def get_last_day_of_trend(trend_id):
    query = 'select %s from %s where %s = %d'%(resources.TREND_LAST_DATE,\
    resources.TREND_TABLE,resources.TREND_ID,trend_id)
    conn = dao.get_connection()
    cur = conn.cursor()
    cur.execute(query)
    row = cur.fetchone()
    dao.free_connection(conn,cur)
    return str(row[0])

def get_max_last_datetime_obj(trend_id,table = sub_resources.TABLE_NAME):
    query = 'select max(%s) from %s where %s = %d'%(sub_resources.LAST_DATE,\
    table,sub_resources.TREND_ID,trend_id)
    conn = dao.get_connection()
    cur = conn.cursor()
    cur.execute(query)
    row = cur.fetchone()
    dao.free_connection(conn,cur)
    return row[0]

def get_max_start_datetime_obj(trend_id,table = sub_resources.TABLE_NAME):
    query = 'select max(%s) from %s where %s = %d'%(sub_resources.START_DATE,\
    table,sub_resources.TREND_ID,trend_id)
    conn = dao.get_connection()
    cur = conn.cursor()
    cur.execute(query)
    row = cur.fetchone()
    dao.free_connection(conn,cur)
    return row[0]

def get_sub_event_of_trend_wo_date(trend_id,day_num = 10):
    last_date_ob  = get_max_last_datetime_obj(trend_id)
    last_day = last_date_ob.date()
    return get_sorted_sub_event(trend_id,str(last_day),day_num)

def get_sorted_sub_event(trend_id,date,pre_interval):
    pre_day = util.get_past_day(date,pre_interval)
    pre_day += ' 00:00:00'
    query = 'select * from %s where (%s = %d) and (%s >= \'%s\') order by %s asc'%(sub_resources.TABLE_NAME,sub_resources.TREND_ID,trend_id,\
    sub_resources.LAST_DATE,pre_day,sub_resources.START_DATE)
    print query
    conn = dao.get_connection()
    cur = conn.cursor(pymysql.cursors.DictCursor)
    cur.execute(query)
    rows = cur.fetchall()
    dao.free_connection(conn,cur)    
    return rows

def update_sub_trend(sb_events):
    conn = dao.get_connection()
    cur = conn.cursor()
    for sub_event in sb_events:
        query = create_update_query(sub_event)
        #print 'UPDATE: %s'%query
        cur.execute(query)
    conn.commit()
    dao.free_connection(conn,cur)
def get_number_trend_record(trend_id):
    query = 'select count(*) from %s where %s = %d'%(sub_resources.TABLE_NAME,sub_resources.TREND_ID,trend_id)
    conn = dao.get_connection()
    cur = conn.cursor()
    cur.execute(query)
    count = cur.fetchone()
    dao.free_connection(conn,cur)
    return count[0]

def delete_record_with_trend_id(trend_id):
    print 'delete trend_id %d'%trend_id
    query = 'delete from %s where %s = %d'%(sub_resources.TABLE_NAME,sub_resources.TREND_ID,trend_id)
    conn = dao.get_connection()
    cur = conn.cursor()
    cur.execute(query)
    conn.commit()
    dao.free_connection(conn,cur)

def get_max_date_in_sub_table(table = sub_resources.TABLE_NAME):
    query = 'select max(%s) from %s'%(sub_resources.LAST_DATE,table)
    conn = dao.get_connection()
    cur = conn.cursor()
    cur.execute(query)
    row = cur.fetchone()
    dao.free_connection(conn,cur)
    date_str = str(row[0].date())
    return date_str

def get_max_date_before(date,trend_id,table):
    last_datetime = '%s 23:59:59'%date
    query = 'select max(date(%s)) from %s where %s = %d and %s <= \'%s\''%(sub_resources.LAST_DATE,table,sub_resources.TREND_ID,\
    trend_id,sub_resources.LAST_DATE,last_datetime)
    print query
    conn = dao.get_connection()
    cur = conn.cursor()
    cur.execute(query)
    row = cur.fetchone()
    dao.free_connection(conn,cur)
    if row:
        return str(row[0])
    return date

def get_sub_trend(trend_id,max_start_date,interval,subsize_threshold=sub_resources.SUB_SIZE_THRESHOLD,table =sub_resources.TABLE_NAME):
    max_last_date = get_max_date_before(max_start_date,trend_id,table)
    max_start_datetime = max_last_date+' 23:59:59'
    min_start_date = util.get_past_day(max_last_date,interval)
    min_start_datetime = min_start_date + ' 00:00:00'
    query = 'select * from %s where %s = %d and %s <= \'%s\' and %s >= \'%s\' and %s >= %d order by %s desc'%(\
    table,sub_resources.TREND_ID,trend_id,sub_resources.START_DATE,max_start_datetime,sub_resources.START_DATE,min_start_datetime,\
    sub_resources.TOTAL_ARTICLE_NUM,subsize_threshold,sub_resources.LAST_DATE)
    print query
    conn = dao.get_connection()
    cur = conn.cursor(pymysql.cursors.DictCursor)
    cur.execute(query)
    rows = cur.fetchall()
    dao.free_connection(conn,cur)
    return rows

def get_real_time_sub_trend(trend_id):
    query = 'select * from %s where %s = %d order by %s desc'%(sub_resources.REAL_TABLE,sub_resources.TREND_ID,\
    trend_id,sub_resources.LAST_DATE)
    conn = dao.get_connection()
    cur = conn.cursor(pymysql.cursors.DictCursor)
    cur.execute(query)
    rows = cur.fetchall()
    dao.free_connection(conn,cur)
    return rows
    
def get_all_sub_trend_with_start_date(trend_id,start_date,table_name):
    res_subs = get_sub_trend(trend_id,start_date,sub_resources.GET_DATE_INTERVAL_THRESHOLD,sub_resources.SUB_SIZE_THRESHOLD,table_name)
    if (len(res_subs) == 0):
        res_subs = get_sub_trend(trend_id,start_date,sub_resources.GET_DATE_INTERVAL_THRESHOLD,1,table_name)
    for row in res_subs:
        print row['id'],row['articles']
    recover_article_dic(res_subs)
    return_obj  = dict()
    if (len(res_subs) > 0):
        add_title_for_sub_trend(res_subs)
        min_date_time_session = res_subs[-1][sub_resources.START_DATE]
        min_date = str(min_date_time_session.date())
        load_next_date = util.get_past_day(min_date,1)
        for sub_trend in res_subs:
            pre_process_json_sub_trend(sub_trend)
        return_obj['load_date'] = load_next_date
        return_obj['data'] = res_subs
    else:
        return_obj['load_date'] ='STOP'
        return_obj['data'] = 'NONE'
    return return_obj

def get_all_sub_trend_of_real_new_trend(trend_id):
    real_subs = get_real_time_sub_trend(trend_id)
    trend = online_dao.get_event_by_id(trend_id)
    trend[resources.TREND_LAST_DATE] = str(trend[resources.EVENT_DATE])
    trend[resources.TREND_IMG] = trend[resources.EVENT_IMG2]
    del trend[resources.EVENT_CENTER]
    del trend[resources.EVENT_DATE]
    recover_article_dic(real_subs)
    #trend = dict()
    filter_res = []
    for sub_trend in real_subs:
            if (sub_trend[sub_resources.TOTAL_ARTICLE_NUM] >= sub_resources.SUB_SIZE_THRESHOLD):
                pre_process_json_sub_trend(sub_trend)
                filter_res.append(sub_trend)
    trend['load_date'] = 'STOP'
    trend['sub_data'] = filter_res
    return trend

def get_all_sub_trend_first_time(trend_id,table_name):
    max_datetime = get_max_last_datetime_obj(trend_id,table_name)
    if (not max_datetime):#real time and completely new
        res_subs = get_all_sub_trend_of_real_new_trend(trend_id)
        return res_subs
    max_date = max_datetime.date()
    res_subs = get_sub_trend(trend_id,str(max_date),sub_resources.GET_DATE_INTERVAL_THRESHOLD,1,table_name)
    if len(res_subs) == 0: #tuc la bi gop tat ca thanh 1
        max_start_datetime = get_max_start_datetime_obj(trend_id,table_name)
        max_start_date = max_start_datetime.date()
        res_subs = get_sub_trend(trend_id,max_start_date,sub_resources.GET_DATE_INTERVAL_THRESHOLD,1,table_name)
    
    recover_article_dic(res_subs)
    subs_dic = util.get_dic_from_list(res_subs,sub_resources.ID)
    real_subs = get_real_time_sub_trend(trend_id)
    real_all_leng = len(real_subs)
    for i in range(real_all_leng):
        real_s = real_subs[real_all_leng - i -1]
        real_sub_id = real_s[sub_resources.ID]
        if real_sub_id in subs_dic:
            merge_sub2_to_sub1(subs_dic[real_sub_id],real_s)
        else:
            recover_article_dic([real_s])
            res_subs.insert(0,real_s)
    if (len(res_subs) > 0):
        add_title_for_sub_trend(res_subs)
        trend = event_dao.fetch_trend_by_id(trend_id)
        if (len(real_subs) > 0):
            trend[resources.TREND_LAST_DATE] = util.get_today_str()
        pre_process_json_trend(trend)
        min_date_time_session = res_subs[-1][sub_resources.START_DATE]
        min_date = str(min_date_time_session.date())
        load_next_date = util.get_past_day(min_date,1)
        filter_res = []
        for sub_trend in res_subs:
            if (sub_trend[sub_resources.TOTAL_ARTICLE_NUM] >= sub_resources.SUB_SIZE_THRESHOLD):
                pre_process_json_sub_trend(sub_trend)
                filter_res.append(sub_trend)
        trend['load_date'] = load_next_date
        trend['sub_data'] = filter_res
        return trend
    else:
        return_obj = dict()
        return_obj['load_date'] ='STOP'
        return_obj['data'] = 'NONE'
        return return_obj

def add_title_for_sub_trend(sub_trends):
    all_ids = []
    for sub_trend in sub_trends:
        typical_id  = sub_trend[sub_resources.TYPICAL_ART]
        all_ids.append(typical_id)
        sub_trend[sub_resources.COHERENCE] = sub_trend[sub_resources.TOTAL_ARTICLE_NUM]
    art_list = event_dao.get_article_urls(all_ids)
    art_dic = util.get_dic_from_list(art_list,dao.ID)
    for sub_trend in sub_trends:
        typical_id  = sub_trend[sub_resources.TYPICAL_ART]
        sub_trend[sub_resources.TITLE] = art_dic[typical_id][dao.TITLE]
    

def pre_process_json_trend(trend):
    trend[resources.TREND_LAST_DATE] = str(trend[resources.TREND_LAST_DATE])
    trend[resources.TREND_START_DATE] = str(trend[resources.TREND_START_DATE])

def recover_article_dic(sub_trends):
    for sub_trend in sub_trends:
        dicstr = sub_trend[sub_resources.ARTICLES]
        sub_trend[sub_resources.ARTICLES] = util.str_to_dict(dicstr)

def pre_process_json_sub_trend(sub_trend):
    sub_id = sub_trend['id']
    print sub_id
    if sub_id == 68276040:
        print 'what is id'
    sub_trend[sub_resources.LAST_DATE]  = str(sub_trend[sub_resources.LAST_DATE])
    sub_trend[sub_resources.START_DATE]  = str(sub_trend[sub_resources.START_DATE])
    all_ids = get_all_aids_from_dic(sub_trend[sub_resources.ARTICLES])
    sub_trend[sub_resources.ARTICLES] = util.list_to_str(all_ids)
    try:
        typical_id = sub_trend[sub_resources.TYPICAL_ART]
        arts = event_dao.get_article_urls([typical_id])
        art = arts[0]
        sub_trend[sub_resources.IMG] = art[news_img.IMG_URL]
        sub_trend[sub_resources.TITLE] = art[dao.TITLE]
    except:
        print 'failt to get typical_id of sub_trend: %d'%sub_trend[sub_resources.ID]

def merge_sub2_to_sub1(sub1,sub2):
    sub1[sub_resources.LAST_DATE] = max(sub1[sub_resources.LAST_DATE],sub2[sub_resources.LAST_DATE])
    art_dic2 = util.str_to_dict(sub2[sub_resources.ARTICLES])
    date2 = art_dic2.keys()[0]
    sub1[sub_resources.ARTICLES][date2] = art_dic2[date2]
    sub1[sub_resources.TOTAL_ARTICLE_NUM] += sub2[sub_resources.TOTAL_ARTICLE_NUM]
    sub1[sub_resources.TYPICAL_ART] = sub2[sub_resources.TYPICAL_ART]
#    if (len(sub2[sub_resources.IMG]) > 5):
#        sub1[sub_resources.IMG] = sub2[sub_resources.IMG]
#    if (len(sub2[sub_resources.TITLE]) > 5):
#        sub1[sub_resources.TITLE] = sub2[sub_resources.TITLE]
#    if (len(sub2[sub_resources.TAG]) > 3):
#        sub1[sub_resources.TAG] = sub2[sub_resources.TAG]

def get_all_aids_from_dic(art_date_dic):
    date_keys = art_date_dic.keys()
    pairs = util.get_sorted_dates(date_keys)
    res = []
    for i in range(len(pairs)):
        temp_date = pairs[i]
        print temp_date
        for el in art_date_dic[temp_date] :
            res.append(el)
    return res

def get_hot_subtrend_in_table(trend_id,limit,table=sub_resources.TABLE_NAME):
    query = 'select * from %s where %s = %d order by %s desc limit %d'%(table,sub_resources.TREND_ID,trend_id,\
    sub_resources.LAST_DATE,limit)
    conn = dao.get_connection()
    cur = conn.cursor(pymysql.cursors.DictCursor)
    cur.execute(query)
    rows = cur.fetchall()
    dao.free_connection(conn,cur)
    return rows

def get_hot_subtrend_in_table_with_black_subs(trend_id,limit,black_sub_list,table=sub_resources.TABLE_NAME):
    if (len(black_sub_list) > 0):
        wherestr  = util.get_in_where_str(black_sub_list)
        query = 'select * from %s where (%s = %d) and (%s not in (%s)) order by %s desc limit %d'%(table,\
        sub_resources.TREND_ID,trend_id,\
        sub_resources.ID,wherestr,sub_resources.LAST_DATE,limit)
    else:
        query = 'select * from %s where (%s = %d) order by %s desc limit %d'%(table,\
        sub_resources.TREND_ID,trend_id,\
        sub_resources.LAST_DATE,limit)

    
    conn = dao.get_connection()
    cur = conn.cursor(pymysql.cursors.DictCursor)
    cur.execute(query)
    print query
    rows = cur.fetchall()
    dao.free_connection(conn,cur)
    return rows

def get_hot_article_of_subtrend(trend_id):
    real_hots = get_hot_subtrend_in_table(trend_id,1,sub_resources.REAL_TABLE)
    if (not real_hots):
        real_hots = get_hot_subtrend_in_table(trend_id,1,sub_resources.TABLE_NAME)
    aid = real_hots[0][sub_resources.TYPICAL_ART]
    articles = event_dao.get_article_urls([aid])
    return articles[0]

def get_hot_sub_trend(trend_id,limit):
    real_hots = get_hot_subtrend_in_table(trend_id,limit,sub_resources.REAL_TABLE)
    real_subids = []
    typicals =[]
    for sub_trend in real_hots:
        real_subids.append(sub_trend[sub_resources.ID])
        typicals.append(sub_trend[sub_resources.TYPICAL_ART])
    real_size = len(real_hots)
    if (real_size < limit):
        add_size = limit - real_size
        p_sub_trends = get_hot_subtrend_in_table_with_black_subs(trend_id,add_size,real_subids,sub_resources.TABLE_NAME)
        for sub_trend in p_sub_trends:
            typicals.append(sub_trend[sub_resources.TYPICAL_ART])
    articles = event_dao.get_article_urls(typicals)
    art_dic = util.get_dic_from_list(articles,dao.ID)
    result  =[]
    for i in range(len(typicals)):
        aid = typicals[i]
        temp = event_dao.get_article_url_infor(art_dic[aid])
        result.append(temp)
    return result

def remove_all_records(table = sub_resources.REAL_TABLE):
    query = 'delete from %s'%table
    conn = dao.get_connection()
    cur = conn.cursor()
    cur.execute(query)
    conn.commit()
    dao.free_connection(conn,cur)

def get_sub_trend_by_start_date(trend_id,start_date):
    return None

def get_sub_event_records(sub_ids,table):
    conn = dao.get_connection()
    cur = conn.cursor(pymysql.cursors.DictCursor)
    wstr = util.get_in_where_str(sub_ids)
    query = 'SELECT * FROM %s WHERE %s in (%s)'%(table,sub_resources.ID,wstr)
    cur.execute(query)
    print query
    rows = cur.fetchall()
    dao.free_connection(conn,cur)
    return rows
#result = get_all_sub_trend_first_time(13866,'sub_event_binh')
#result = get_all_sub_trend_with_start_date(4636,'2016-05-30','sub_event_binh')
