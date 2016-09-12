import util,resources,trend_util,data_reader,static_resources,dao
def get_stat_doc_count(day1,day2,select_domains=None):
    count = 0
    dic_domain = dict()
    domains = data_reader.get_domains(resources.DOMAIN_PATH)
    for domain in domains:
        dic_domain[domain] = []
    while (True):
        day = util.get_ahead_day(day1,count)
        dic_day = read_stat_count(day)
        for domain in dic_day:
            if domain in dic_domain:
                pair = dict()
                pair[day] = dic_day[domain]
                dic_domain[domain].append(pair)
        if (day == day2):
            break
        count += 1
    if ( not select_domains):
        return dic_domain
    else:
        result = dict()
        for did in select_domains:
            domain = static_resources.ID_TO_DOMAIN[did]
            result[domain] = dic_domain[domain]
        return result
            
def read_stat_count(day):
    stat_folder = trend_util.get_statistic_folder(day)
    stat_file_path = stat_folder + '/'+resources.DOC_STAT_FILE_NAME
    result = dict()
    f = open(stat_file_path,'r')
    f.readline()# the first line is not included
    for line in f:
        temp = line.strip()
        tgs = temp.split(',')
        if (tgs[0] != 'total'):
            right = int(tgs[1])
            error = int(tgs[2])
            short = int(tgs[3])
            result[tgs[0]] = right + error + short
        else:
            result['total'] = int(tgs[1])
    f.close()
    return result

def read_detect_time(day):
    time_path = trend_util.get_statistic_folder(day)+'/detect_time.txt'
    f = open(time_path,'r')
    line = f.readline()
    f.close()
    line = line.strip()
    numb = int(line)
    return numb
    
def get_detect_time(date1,date2):
    count = 0
    result = []
    while (True):
        day = util.get_ahead_day(date1,count)
        print 'day =%s'%day
        score = read_detect_time(day)
        pair = dict()
        pair[day] = score
        result.append(pair)
        if (day == date2):
            break
        count += 1
    return result
def get_total_doc_count(date1,date2):
    count = 0
    result = []
    while (True):
        day = util.get_ahead_day(date1,count)
        print 'day =%s'%day
        score = read_doc_count(day)
        pair = dict()
        pair[day] = score
        result.append(pair)
        if (day == date2):
            break
        count += 1
    return result

def read_doc_count(day):
    stat_folder = trend_util.get_statistic_folder(day)
    stat_file_path = stat_folder + '/'+resources.DOC_STAT_FILE_NAME
    f = open(stat_file_path,'r')
    total = 0
    for line in f:
        if ('total' in line):
            temp = line.strip()
            tgs = temp.split(',')
            total = int(tgs[1])
    f.close()
    return total

def get_like_str(domains):
    result = ''
    leng = len(domains)
    for i in range(leng-1):
        result += '%s like \'%s\' or '%(dao.URL,domains[i])
    result += '%s like \'%s\' '%(dao.URL,domains[i])
    return result

def get_aver_gap_time(day1,day2,domain):
    likestr = '%'+domain+'%'
    query = 'select date(%s),avg(TIMESTAMPDIFF(MINUTE, %s, %s)) \
    from %s where %s like \'%s\' and %s >= \'%s 00:00:00\' and \
    %s <= \'%s 23:59:59\' and %s < %s \
    group by date(%s) order by date(%s)'%(dao.CREATE_TIME,dao.CREATE_TIME,dao.GET_TIME,\
    dao.NEWS_TABLE,dao.URL,likestr,dao.CREATE_TIME,day1,
    dao.CREATE_TIME,day2,dao.CREATE_TIME,dao.GET_TIME,\
    dao.CREATE_TIME,dao.CREATE_TIME)
    print '%s'%query
    conn = dao.get_connection()
    cur = conn.cursor()
    cur.execute(query)
    rows = cur.fetchall()
    dao.free_connection(conn,cur)
    series = []
    for row in rows:
        day = str(row[0])
        averg = float(row[1])
        pair = dict()
        pair[day] = averg
        series.append(pair)
    return series
    
def get_averg_gap(day1,day2,selected_domains):
    data = dict()
    for did in selected_domains:
        domain = static_resources.ID_TO_DOMAIN[did]
        data[domain] = get_aver_gap_time(day1,day2,domain)
    return data

def get_error_create_time_count(day1,day2,selected_domains):
    data = dict()
    for did in selected_domains:
        domain = static_resources.ID_TO_DOMAIN[did]
        data[domain] = get_create_time_equal_get_time(day1,day2,domain)
    return data

def get_create_time_equal_get_time(day1,day2,domain):
    likestr = '%'+domain+'%'
    query = 'select date(%s),count(*) \
    from %s where %s like \'%s\' and %s >= \'%s 00:00:00\' and \
    %s <= \'%s 23:59:59\' and %s >= %s \
    group by date(%s) order by date(%s)'%(dao.CREATE_TIME,\
    dao.NEWS_TABLE,dao.URL,likestr,dao.CREATE_TIME,day1,
    dao.CREATE_TIME,day2,dao.CREATE_TIME,dao.GET_TIME,\
    dao.CREATE_TIME,dao.CREATE_TIME)
    print '%s'%query
    conn = dao.get_connection()
    cur = conn.cursor()
    cur.execute(query)
    rows = cur.fetchall()
    dao.free_connection(conn,cur)
    series = []
    for row in rows:
        day = str(row[0])
        averg = float(row[1])
        pair = dict()
        pair[day] = averg
        series.append(pair)
    return series
