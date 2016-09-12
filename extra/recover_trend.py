import sys,os
parent_folder = os.path.dirname(os.path.abspath(__file__))+'/..'
sys.path.insert(0,parent_folder)
import event_dao,resources,util,dao
#print resources.DATA_FOLDER
from sets import Set
def recover_trends_before_date(date):
    print 'recover state before %s'%date
    trends = event_dao.get_trend_update_after_date(date)
    event_size_dic = get_event_ids_in_range(date,util.get_today_str())
    remove_event_set =  Set(event_size_dic.keys())
    new_trend_from_date = [] #trend, start >= date
    last_date_dic = dict()
    change_trend_dic = dict()
    for trend in trends:
        trend_id = trend[resources.TREND_ID]
        event_ids = util.str_to_int_array(trend[resources.TREND_EVENT_IDS],',')
        cut_index  = get_cut_index(event_ids,remove_event_set)
        if cut_index == -1:
            new_trend_from_date.append(trend[resources.TREND_ID])
        else:
            old_event_ids = event_ids[0:cut_index+1]
            old_event_num = cut_index+1
            trend[resources.TREND_EVENT_NUM] = old_event_num
            trend[resources.TREND_EVENT_IDS] = get_event_list_str(old_event_ids)
            remove_size = get_total_event_size(event_ids[cut_index+1:],event_size_dic)
            trend[resources.TREND_SIZE] = trend[resources.TREND_SIZE] - remove_size
            last_date_dic[trend[resources.TREND_ID]] = event_ids[cut_index]
            change_trend_dic[trend[resources.TREND_ID]] = trend
    date_dic = get_event_date_dic(last_date_dic.values())
    for trend_id in change_trend_dic:
        last_event = last_date_dic[trend_id]
        change_trend_dic[trend_id][resources.TREND_LAST_DATE] = date_dic[last_event]
    #print 'removing detail records on event_trend_table'
    #remove_records_in_trend_event_table(event_size_dic.keys())
    print 'update db, removing trends after date: %s'%date
    remove_trend(new_trend_from_date)
    print 'update db, update trend-infor before date: %s'%date
    for trend_id in change_trend_dic:
        print 'update infor for trend: %d'%trend_id
        event_dao.update_trend(change_trend_dic[trend_id])
    
    # calculate last_date for trends
    print 'remove events on table events'
    remove_events_on_date(date)
    print 'finished ...'
    

def remove_events_on_date(date):
    query  = 'delete from %s where %s >= \'%s\''%(resources.EVENT_TABLE,resources.EVENT_DATE,date)
    print query
    conn = dao.get_connection()
    cur = conn.cursor()
    cur.execute(query)
    conn.commit()
    dao.free_connection(conn,cur)

def get_event_date_dic(event_ids):
    wstr = util.get_in_where_str(event_ids)
    query = 'select %s,%s from %s where %s in (%s)'%(resources.EVENT_ID,resources.EVENT_DATE,resources.EVENT_TABLE,resources.EVENT_ID,wstr)
    print query
    conn = dao.get_connection()
    cur = conn.cursor()
    cur.execute(query)
    result = dict()
    rows = cur.fetchall()
    for row in rows:
        result[row[0]] = str(row[1])
    dao.free_connection(conn,cur)
    return result

def get_total_event_size(event_ids,size_dic):
    count = 0
    for i in range(len(event_ids)):
        count += size_dic[event_ids[i]]
    return count
def get_event_list_str(event_ids):
    result = ''
    for i in range(len(event_ids)):
        result += '%d,'%event_ids[i]
    return result

def remove_trend(trend_ids):
    if len(trend_ids) == 0:
        return
    wstr = util.get_in_where_str(trend_ids)
    query = 'delete from %s where %s in (%s)'%(resources.TREND_TABLE,resources.TREND_ID,wstr)
    print query
    conn = dao.get_connection()
    cur = conn.cursor()
    cur.execute(query)
    conn.commit()
    dao.free_connection(conn,cur)

def get_cut_index(event_ids,remove_event_set):
    event_num = len(event_ids)
    for i in range(len(event_ids)):
        index = event_num - 1 - i
        if (event_ids[index] not in remove_event_set):
            return index
    return -1
def get_event_ids_in_range(date1,date2):
    query = 'select %s,%s from %s where %s >= \'%s\' and %s <= \'%s\''%(resources.EVENT_ID,resources.EVENT_SIZE,resources.EVENT_TABLE,\
    resources.EVENT_DATE,date1,resources.EVENT_DATE,date2)
    print query
    conn = dao.get_connection()
    cur = conn.cursor()
    cur.execute(query)
    rows = cur.fetchall()
    dao.free_connection(conn,cur)
    result = dict()
    for row in rows:
        result[row[0]] = row[1]
    return result

def main():
    if (len(sys.argv) != 2):
        print 'usage: python re_run_trend.py date(only trend with last_date  < date is held)'
        sys.exit(1)
    date = sys.argv[1]
    recover_trends_before_date(date)
if __name__ == '__main__':
    main()
    
