import sys
sys.path.insert(0,'..')
sys.path.insert(1,'../bolx')
import i_real_time,thread
import dao,util,time,news_time_stat,news_img

def find_img_on_day(date):
    date1 = '%s 00:00:00'%date
    date2 = '%s 23:59:59'%date
    #query = 'SELECT id from news_time_stat where \'%s\' <= get_time and \'%s\' >= get_time and no exists (select * from %s where )'
    query = 'SELECT id FROM %s as t1 where (\'%s\' <= get_time and \'%s\' >= get_time)\
    and (not exists(select * from %s as t2 where t2.news_id = t1.id))'%(news_time_stat.TABLE_NAME,date1,date2,news_img.TABLE_NAME)
    print query
    conn = dao.get_connection()
    cur = conn.cursor()
    cur.execute(query)
    rows = cur.fetchall()
    all_ids = []
    for row in rows:
        all_ids.append(row[0])
    print 'START FINDING IMAGES OF %d ARTICLES'%len(all_ids)
    if (len(all_ids) > 0):
        i_real_time.process(all_ids)
    else:
        print 'NO NEW IMAGE '
def main():
    if (len(sys.argv) != 1):
        print 'usage: python find_img_task.py'
        sys.exit(1)
    TODAY = util.get_today_str()
    while (True):
        day = util.get_today_str()
        if (day != TODAY):
            print 'finish ...'
            break
        find_img_on_day(TODAY)
        time.sleep(3)
    print 'finish ...'
if (__name__ == '__main__'):
    main()    
    
