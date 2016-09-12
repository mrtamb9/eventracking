#-*- coding: utf-8 -*-
import sys
sys.path.insert(0,'..')
import codecs,pymysql,dao
import sub_util,sub_resources,util,event_dao
def visualize_sub_trend(trend_id):
    print 'visualize trend_id: %d'%(trend_id)
    gen_html_description(trend_id)

def get_demo_data(trend_id,day_num = 15):
    sub_events = sub_util.get_sub_event_of_trend_wo_date(trend_id,day_num)
    print 'number of sub_events: %d'%(len(sub_events))
    result = list()
    imgs = []
    titles = []
    urls = []
    coherences = []
    start_dates = []
    last_dates = []
    a_sizes = []
    tags = []
    sub_ids = []
    for sub_event in sub_events:
        sub_ids.append(sub_event[sub_resources.ID])
        img = sub_event[sub_resources.IMG]
        imgs.append(img)
        title = sub_event[sub_resources.TITLE]
        titles.append(title)
        
        date_dic = util.str_to_dict(sub_event[sub_resources.ARTICLES])
        aids = get_all_article_ids(date_dic)
        url = get_url_for_showing_articles(aids)
        urls.append(url)
        coherences.append(sub_event[sub_resources.COHERENCE])
        
        start_date = sub_event[sub_resources.START_DATE]
        start_dates.append(start_date)
        last_date = sub_event[sub_resources.LAST_DATE]
        last_dates.append(last_date)
        a_sizes.append(sub_event[sub_resources.TOTAL_ARTICLE_NUM])
        tags.append(sub_event[sub_resources.TAG])
import numpy as np
def gen_html_description(trend_id,day_num = 10):
    sub_events = sub_util.get_sub_event_of_trend_wo_date(trend_id,day_num)
    print 'number of sub_events: %d'%(len(sub_events))
    imgs = []
    titles = []
    urls = []
    coherences = []
    start_dates = []
    last_dates = []
    a_sizes = []
    tags = []
    sub_ids = []
    for sub_event in sub_events:
        sub_ids.append(sub_event[sub_resources.ID])
        img = sub_event[sub_resources.IMG]
        imgs.append(img)
        title = sub_event[sub_resources.TITLE]
        titles.append(title)
        
        date_dic = util.str_to_dict(sub_event[sub_resources.ARTICLES])
        aids = get_all_article_ids(date_dic)
        url = get_url_for_showing_articles(aids)
        urls.append(url)
        coherences.append(sub_event[sub_resources.COHERENCE])
        
        start_date = sub_event[sub_resources.START_DATE]
        start_dates.append(start_date)
        last_date = sub_event[sub_resources.LAST_DATE]
        last_dates.append(last_date)
        a_sizes.append(sub_event[sub_resources.TOTAL_ARTICLE_NUM])
        tags.append(sub_event[sub_resources.TAG])
    save_path = sub_util.get_trend_release_path(trend_id)
    
    gen_html_file_wo_debug(sub_ids,imgs,titles,urls,coherences,start_dates,last_dates,a_sizes,tags,save_path)
    save_path = sub_util.get_sub_trend_descripton_path(trend_id)
    gen_html_file(sub_ids,imgs,titles,urls,coherences,start_dates,last_dates,a_sizes,tags,save_path)

def get_url_for_showing_articles(aids):
    idstr = util.list_to_str(aids)
    return 'http://analytics.admicro.vn/bigdata/vingo/article-event/details/61434937?article_ids=%s'%idstr

def get_all_article_ids(date_dic):
    dates = util.get_sorted_dates(date_dic.keys())
    ids = []
    for i in range(len(dates)):
        date = dates[i]
        date_ids = date_dic[date]
        for aid in date_ids:
            ids.append(aid)
    return ids

def datetime_to_int(dt_time):
    return dt_time.second + dt_time.minute*60 + dt_time.hour*3600 + dt_time.day*3600*24 + dt_time.month*3600*24*31 + dt_time.year*3600*23*31*12

def get_date_order(dates):
    int_dates = []
    for i in range(len(dates)):
        temp = datetime_to_int(dates[i])
        int_dates.append(temp)
    orders = argsort(int_dates)
    return orders

def argsort(seq):
    return sorted(range(len(seq)), key=seq.__getitem__)     
def get_arg_sort_dates(datetimes):
    dates = list(datetimes)
    size = len(dates)
    index = [i for i in range(size)]
    print index
    leng = len(dates)         
    for i in range(leng-1):
        for j in range(i+1,leng):
            if (dates[i] < dates[j]):
                temp = dates[i]
                dates[i] = dates[j]
                dates[j] = temp
                t_index = index[i]
                index[i] = index[j]
                index[j] = t_index
    return index
                

def gen_html_file_wo_debug(sub_ids,imgs,titles,urls,coherences,min_dates,last_dates,a_sizes,tags,save_path):
    html = get_html_template()
    #date_order = get_arg_sort_dates(min_dates)
    leng = len(min_dates)
    count = 0
    for i in range(len(imgs)):
        index = leng - 1-i
        #index = i
        print 'index: %d start_date = %s'%(index,str(min_dates[index]))
        if (a_sizes[index] > 2):
            count += 1
            img = imgs[index]
            title = titles[index]
            url = urls[index]
            part = '<tr>\n\
            <td>\n\
            %d\n\
            </td>\n\
            <td>\n\
            <a href = "%s">\n\
            <img src="%s" style="width: 200px;">\n\
            </a> \n\
            </td>  \n\
            <td>\n\
            %s \n\
            </td>\n\
            <td>\n\
            %s\n\
            </td>\n\
            <td>\n\
            %s\n\
            </td>\n\
            </tr>\n\
            <tr>'%(count,url,img,title,str(min_dates[index]),tags[index])
            html += part
     
    html += ' </tbody>\n\
    </table>'
    f = codecs.open(save_path,'w',encoding='utf8')
    f.write(html)
    f.close()

def gen_html_file(sub_ids,imgs,titles,urls,coherences,min_dates,last_dates,a_sizes,tags,save_path):
    html = u'<html lang="vi"> <meta http-equiv="Content-Type" content="text/html; charset=utf-8" /><body> <table  BORDER="1">'
    #print min_dates
    #date_order = get_arg_sort_dates(min_dates)
    #print min_dates
    leng = len(min_dates)
    count = 0
    for i in range(len(imgs)):
        index = leng - 1-i
        print 'index: %d start_date = %s'%(index,str(min_dates[index])) 
        if (a_sizes[index] > 0):
            count += 1
            img = imgs[index]
            title = titles[index]
            url = urls[index]
            part = '<tr>\
            <td>\
            <h1>%d </h1>\
            </td>\
            <td>\
            <h1>%d </h1>\
            </td>\
            <td>\
            <a href = "%s">\
            <img src="%s" style="height: 200px;">\
            </a> \
            </td>  \
            <td>\
            <h1>%s </h1>\
            </td>\
            <td>\
            <h1>coherence=%f </h1>\
            </td>\
            <td>\
            <h1>sizes=%d </h1>\
            </td>\
            <td>\
            <h1>start=%s </h1>\
            </td>\
            <td>\
            <h1>last=%s </h1>\
            </td>\
            <td>\
            <h1>%s </h1>\
            </td>\
            </tr>\
            <tr>'%(count,sub_ids[index],url,img,title,coherences[index],a_sizes[index],str(min_dates[index]),str(last_dates[index]),tags[index])
            html += part
     
    html += '</body></html>'
    f = codecs.open(save_path,'w',encoding='utf8')
    f.write(html)
    f.close()

def get_html_template():
    html = '<table class="table table-hover">\n\
    <thead>\n\
        <tr>\n\
            <th class="text-center">STT</th>\n\
            <th class="text-center">IMG</th>\n\
            <th class="text-center">Title</th>\n\
            <th class="text-center">Start_Date</th>\n\
            <th class="text-center">Size</th>\n\
        </tr>\n\
    </thead>\n\
      <tbody>'
    
    return html

def gen_html_description2(trend_id,date1,date2,table):
    sub_events =  get_trend_in_dates(trend_id,date1,date2,table) #sub_util.get_sub_event_of_trend_wo_date(trend_id,day_num)
    print 'number of sub_events: %d'%(len(sub_events))
    all_aids = []
    for sub_event in sub_events:
        update_sub_event_within_range(sub_event,date1,date2)
        temp_ids = sub_event['aids']
        util.migrate_list2_to_list1(all_aids,temp_ids)
    articles = event_dao.get_article_urls(all_aids)
    art_dic = util.get_dic_from_list(articles,dao.ID)
    st_dates = []
    for i in range(len(sub_events)):
        sub_event = sub_events[i]
        aids = sub_event['aids']
        s_aids = sort_aid_by_dates(aids,art_dic)
        start_datetime = art_dic[s_aids[-1]][dao.CREATE_TIME]
        end_datetime = art_dic[s_aids[0]][dao.CREATE_TIME]
        sub_event[sub_resources.START_DATE] = start_datetime
        sub_event[sub_resources.LAST_DATE]= end_datetime
        sub_event[sub_resources.TYPICAL_ART] = s_aids[0]
        typical_aid = s_aids[0]
        sub_event[sub_resources.IMG] = art_dic[typical_aid]['url_image']
        sub_event[sub_resources.TITLE] = art_dic[typical_aid]['title']
        sub_event[sub_resources.TOTAL_ARTICLE_NUM] = len(s_aids)
        sub_event[sub_resources.TAG] = 'k'
        sub_event[sub_resources.COHERENCE] = 1
        st_dates[i] = start_datetime
    date_array = np.array(st_dates)
    order = np.argsort(date_array)

    imgs = []
    titles = []
    urls = []
    coherences = []
    start_dates = []
    last_dates = []
    a_sizes = []
    tags = []
    sub_ids = []
    for i in range(len(sub_events)):
        sub_event = sub_events[order[i]]
        sub_ids.append(sub_event[sub_resources.ID])
        img = sub_event[sub_resources.IMG]
        imgs.append(img)
        title = sub_event[sub_resources.TITLE]
        titles.append(title)
        
        #date_dic = util.str_to_dict(sub_event[sub_resources.ARTICLES])
        aids =  sub_event['aids'] #get_all_article_ids(date_dic)
        url = get_url_for_showing_articles(aids)
        urls.append(url)
        coherences.append(sub_event[sub_resources.COHERENCE])
        
        start_date = sub_event[sub_resources.START_DATE]
        start_dates.append(start_date)
        last_date = sub_event[sub_resources.LAST_DATE]
        last_dates.append(last_date)
        a_sizes.append(sub_event[sub_resources.TOTAL_ARTICLE_NUM])
        tags.append(sub_event[sub_resources.TAG])
    save_path = get_save_path_for_trend(trend_id,table)#sub_util.get_trend_release_path(trend_id)
    gen_html_file_wo_debug(sub_ids,imgs,titles,urls,coherences,start_dates,last_dates,a_sizes,tags,save_path)
#    save_path = sub_util.get_sub_trend_descripton_path(trend_id)
#    gen_html_file(sub_ids,imgs,titles,urls,coherences,start_dates,last_dates,a_sizes,tags,save_path)

def get_save_path_for_trend(trend_id,table_name):
    return '/media/khaimai/F8C6516EC6512DDE/khai_folder/projects/trend_py/trendy_ploy1.0/annote/report/%d_%s.html'%(trend_id,table_name)

def get_trend_in_dates(trend_id,date1,date2,table):
    query = 'select * from %s where (%s >= \'%s 00:00:00\') and (%s < \'%s 00:00:00\')'%(table,sub_resources.LAST_DATE,date1\
    ,sub_resources.START_DATE,date2)
    print query
    conn = dao.get_connection()
    cur = conn.cursor(pymysql.cursors.DictCursor)
    cur.execute(query)
    rows = cur.fetchall()
    dao.free_connection(conn,cur)
    return rows

def update_sub_event_within_range(sub_event,date1,date2):
    art_dic = util.str_to_dict(sub_event[sub_resources.ARTICLES])
    merge_ids = []
    for date in art_dic:
        lower = util.compare_dates(date1,date)
        higher = util.compare_dates(date2,date)
        if ((lower == util.SMALLER) or (lower == util.EQUAL)) and (higher == util.GREATER):
            util.migrate_list2_to_list1(merge_ids,art_dic[date])
    sub_event['aids'] = merge_ids

def sort_aid_by_dates(aids,art_dic):
    date_dic = dict()
    for aid in aids:
        date_dic[aid] = art_dic[aid][dao.CREATE_TIME]
    pairs = util.sort_dic_des(date_dic)
    s_aids = []#[0] is max 
    for (v,k) in pairs:
        s_aids.append(k)
    return s_aids

def filter_sub_events(rows,date1,date2):
    result = dict()
    for i in range(len(rows)):
        sub = rows[i]
        art_dic = util.str_to_dict(sub[sub_resources.ARTICLES])
        merge_ids = []
        for date in art_dic:
            lower = util.compare_dates(date1,date)
            higher = util.compare_dates(date2,date)
            if ((lower == util.SMALLER) or (lower == util.EQUAL)) and (higher == util.GREATER):
                util.migrate_list2_to_list1(merge_ids,art_dic[date])
        result[sub[sub_resources.ID]] = merge_ids
    return result

def main():
    if (len(sys.argv) != 2):
        print 'usage: python sub_visualize.py trend_id/all'
        sys.exit(1)
    para = sys.argv[1]
    if (para != 'all'):
        trend_id = int(para)
        visualize_sub_trend(trend_id)
    else:
        trend_list = event_dao.get_all_trend_id()
        for trend_id in trend_list:
            try:
                visualize_sub_trend(trend_id)
            except:
                print 'error visualizing subevents in %d'%trend_id
def gen_html_report(trend_id,date1,date2):
    gen_html_description2(trend_id,date1,date2,'new_sub_trend_temp')
    gen_html_description2(trend_id,date1,date2,'new_sub_v2')
    gen_html_description2(trend_id,date1,date2,'new_sub_v3')
    gen_html_description2(trend_id,date1,date2,'sub_event_binh')
#if __name__ == '__main__':
#    main()
#visualize_sub_trend(673)
gen_html_report(16394,'2016-06-14','2016-06-15')
print 'finished ...'