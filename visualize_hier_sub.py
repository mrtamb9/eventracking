import dao,sys,codecs
import h_sub_util,util,event_dao,sub_resources, news_img,sub_util,deploy_hier_v2
import collections,resources
BLACK_DOMAIN_PATH = resources.BLACK_DOMAIN_PATH

def get_full_article_ids(records):
    result = []
    for record in records:
        aids = util.get_full_article_ids(record[sub_resources.ARTICLES])
        util.migrate_list2_to_list1(result,aids)
        record[sub_resources.ARTICLES] = aids
    return result

def get_black_domains():
    domain_set = set()    
    BLACK_DOMAIN_PATH
    f = open(BLACK_DOMAIN_PATH,'r')
    for line in f:
        temp = line.strip()
        domain_set.add(temp)
    f.close()
    return domain_set

BLACK_DOMAIN_SET = get_black_domains()

def get_group(records):
    result = dict()
    for record in records:
        gid = record[h_sub_util.GROUP_ID]
        sub_id = record[sub_resources.ID]
        if gid in result:
            result[gid].append(sub_id)
        else:
            result[gid] = [sub_id]
    for gid in result:
        result[gid] = sorted(result[gid])
    return result

def get_min_date(record_ids,record_dic):
    min_date = record_dic[record_ids[0]][sub_resources.START_DATE]
    for record_id in record_ids:
        temp = record_dic[record_id][sub_resources.START_DATE]
        if min_date > temp:
           min_date =  temp
    return min_date

def get_max_date(record_ids,record_dic):
    min_date = record_dic[record_ids[0]][sub_resources.LAST_DATE]
    for record_id in record_ids:
        temp = record_dic[record_id][sub_resources.LAST_DATE]
        if min_date < temp:
           min_date =  temp
    return min_date

def get_group_min_date(group_dic,record_dic):
    result = dict()
    for key in group_dic:
        result[key] = get_min_date(group_dic[key],record_dic)
    return result

def get_group_max_date(group_dic,record_dic):
    result = dict()
    for key in group_dic:
        result[key] = get_max_date(group_dic[key],record_dic)
    return result
def get_key_order(dic):
    result = []
    pairs = util.sort_dic_des(dic)
    for (v,k) in pairs:
        result.append(k)
    return result 

def filter_black_domains(records,art_dic):
    result = []
    for record in records:
        aids = record[sub_resources.ARTICLES]
        f_aids =filter_black_domain_for_aids(aids,art_dic)
        if len(f_aids) > 0:
            record[sub_resources.ARTICLES] = f_aids
            result.append(record)
    return result
        
def filter_black_domain_for_aids(aids,art_dic):
    result = []
    for aid in aids:
        domain = art_dic[aid][dao.DOMAIN]
        if domain not in BLACK_DOMAIN_SET:
            result.append(aid)
    return result

def get_real_time_group_sub(trend_id):
    date = util.get_today_str()
    records = h_sub_util.get_sub_from_today(deploy_hier_v2.REAL_TABLE_NAME,trend_id,date)
    return records

def merge_real_time_with_previous_records(r_records,records):
    if (len(r_records) == 0):
        return 
    r_sub_dic = util.get_dic_from_list(r_records,sub_resources.ID)
    sub_dic = util.get_dic_from_list(records,sub_resources.ID)
    for sub_id in r_sub_dic:
        if sub_id in sub_dic:
            merge_real_record2record(sub_dic[sub_id],r_sub_dic[sub_id])
        else:
            sub_dic[sub_id] = r_sub_dic[sub_id]
    return sub_dic.values()


def merge_real_record2record(record,r_record):
    util.migrate_list2_to_list1(record[sub_resources.ARTICLES],r_record[sub_resources.ARTICLES])
    record[sub_resources.LAST_DATE] = r_record[sub_resources.LAST_DATE]

def get_group_sub_from_date_2_date2(trend_id,table,date1,date2):
    r_records = get_real_time_group_sub(trend_id)
    r_aids = get_full_article_ids(r_records)
    records = h_sub_util.get_sub_records(trend_id,date1,date2,table)
    all_aids = get_full_article_ids(records)
    #merge sub_event real-time and all_aids
    if (len(r_records) > 0):
        util.migrate_list2_to_list1(all_aids,r_aids)
        records = merge_real_time_with_previous_records(r_records,records)
    #print 'all_ids: %s'%str(all_aids)
    articles = event_dao.get_article_urls(all_aids)
    art_dic = util.get_dic_from_list(articles,'id')
    records = filter_black_domains(records,art_dic)
    record_dic = util.get_dic_from_list(records,'id')
    group_dic = get_group(records)
    group_min_date = get_group_min_date(group_dic,record_dic)
    group_max_date = get_group_max_date(group_dic,record_dic)
    gid_order = get_key_order(group_min_date)
    #sub_data = []
    group_data = collections.OrderedDict()
    leng = len(gid_order)
    for i in range(leng):
        gid = gid_order[i]
        re_ids = group_dic[gid]
        temp_data = []
        title_agg = ''
        total_art_num = 0
        for sub_id in re_ids:
            sub = get_sub_data_from_record(record_dic[sub_id],art_dic)
            temp_data.append(sub)
            total_art_num += sub[sub_resources.COHERENCE]
            title_agg += '%s\n'%sub[sub_resources.TITLE]
        group_data[i] = dict()
        group_data[i]['sub_data'] = temp_data
        group_data[i][sub_resources.START_DATE] = str(group_min_date[gid])
        group_data[i][sub_resources.LAST_DATE] = str(group_max_date[gid])
        group_data[i][sub_resources.COHERENCE] = len(re_ids)
        group_data[i]['group_id'] = gid
        group_data[i]['sub_size'] = total_art_num
        #g_typical_id = temp_data[-1][sub_resources.ID]
        group_data[i]['title'] = title_agg#aggregate all titles#art_dic[g_typical_id][dao.TITLE]
    
    trend = event_dao.fetch_trend_by_id(trend_id)
    if not trend:
        trend = {'tag':'real-time',"start_date":util.get_today_str(),"last_date":util.get_today_str()}
    last_sub_id = group_dic[gid_order[-1]][-1]
    min_date = str(record_dic[last_sub_id][sub_resources.START_DATE].date())
    load_next_date = util.get_past_day(min_date,1)
    sub_util.pre_process_json_trend(trend)
    trend['load_date'] = load_next_date
    trend['sub_data'] = group_data
    return trend

def get_group_sub_with_max_date2(trend_id,table,max_date):
    max_date_time_db = h_sub_util.get_max_before_date(trend_id,table,max_date)
    print 'max_date before %s is %s'%(max_date,str(max_date_time_db))
    if not max_date_time_db:
        return None
    max_date_db = str(max_date_time_db.date())
    pre_date = util.get_past_day(max_date_db,2)
    return get_group_sub_from_date_2_date2(trend_id,table,pre_date,max_date_db)

def get_group_sub_first_time2(trend_id,table,number_date=2):
    max_date_time = h_sub_util.get_max_date(trend_id,table)
    max_date = str(max_date_time.date())
    pre_date = util.get_past_day(max_date,number_date)
    return get_group_sub_from_date_2_date2(trend_id,table,pre_date,max_date)

def get_sub_data_from_record(record,art_dic):
    result = dict()
    aids = record[sub_resources.ARTICLES]
    typical_id = aids[-1]
    result['title'] = art_dic[typical_id][dao.TITLE]
    result[sub_resources.IMG] = art_dic[typical_id][news_img.IMG_URL]
    result[sub_resources.COHERENCE] = len(aids)
    result[sub_resources.START_DATE] = str(record[sub_resources.START_DATE])
    result[sub_resources.LAST_DATE] = str(record[sub_resources.LAST_DATE])
    result[sub_resources.ARTICLES] = util.get_in_where_str(aids)
    result[sub_resources.ID] = record[sub_resources.ID]
    result[h_sub_util.GROUP_ID] = record[h_sub_util.GROUP_ID]
    result[sub_resources.TYPICAL_ART] = typical_id
    result['tag'] =''
    result['url'] = art_dic[typical_id][dao.URL]
    return result

def gen_html(trend_id,table,number_date = 3):
    max_date_time = h_sub_util.get_max_date(trend_id,table)
    max_date = str(max_date_time.date())
    pre_date = util.get_past_day(max_date,number_date)
    records = h_sub_util.get_sub_records(trend_id,pre_date,max_date,table)
    all_aids = get_full_article_ids(records)
    #print 'all_ids: %s'%str(all_aids)
    articles = event_dao.get_article_urls(all_aids)
    art_dic = util.get_dic_from_list(articles,'id')
    record_dic = util.get_dic_from_list(records,'id')
    group_dic = get_group(records)
    group_min_date = get_group_min_date(group_dic,record_dic)
    gid_order = get_key_order(group_min_date)
    res_str = get_html_str(gid_order,group_dic,record_dic,art_dic)
    return res_str

def get_html_str(gid_order,group_dic,record_dic,art_dic):
    result = '<html lang="vi" xmlns="http://www.w3.org/1999/xhtml"><meta http-equiv="Content-Type" content="text/html; charset=utf-8" />\n'
    result += '<table border="1">'
    count = 0
    for gid in gid_order:
        sub_ids = group_dic[gid]
        result += get_html_for_group(sub_ids,record_dic,art_dic,count)
        count += 1
    result += '</table>'
    return result

def get_html_for_group(sub_ids,record_dic,art_dic,order_number):
    leng = len(sub_ids)
    result = '<tr>\n<td rowspan="%d"> %d</td>\n<td> size %d</td>\n</tr>\n'%(leng+1,order_number,leng)
    for sub_id in sub_ids:
        result += '<tr>\n'
        result += get_html_for_sub(record_dic[sub_id],art_dic)
        result += '</tr>\n'
    return result
    

def get_html_for_sub(record,art_dic):
    aids = record[sub_resources.ARTICLES]
    #result = '<tr>\n<td rowspan="%d"> %d</td>\n<td> %d</td>\n</tr>\n'%(len(aids),order_number,len(aids))
    typ_aid = aids[-1]#record[sub_resources.TYPICAL_ART]
    create_time = str(art_dic[typ_aid][dao.CREATE_TIME])
    result = '<td>\n'
    result += '\t %s <a href="%s">%s </a> <br> \n'%(create_time,art_dic[typ_aid][dao.URL],art_dic[typ_aid][dao.TITLE])     
    result += '</td>\n'
    result += '<td>\n'
    
    for aid in aids:
        result += get_html_for_article(aid,art_dic)
    result += '</td>'
    return result
        
def get_html_for_article(aid,art_dic):
    #result = '<td>\n'
    result = ''
    #create_time = str(art_dic[aid][dao.CREATE_TIME])
    result += '\t <a href="%s">%s </a> <br> \n'%(art_dic[aid][dao.URL],art_dic[aid][dao.TITLE])           
    #result += '</td>\n'
    return result

def get_save_path(trend_id,number):
    return '/media/khaimai/F8C6516EC6512DDE/khai_folder/projects/trend_py/trendy_ploy1.0/annote/report/hier_%d_%d.html'%(trend_id,number)

def save_file(save_path,save_str):
    f = codecs.open(save_path,'w',encoding='utf8')
    f.write(save_str)
    f.close()
    print 'finish saving file: %s'%save_path

def main():
    if (len(sys.argv) != 3):
        print 'usage: python visualize_hier_sub.py trend_id number'
        sys.exit(1)
    trend_id = int(sys.argv[1])
    number = int(sys.argv[2])
    res_str = gen_html(trend_id,'hier_sub_v1',number)
    save_path = get_save_path(trend_id,number)
    save_file(save_path,res_str)
#if __name__ == '__main__':
#    main()
#get_group_sub(15161,'hier_sub_v1',3)
#gen_html(15161,'hier_sub_v1',3)
#get_group_sub_first_time(15161,'hier_sub_v1')
#import json
#obj = get_group_sub_from_date_2_date2(19235,'hier_sub_v2','2016-08-12','2016-08-12')
#res_str = json.dumps(obj)
#print res_str
#get_group_sub_first_time2(22359,'hier_sub_v2',3)