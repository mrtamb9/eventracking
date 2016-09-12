import resources,util,pickle
def get_corpus_size_path(day):
    return resources.DAYS_PATH+'/'+day+'/'+resources.CORPUS_SIZE_FILE_NAME

def get_dictionary_path(day):
    return resources.DAYS_PATH+'/'+day+'/'+resources.DICTIONARY_FILE_NAME

def get_dictionary_temp_path(day):
    return resources.DAYS_PATH+'/'+day+'/'+resources.DICTIONARY_FILE_TEMP_NAME
def get_data_folder(day):
    return resources.TEMPARORY_DATA+'/'+day

def save_corpus_size(corpus_size,day):
    save_path = get_corpus_size_path(day)
    f = open(save_path,'w')
    f.write('%d'%corpus_size)
    f.close()

def get_trend_top_img_path(trend_id):
    return get_trend_folder(trend_id)+'/top.'+resources.TOP_IMG_EXTENSION

def get_event_centers_context_path(day):
    return resources.TEMP_CENTER_FOLDER+'/'+day+'.dat'

def get_event_centers_entity_path(day):
    return resources.TEMP_CENTER_FOLDER+'/'+day+'_entity.dat'
def save_event_centers_context(centers,sizes,date):
    save_path = get_event_centers_context_path(date)
    save_object((centers,sizes),save_path)

def save_event_centers_entity(centers,sizes,date):
    save_path = get_event_centers_entity_path(date)
    save_object((centers,sizes),save_path)

def get_all_event_centers_entity(date):
    fpath = get_event_centers_entity_path(date)
    return read_object(fpath)

def get_all_event_centers_context(date):
    fpath = get_event_centers_context_path(date)
    return read_object(fpath)

def get_trend_folder(trend_id):
    return resources.TREND_FOLDER + '/'+str(trend_id)

def get_event_top_img_path(event_id,date_str):
    return resources.IMG_FOLDER+'/'+date_str+'/top_'+str(event_id)+resources.TOP_IMG_EXTENSION

def get_img_path(event_id,date):
    return resources.IMG_FOLDER+'/'+date+'/top_'+str(event_id)+'.'+resources.TOP_IMG_EXTENSION

def get_statistic_folder(day):
    res = resources.STAT_FOLDER+'/'+day   
    return res

def get_html_report_events_path(day):
    return resources.IMG_FOLDER+'/'+day+'/report.html'

def get_trend_img_path(trend_id):
    trend_folder = get_trend_folder(trend_id)
    return trend_folder+'/img.'+resources.TOP_IMG_EXTENSION

def get_text_top_path(event_id,day):
    text_folder = get_text_folder(day)
    return text_folder+'/top_%d'%event_id

def get_clusters_img_description(day):
    return resources.IMG_FOLDER+'/'+day+'/description.'+resources.TOP_IMG_EXTENSION
def get_tree_path(day):
    return resources.TEMP_CENTER_FOLDER+'/tree_%s'%day

def get_catid_folder(day):
    return resources.CATID_FOLDER+'/'+day
def get_catid_path(day):
    return resources.CATID_FOLDER+'/'+day+'/catid.dat'
def get_text_folder(day):
    return resources.TEXT_FOLDER+'/'+day
def get_img_url(img_url):
    img_path = img_url.replace('/var/www/html/','http://bigdataml-trending.admicro.vn/')
    return img_path
def get_trend_img_url(trend_id):
    trend_img = get_trend_top_img_path(trend_id)
    trend_img = get_img_url(trend_img)
    return trend_img
def handle_event_object(row):
    date = str(row[resources.EVENT_DATE])
    row[resources.EVENT_DATE] = date
    del row[resources.EVENT_CENTER]
    img_path = row[resources.EVENT_TOPWORD_IMG]
    img_path = get_img_url(img_path)
    row[resources.EVENT_TOPWORD_IMG] = img_path
    #row[resources.EVENT_ARTICCLE_IDS] = util.convert_dic_article_to_list(row[resources.EVENT_ARTICCLE_IDS])
    aids = util.get_full_article_ids(row[resources.EVENT_ARTICCLE_IDS])
    row[resources.EVENT_ARTICCLE_IDS] = util.get_in_where_str(aids)

def save_object(obj,save_path):
    with open(save_path,'wb') as f:
        pickle.dump(obj, f)

def read_object(fpath):
    f = open(fpath,'r')
    obj = pickle.load(f)
    f.close()
    return obj


def save_trend_last_size(trend_id,last_date,last_event_size):
    last_folder = get_trend_last_size_folder(trend_id)
    util.create_folder(last_folder)
    f = open(get_trend_last_size_path(trend_id,last_date),'w')
    f.write('%d'%last_event_size)
    f.close()
def get_trend_last_size(trend_id,last_date):
    fpath = get_trend_last_size_path(trend_id,last_date)
    f = open(fpath,'r')
    size = int(f.readline())
    f.close()
    return size

def get_trend_center_context(trend_id,last_date):
    fpath = get_trend_context_center_path(trend_id,last_date)
    return read_object(fpath)

def get_trend_center_entity(trend_id,last_date):
    fpath = get_trend_entity_center_path(trend_id,last_date)
    return read_object(fpath)

def save_trend_center_entity(center,trend_id,last_date):
    save_path = get_trend_entity_center_path(trend_id,last_date)
    save_object(center,save_path)

def save_trend_center_context(center,trend_id,last_date):
    save_path = get_trend_context_center_path(trend_id,last_date)
    save_object(center,save_path)

def get_trend_context_center_folder(trend_id):
    return get_trend_folder(trend_id)+'/context'

def get_trend_context_center_path(trend_id,last_date):
    return get_trend_context_center_folder(trend_id)+'/'+last_date+'.dat'

def get_trend_entity_center_folder(trend_id):
    return get_trend_folder(trend_id)+'/entity'

def get_trend_entity_center_path(trend_id,last_date):
    return get_trend_entity_center_folder(trend_id)+'/'+last_date+'.dat'

def get_trend_last_size_folder(trend_id):
    return get_trend_folder(trend_id)+'/last_event_size'
    
def get_trend_last_size_path(trend_id,last_date):
    return get_trend_last_size_folder(trend_id)+'/'+last_date+'.dat'
def get_day_data_path(date):
    return resources.DAY_DATA_FOLDER+'/'+date+'.dat'
def get_cluster_result_trend_folder(trend_id):
    return resources.TREND_FOLDER + '/%d/cluster_result'%trend_id

def get_cluster_result_path_trend(trend_id,date):
    cluster_folder = get_cluster_result_trend_folder(trend_id)
    return cluster_folder + '/%s.dat'%date
def get_cluster_result_current_trend_path(trend_id):
    return resources.TREND_FOLDER + '/%d/previous_cluster_result.dat'%trend_id


