import util, sys, os
import resources, event_dao, data_reader, detect_event_util, tree
import logging
import cluster_doc2
import dao
import time
import detec_trend, trend_util
import top_word_img
from sets import Set

# sys.path.insert(0, 'bolx')
from bolx import e_data_reader_entity

logger = logging.getLogger("detec_event_service")
logger.setLevel(logging.INFO)
util.set_log_file(logger, resources.GENERAL_LOG)
CONTENT_THESHOLD = 250  # remove text with short length ()


def detect_event_at1(day):
    logger.info('start detecting event on : %s' % day)
    t1 = time.time()
    (ids, corpus, domains) = data_reader.read_all_data(day)
    # day_data_path = trend_util.get_day_data_path(day)
    # logger.info('save data on day : %s'%day)
    # trend_util.save_object((ids,corpus,domains),day_data_path)
    catid_path = trend_util.get_catid_path(day)
    catids = data_reader.read_catid(catid_path)
    t2 = time.time()
    logger.info('time for reading data: %d' % (t2 - t1))
    logger.info('clustering: %d' % (len(ids)))
    # (clusters,centers,sizes) = cluster_doc2.cluster_docs_with_thresholds(clusters,centers,sizes,resources.DAY_THRESHOLDS,resources.CHANGE_THRESHOLD)
    bucket = util.get_bucket_from_dic(catids)
    merge_thresholds = [resources.DAY_THRESHOLDS[-1]]
    # cat_tree = data_reader.read_cat_tree(resources.CAT_TREE_FILE)
    # (clusters,centers,sizes) = cluster_doc2.merge_cluster_by_tree(cat_tree,bucket,corpus,resources.DAY_THRESHOLDS,merge_thresholds)
    (clusters, centers, sizes) = cluster_doc2.detect_event_with_buckets(bucket, corpus, resources.DAY_THRESHOLDS,
                                                                        merge_thresholds, online_mode=False)
    print clusters
    print '=========================================================='
    remove_count = detect_event_util.remove_small_events(domains, clusters, centers, sizes,
                                                         resources.REMOVE_EVENT_THRESHOLD)
    logger.info('number of removed events: %d' % remove_count)
    # save_data_temp(clusters,centers,sizes,day,corpus,domains)
    clusters = review_after_clustering(clusters, centers, sizes, day, corpus, domains)

    event_catids = save_events(clusters, corpus, day, catids, resources.INSERT_EVENT_DB)
    logger.info('finish storing events to event table %s' % day)
    return (centers, sizes, event_catids)


def get_confused_events_path(date):
    return resources.DAY_DATA_FOLDER + '/%s.dat' % date


def read_confused_trend_on_day(date):
    separate_event_path = get_confused_events_path(date)
    if (not os.path.exists(separate_event_path)):
        return Set()
    close_list = util.read_object(separate_event_path)
    trend_set = Set()
    all_eids = []
    for gset in close_list:
        for eid in gset:
            all_eids.append(eid)
    if (len(all_eids) > 0):
        events = event_dao.get_event_by_ids(all_eids)
        for event in events:
            trend_id = event[resources.EVENT_TREND_ID]
            trend_set.add(trend_id)
    return trend_set


def get_confused_trend(day, threshold_back=10):
    final_trend_set = Set()
    for i in range(1, threshold_back):
        temp_date = util.get_past_day(day, i)
        temp_trend_set = read_confused_trend_on_day(temp_date)
        for el in temp_trend_set:
            final_trend_set.add(el)
    return final_trend_set


def save_data_temp(clusters, centers, sizes, corpus, domains, catids, day):
    save_path = '/media/khaimai/F8C6516EC6512DDE/khai_folder/projects/trend_py/trendy_ploy1.0/test/imme/%s.dat' % day
    util.save_object(save_path, (clusters, centers, sizes, corpus, domains, catids))


def get_containing_set(list_set, element):
    for i in range(len(list_set)):
        if element in list_set[i]:
            return i
    return -1


def review_after_clustering(clusters, centers, sizes, day, corpus, domains):
    cluster_doc2.hierarchical_clustering(clusters, centers, sizes, resources.HIERARCHICAL_THRESHOLD)
    logger.info(str(clusters))
    root_node = detect_event_util.separate_clusers_smaller(clusters, centers, sizes, corpus, domains, logger=logger)
    if (root_node):
        logger.info('save root file')
        detect_event_util.save_tree(root_node, trend_util.get_tree_path(day))
    # logger.info(str(clusters))
    logger.info('after separate clusters to smaller: %d' % len(clusters))
    coherences = dict()
    for key in clusters:
        coherences[key] = detect_event_util.get_coherence_of_cluster(clusters[key], corpus, centers[key])
    # TO-DO
    return clusters


def save_events(clusters, corpus, day, catid_dic, insert_to_db=True):
    event_catids = dict()
    vocab_map = util.get_dictionary_map(trend_util.get_dictionary_path(day))
    cluster_ids = []
    aid_urls = []
    article_nums = []
    coherences = dict()
    if (insert_to_db):
        conn = dao.get_connection()
        cur = conn.cursor()
    num_record = 0
    sizes = dict()
    centers = dict()
    for key in clusters:
        l_center = detect_event_util.get_center(clusters[key], corpus)
        centers[key] = l_center
        (coherences[key], a_list) = detect_event_util.calculate_coherence(clusters[key], corpus,
                                                                          l_center)  # a_list is sorted articles
        clusters[key] = a_list
        top_img_path = trend_util.get_img_path(key, day)
        url_dict = detect_event_util.get_similar_urls(clusters[key], corpus, resources.IDENTICAL_DOC_THRESHOLD)

        url_values = url_dict.values()
        aid_temp = ''
        for url_cluster in url_values:
            for aid in url_cluster:
                aid_temp += '%d,' % aid
        aid_urls.append(aid_temp)
        l_size = len(clusters[key])
        sizes[key] = l_size
        if (insert_to_db):
            catid = detect_event_util.get_catid_of_cluster(clusters[key], catid_dic)
            event_catids[key] = catid
            insert_events(key, day, url_dict, l_center, l_size, top_img_path, coherences[key], cur, catid)
        num_record += 1
        debug_docs(clusters[key], l_center, day, key, vocab_map)
        cluster_ids.append(key)
        article_nums.append(l_size)
    if (insert_to_db):
        conn.commit()
        dao.free_connection(conn, cur)
    logger.info('Number of events on %s is %d' % (day, num_record))
    # save coherence
    coherence_path = trend_util.get_text_folder(day) + '/coherences.txt'
    detect_event_util.write_coherences(coherence_path, coherences, sizes)
    # print visualize 
    print 'gen html report for events on %s' % day
    report_html_path = trend_util.get_html_report_events_path(day)
    gen_html_report(cluster_ids, aid_urls, article_nums, day, report_html_path)
    # save context center and entity center
    logger.info('save context event_centers')
    trend_util.save_event_centers_context(centers, sizes, day)
    logger.info('save entity event_centers')
    # read entity data to compute center of events
    (ids2, corpus2, domains2, catids2) = e_data_reader_entity.read_all_data(day)
    (en_centers, en_sizes) = detect_event_util.get_entity_centers(clusters, corpus2)
    trend_util.save_event_centers_entity(en_centers, en_sizes, day)


def gen_html_report(cluster_ids, aid_urls, article_nums, day, save_path):
    event_imgs = []
    for i in range(len(cluster_ids)):
        img_path = trend_util.get_img_path(cluster_ids[i], day)
        # img_path = trend_util.get_img_url(img_path)
        event_imgs.append(img_path)
    util.gen_html_event(event_imgs, save_path, article_nums, aid_urls)


def debug_docs(cluster, center, day, key, vocab_map):
    img_day_folder = resources.IMG_FOLDER + '/' + day
    util.create_folder(img_day_folder)
    text_folder = trend_util.get_text_folder(day)
    util.create_folder(text_folder)
    detect_event_util.fetch_titles(cluster, text_folder + '/%d' % key)
    top_path = trend_util.get_text_top_path(key, day)
    cluster_doc2.visualize_centre(vocab_map, top_path, center)
    top_img_path = trend_util.get_img_path(key, day)
    top_word_img.generate_img(top_path, top_img_path)
    ##visualize top words 


def get_img_path_debug(event_id, date):
    return resources.IMG_FOLDER + '/' + date + '/top_' + str(event_id) + '.' + resources.TOP_IMG_EXTENSION


def insert_events(event_id, date, url_dicts, center, size, img_path, coherence, cur, catid):
    # first we need to sort ids by the closeness to centers
    article_ids_str = detect_event_util.get_article_ids_str(url_dicts)
    center_str = str(center)
    query = event_dao.create_insert_events_query(event_id, date, article_ids_str, center_str, size, img_path, coherence,
                                                 catid)
    logger.info('insert event: %d' % event_id)
    cur.execute(query)


def detec_events_by_day(day, update_trend):
    logger.info('start detecting events on day: %s' % day)
    t1 = time.time()
    detect_event_at1(day)
    t2 = time.time()
    logger.info('Finish detection events on day: %s' % day)
    stat_folder = trend_util.get_statistic_folder(day)
    f = open(stat_folder + '/detect_time.txt', 'a')
    f.write('%d\n' % (t2 - t1))
    f.close()
    if update_trend:
        detec_trend.compute_trend(day, False)


def main():
    if (len(sys.argv) != 4):
        print 'usage: python detec_event_service.py next/today/yesterday/day(input a specific day: 2015-06-20) num_day update_trend(True/False)'
        sys.exit(1)
    day = sys.argv[1]
    print 'day = %s' % day
    num_day = int(sys.argv[2])
    print 'num_day = %d' % num_day
    update_trend_str = sys.argv[3]
    print 'update_trend = %s' % update_trend_str
    update_trend = True
    if (update_trend_str == 'True'):
        update_trend = True
    elif (update_trend_str == 'False'):
        update_trend = False
    else:
        sys.exit(1)
    if (day == 'today'):
        print 'detect event on day: %s' % day
        today_str = util.get_today_str()
        detec_events_by_day(today_str, update_trend)
    elif (day == 'yesterday'):
        yesterday = util.get_yesterday_str()
        print 'detect event on yesterday: %s' % yesterday
        detec_events_by_day(yesterday, update_trend)
    elif (day == 'next'):
        current_day = event_dao.get_max_date(resources.EVENT_TABLE)
        next_day = util.get_ahead_day(current_day, 1)
        print 'detect event on: %s' % next_day
        detec_events_by_day(next_day, update_trend)
    else:
        num_day = int(sys.argv[2])
        print 'num_day = %d' % num_day
        for i in range(num_day):
            temp_day = util.get_ahead_day(day, i)
            print 'detecting events on : %s' % temp_day
            detec_events_by_day(temp_day, update_trend)


# detec_events_by_day('2015-10-23',False)
if __name__ == '__main__':
    main()
