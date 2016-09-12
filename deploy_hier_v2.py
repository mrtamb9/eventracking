import util, sub_data_reader, cluster_doc2, event_dao, resources, trend_util, os, dao, h_sub_util, sub_resources, sys
import logging, codecs

TABLE_NAME = 'hier_sub_v2'
REAL_TABLE_NAME = 'hier_sub_real'
logger = logging.getLogger("deploy_hier_v2.py")
logger.setLevel(logging.INFO)
util.set_log_file(logger, resources.LOG_FOLDER + '/deploy_hier_v2.log')
util.set_log_console(logger)
BINARY_THRESHOLD = 0.5
IDENTICAL_THRESHOLD = 0.85
CONNECT_THRESHOLD = 0.13
INTER_CONNECT_THRESHOLD = 0.25
INTERVAL_MINUS = 0.00
LOW_DISTANCE = 0.15
QUALITY_THRESHOLD = 0.10
# MODEL_PATH = '/media/khaimai/F8C6516EC6512DDE/khai_folder/projects/trend_py/trendy_ploy1.0/annote/sim_vec/model/10.dat'
# CLUSTER_DIC_TEMP_FOLDER = '/media/khaimai/F8C6516EC6512DDE/khai_folder/projects/trend_py/trendy_ploy1.0/annote/cluster_dic/h_v2'
CLUSTER_DIC_TEMP_FOLDER = resources.DATA_FOLDER + '/sub_event/hier_dic_v2'
# CONNECT_MODEL_PATH = '/media/khaimai/F8C6516EC6512DDE/khai_folder/projects/trend_py/trendy_ploy1.0/annote/sim_vec/connect_model/40.dat'
util.create_folder(CLUSTER_DIC_TEMP_FOLDER)
# CLASSIFIER = util.read_object(MODEL_PATH)
# logger.info('successfully load classifier for in-day classifier')
# CONNECT_CLASSIFIER = util.read_object(CONNECT_MODEL_PATH)
# logger.info('successfully load classifier for connected-day classifier')
TREND_IDS = dict()
TREND_IDS[11172] = sorted([66265161, 66267213])
TREND_IDS[12596] = sorted([67336707, 67357034])
TREND_IDS[9468] = sorted([66011159, 65968207])
TREND_IDS[11172] = sorted([66267213, 66265161])
TREND_IDS[4636] = sorted([66516313, 66266154])
TREND_IDS[4715] = sorted([67324099, 67225659])
TREND_IDS[4649] = sorted([64422063, 64393272])
TREND_IDS[12281] = sorted([67315715, 67306008])
TREND_IDS[10425] = sorted([65783832, 65831958])
TREND_IDS[10865] = sorted([65699938, 65622016])
TREND_IDS[11201] = sorted([67178526, 67338293])
import time

TREND_FILDER_IDS = []  # [16394,15091,13118]if we want to run only several trends, input trend_ids to here


def get_bin_sim(v1, v2):
    if len(v1) == 0 or len(v2) == 0:
        return [0.0, 0.0, 0.0]
    x1 = util.get_bin_jaccard(v1, v2)
    x2 = util.get_bin_sim_max(v1, v2)
    x3 = util.get_bin_sim_min(v1, v2)
    return [x1, x2, x3]


def get_tfidf_contain_sim(v1, v2):
    score = 0.0
    for key in v1:
        if key in v2:
            score += v1[key] * v1[key]
    return score


def update_df_dic(df_dic, doc_count):
    for w in doc_count:
        if w in df_dic:
            df_dic[w] += 1
        else:
            df_dic[w] = 1


def get_mixed_title_content(tit, con, alpha=0.35, min_context=0.15, max_tit=0.8, min_title=0.28):
    score = (tit * alpha + con * (1 - alpha))
    result = score
    # return score
    if tit < min_title:
        result = 0.2
    if (tit > max_tit and con > min_context):
        result = max(tit, score)
    return result


class Iden_Comparator:
    def __init__(self, trend_comp):
        self.trend_comp = trend_comp

    def get_similarity(self, aid1, aid2):
        tit = self.trend_comp.get_title_sim(self.trend_comp.corpus[aid1], self.trend_comp.corpus[aid2],
                                            self.trend_comp.corpus_size)
        con = self.trend_comp.get_conf_sim(self.trend_comp.corpus[aid1], self.trend_comp.corpus[aid2],
                                           self.trend_comp.corpus_size)  # self.trend_comp.get_expan_sim(self.trend_comp.corpus[aid1],self.trend_comp.corpus[aid2],self.trend_comp.corpus_size)
        # score = get_mixed_title_content(tit,con)
        if tit > 0.9:
            return 1.0
        score = (tit + con) / 2.0
        return score


class Content_Focus_Comparator:
    def __init__(self, trend_comp, alpha=0.80):
        self.trend_comp = trend_comp
        self.alpha = alpha

    def get_similarity(self, aid1, aid2):
        tit = self.trend_comp.get_title_sim(self.trend_comp.corpus[aid1], self.trend_comp.corpus[aid2],
                                            self.trend_comp.corpus_size)
        con = self.trend_comp.get_expan_sim(self.trend_comp.corpus[aid1], self.trend_comp.corpus[aid2],
                                            self.trend_comp.corpus_size)
        score = con * self.alpha + tit * (1 - self.alpha)
        return score


class Trend_Comparator:
    def __init__(self, corpus):
        self.tit_df = dict()
        self.tit_word_df = dict()
        self.expand_title_df = dict()
        self.con_df = dict()
        self.tit_alpha = 0.8
        self.mix_alpha = 0.65
        self.corpus = corpus
        self.corpus_size = 0
        self.domain_dic = dict()
        if corpus:
            self.update_new_doc_with(corpus)
            # self.domain_dic = get_domain_dic(self.corpus)
            # self.corpus_size = len(corpus)
        else:
            self.corpus = dict()

    def get_Iden_comp(self):
        result = Iden_Comparator(self)
        return result

    def update_new_doc_with(self, corpus):
        for aid in corpus:
            self.corpus[aid] = corpus[aid]
            update_df_dic(self.expand_title_df, corpus[aid].expan_title)
            update_df_dic(self.tit_df, corpus[aid].title)
            update_df_dic(self.tit_word_df, corpus[aid].title_word)
            update_df_dic(self.con_df, corpus[aid].con_tf)
            self.corpus_size += 1
            doc = corpus[aid]
            domain = doc.domain
            self.domain_dic[aid] = domain

    def get_title_sim(self, doc1, doc2, corpus_size):
        tit_vec1 = util.get_tf_idf_rep(doc1.title, self.tit_df, corpus_size, 2.0)
        tit_vec2 = util.get_tf_idf_rep(doc2.title, self.tit_df, corpus_size, 2.0)
        tit_sim = util.get_similarity(tit_vec1, tit_vec2)

        tit_word_vec1 = util.get_tf_idf_rep(doc1.title_word, self.tit_word_df, corpus_size, 2.0)
        tit_word_vec2 = util.get_tf_idf_rep(doc2.title_word, self.tit_word_df, corpus_size, 2.0)
        tit_word_sim = util.get_similarity(tit_word_vec1, tit_word_vec2)
        tit = tit_sim * (1 - self.tit_alpha) + self.tit_alpha * tit_word_sim
        return tit

    def get_expan_sim(self, doc1, doc2, corpus_size):
        v1 = util.get_tf_idf_rep(doc1.expan_title, self.expand_title_df, corpus_size, 2.0)
        v2 = util.get_tf_idf_rep(doc2.expan_title, self.expand_title_df, corpus_size, 2.0)
        con = util.get_similarity(v1, v2)
        return con

    def get_conf_sim(self, doc1, doc2, corpus_size):
        v1 = util.get_tf_idf_rep(doc1.con_tf, self.con_df, corpus_size, 2.0)
        v2 = util.get_tf_idf_rep(doc2.con_tf, self.con_df, corpus_size, 2.0)
        con = util.get_similarity(v1, v2)
        return con

    def get_similarity_connect(self, doc1, doc2, corpus_size):
        tit = self.get_title_sim(doc1, doc2, corpus_size)
        con = self.get_expan_sim(doc1, doc2, corpus_size)
        score = self.mix_alpha * tit + (1 - self.mix_alpha) * con  # get_mixed_title_content(tit,con)
        # print 'TITLE = %f; CON = %f'%(tit,con)
        return score

    def get_similarity(self, aid1, aid2):
        return self.get_similarity_connect(self.corpus[aid1], self.corpus[aid2], self.corpus_size)


def get_domain_dic(corpus):
    result = dict()
    for aid in corpus:
        doc = corpus[aid]
        domain = doc.domain
        result[aid] = domain
    return result


def get_full_article(aids, iden_dic):
    result = []
    for aid in aids:
        util.migrate_list2_to_list1(result, iden_dic[aid])
    return result


def filter_black_domain_from_dic(doc_dic, domain_dic):
    result = dict()
    for key in doc_dic:
        doc_cluster = doc_dic[key]
        new_key = get_good_key(doc_cluster, domain_dic)
        result[new_key] = doc_cluster
    return result


def get_good_key(cluster, domain_dic):
    new_key = cluster[0]
    for aid in cluster:
        if domain_dic[aid] != 'xaluan.com':
            return new_key
    return new_key


def decompose_event(comparator, comparator2, all_ids, max_group_id=0):
    iden_comp = comparator.get_Iden_comp()
    iden_dic = cluster_doc2.cluster_doc_with_comparator(all_ids, iden_comp, IDENTICAL_THRESHOLD)
    iden_dic = filter_black_domain_from_dic(iden_dic, comparator.domain_dic)
    retain_ids = iden_dic.keys()
    print 'number of identical articles: %d' % (len(all_ids) - len(retain_ids))
    (cluster_dic, group_dic) = decompose_articles(retain_ids, comparator, comparator2)
    for key in group_dic:
        group_dic[key] += max_group_id + 1
    return (cluster_dic, group_dic)


def get_bridging_articles(all_ids, comparator, low_threshold, high_threshold):
    return dict()
    print 'get bridge_dic with thresholds: %f, %f' % (low_threshold, high_threshold)
    art_num = len(all_ids)
    neighbor_dic = dict()
    for aid in all_ids:
        neighbor_dic[aid] = []
    for i in range(art_num - 1):
        for j in range(i + 1, art_num):
            id1 = all_ids[i]
            id2 = all_ids[j]
            temp = comparator.get_similarity(id1, id2)
            if temp > high_threshold:
                neighbor_dic[id1].append(id2)
                neighbor_dic[id2].append(id1)
    bridge_dic = dict()
    for aid in neighbor_dic:
        n_ids = neighbor_dic[aid]
        if len(n_ids) > 1:
            pairs = get_pair_smaller_than(n_ids, comparator, low_threshold)
            if len(pairs) > 0:
                # print 'aid = %d'%aid
                bridge_dic[aid] = pairs
                # print '%s'%str(pairs)
    return bridge_dic


def get_pair_smaller_than(aids, comparator, low_threshold):
    leng = len(aids)
    pairs = []
    for i in range(leng - 1):
        for j in range(i + 1, leng):
            id1 = aids[i]
            id2 = aids[j]
            temp = comparator.get_similarity(id1, id2)
            if temp < low_threshold:
                pairs.append((id1, id2))
    return pairs


def get_min_distance(aids, comparator):
    leng = len(aids)
    min_distance = 1.0
    min_pair = None
    for i in range(leng - 1):
        for j in range(i + 1, leng):
            temp = comparator.get_similarity(aids[i], aids[j])
            if (temp < min_distance):
                min_distance = temp
                min_pair = (aids[i], aids[j])
    return (min_distance, min_pair)


def decide_separation(min_distance, quality_threshold=QUALITY_THRESHOLD):
    if min_distance < quality_threshold:
        return True
    return False


def separate_cluster(cluster_dic, comparator):
    result = dict()
    group = dict()
    group_count = 0
    for key in cluster_dic:
        aids = cluster_dic[key]
        (min_distance, min_pair) = get_min_distance(aids, comparator)
        # coherence = get_coherence_of_cluster(aids,comparator)
        separate = decide_separation(min_distance)
        if not separate:  # false
            print 'RETAIN: %s with min_score = %f' % (str(aids), min_distance)
            result[key] = aids
            group[key] = group_count
        else:
            print 'SEPARATE: %s' % str(aids)
            temp_dic = seprate_until_good_cluster(aids,
                                                  comparator)  # separate_into_two(aids,comparator)#separate_cluster_to_smaller(aids,comparator,connect_threshold)
            print 'separate result: %s' % str(temp_dic)
            for sid in temp_dic:
                result[sid] = temp_dic[sid]
                group[sid] = group_count
                group[sid] = group_count
        group_count += 1
    return (result, group)


def get_bad_good_cluster(cluster_dic, comparator):
    good_cluster = []
    bad_cluster = []
    for key in cluster_dic:
        (min_dis, pair) = get_min_distance(cluster_dic[key], comparator)
        if decide_separation(min_dis):
            bad_cluster.append(key)
            print 'bad cluster score = %f ; %d: %s ' % (min_dis, key, str(cluster_dic[key]))
        else:
            good_cluster.append(key)
    return (good_cluster, bad_cluster)


def seprate_until_good_cluster(cluster, comparator):
    cluster_list = [cluster]
    result = dict()
    while (True):
        if (len(cluster_list) == 0):
            break
        temp = cluster_list.pop()
        res_dic = separate_into_two(temp, comparator)
        (good_cluster, bad_cluster) = get_bad_good_cluster(res_dic, comparator)
        for key in good_cluster:
            result[key] = res_dic[key]
        for key in bad_cluster:
            cluster_list.append(res_dic[key])
    return result


def separate_into_two(aids, comparator):
    (min_distance, min_pair) = get_min_distance(aids, comparator)
    print 'pair with lowest similarity: %s = %f' % (str(min_pair), min_distance)
    aid1 = min_pair[0]
    aid2 = min_pair[1]
    result = dict()
    result[aid1] = [aid1]
    result[aid2] = [aid2]
    for aid in aids:
        if aid != aid1 and aid != aid2:
            dis1 = comparator.get_similarity(aid1, aid)
            dis2 = comparator.get_similarity(aid2, aid)
            if dis1 > dis2:
                result[aid1].append(aid)
            else:
                result[aid2].append(aid)
    return result


def decompose_articles(aids, comparator1, comparator2):
    connect_threshold = CONNECT_THRESHOLD  # get_connect_threshld(aver_sim)
    cluster_dic = cluster_doc2.hierarcical_cluster_doc_with_comparator(aids, comparator1, connect_threshold)
    (cluster_dic, group_dic) = separate_cluster(cluster_dic, comparator2)
    return (cluster_dic, group_dic)


def merge_bridge_articles(cluster_dic, bridge_aids, comparator, merge_threshold=0.38):
    for aid in bridge_aids:
        distance = get_sim_to_all_clusters(aid, cluster_dic, comparator)
        key = util.get_max_key_dic(distance)
        if (distance[key] > merge_threshold):
            print 'ADD: %d to %s with SCORE = %f' % (aid, cluster_dic[key], distance[key])
            cluster_dic[key].append(aid)
        else:
            print 'DO NOT ADD: %d to %s with SCORE = %f' % (aid, cluster_dic[key], distance[key])
            cluster_dic[aid] = [aid]


def get_sim_to_all_clusters(aid, cluster_dic, comparator):
    distance = dict()
    for key in cluster_dic:
        distance[key] = get_sim_one2cluster(aid, cluster_dic[key], comparator)
    return distance


def get_sim_one2cluster(aid, cluster, comparator):
    score = 0.0
    count = 0.0
    for temp in cluster:
        score += comparator.get_similarity(aid, temp)
        count += 1
    return score / count


def connect_new_article_to_old_clusters(cluster_dic, corpus1, trend_comparator, corpus2):
    print 'FIRST TYPE OF CONNECT'
    merge_dic = dict()
    new_corpus = dict()
    id2label = util.get_id2label_dic(cluster_dic)
    corpus_size = len(corpus1)
    iden_comp = trend_comparator.get_Iden_comp()
    iden_dic = cluster_doc2.cluster_doc_with_comparator(corpus1.keys(), iden_comp,
                                                        IDENTICAL_THRESHOLD)  # giam nhung bai trung so sanh cho nhanh
    for aid2 in corpus2:
        labels = []
        for aid1 in iden_dic:
            sim = trend_comparator.get_similarity_connect(corpus1[aid1], corpus2[aid2], corpus_size)
            if sim >= INTER_CONNECT_THRESHOLD:
                label = id2label[aid1]
                labels.append(label)
        if (len(labels) > 0):
            temp_dic_count = util.list_to_dic_count(labels)
            if (len(temp_dic_count) > 1):
                print '%d belongs to %s' % (aid2, str(temp_dic_count))
            max_key = util.get_max_key_dic(temp_dic_count)
            merge_dic[aid2] = max_key
        else:
            new_corpus[aid2] = corpus2[aid2]
    return (merge_dic, new_corpus)


def add_to_label_score(score_dic, label, score):
    if label in score_dic:
        score_dic[label].append(score)
    else:
        score_dic[label] = [score]


def remove_completely_identical_in_dic(cluster_dic, iden_comp):
    result = dict()
    for key in cluster_dic:
        alist = cluster_dic[key]
        temp_iden_dic = cluster_doc2.cluster_doc_with_comparator(alist, iden_comp, IDENTICAL_THRESHOLD)
        new_list = temp_iden_dic.keys()
        result[key] = new_list
    return result


def get_avg_sim_form_doc2cluster(old_cluster, pre_copus, doc, comparator):
    count = 0.0
    average = 0.0
    for el_aid in old_cluster:
        average += comparator.get_similarity_connect(doc, pre_copus[el_aid], len(pre_copus))
        count += 1.0
    return average / count


def get_closest_cluster(cluster_dic, doc, comparator):
    if len(cluster_dic) == 0:
        return (-1, 0)
    distance_dic = dict()
    pre_corpus = comparator.corpus
    for key in cluster_dic:
        distance_dic[key] = get_avg_sim_form_doc2cluster(cluster_dic[key], pre_corpus, doc, comparator)
    max_key = util.get_max_key_dic(distance_dic)
    return (max_key, distance_dic[max_key])


def connect_new_article_to_old_clusters2(cluster_dic, trend_comparator, corpus2,
                                         connect_threshold=INTER_CONNECT_THRESHOLD):
    print 'SECOND TYPE'
    merge_dic = dict()
    new_corpus = dict()
    iden_comp = trend_comparator.get_Iden_comp()
    pre_cluster_dic = remove_completely_identical_in_dic(cluster_dic,
                                                         iden_comp)  # remove identical articles in cluster_dic

    for aid in corpus2:
        (max_key, score) = get_closest_cluster(pre_cluster_dic, corpus2[aid], trend_comparator)
        if (score > connect_threshold):
            print 'connect %d to cluster %s, score = %f\n --------------------------------' % (
                aid, str(pre_cluster_dic[max_key]), score)
            merge_dic[aid] = max_key
        else:
            new_corpus[aid] = corpus2[aid]
    return (merge_dic, new_corpus)


def connect_on_date(date):
    # remove real_time
    logger.info('remove real_time records')
    h_sub_util.remove_real_trend_today(TABLE_NAME)
    logger.info('recover state before date')
    recover_before_date_sub(date)
    t1 = time.time()
    today = util.get_today_str()
    table_name = resources.EVENT_TABLE
    overwrite = True
    if (date == today):
        table_name = resources.EVENT_CURRENT_TABLE
        overwrite = False
    events = event_dao.get_event_in_range(date, date, table_name)
    logger.info('number of events on %s is: %d' % (date, len(events)))
    trend_dic = dict()
    vocab_date = date
    if (date == today):
        vocab_date = util.get_yesterday_str()
    vocab_map = util.get_word_2_id(trend_util.get_dictionary_path(vocab_date))
    for event in events:
        trend_id = get_trend_id(event)  # event[resources.EVENT_TREND_ID]
        if trend_id in trend_dic:
            trend_dic[trend_id].append(event)
        else:
            trend_dic[trend_id] = [event]
    for trend_id in trend_dic:
        if (len(TREND_FILDER_IDS) == 0):
            # recover_today_record(trend_id,date)
            connect_in_trend(trend_id, trend_dic[trend_id], vocab_map, overwrite)
        else:
            if trend_id in TREND_FILDER_IDS:
                # recover_today_record(trend_id,date)
                connect_in_trend(trend_id, trend_dic[trend_id], vocab_map, overwrite)
    t2 = time.time()
    logger.info('time for process %s is %s seconds' % (date, t2 - t1))


def get_trend_id(event):
    trend_id = event[resources.EVENT_TREND_ID]
    if trend_id == -1:
        return event[resources.EVENT_ID]
    return trend_id


def create_sub_event_record(sub_id, trend_id, aids, art_dic):
    s_aids = sort_aid_by_dates(aids, art_dic)
    start_datetime = art_dic[s_aids[-1]][dao.CREATE_TIME]
    end_datetime = art_dic[s_aids[0]][dao.CREATE_TIME]
    current_date = str(end_datetime.date())
    sub_event = dict()
    sub_event[sub_resources.ID] = sub_id
    sub_event[sub_resources.START_DATE] = str(start_datetime)
    sub_event[sub_resources.LAST_DATE] = str(end_datetime)
    sub_event[sub_resources.ARTICLES] = {current_date: s_aids}
    sub_event[sub_resources.TOTAL_ARTICLE_NUM] = len(aids)
    sub_event[sub_resources.TREND_ID] = trend_id
    sub_event[sub_resources.TYPICAL_ART] = s_aids[0]
    sub_event[sub_resources.COHERENCE] = 1.0
    sub_event[sub_resources.TAG] = ''
    return sub_event


def sort_aid_by_dates(aids, art_dic):
    date_dic = dict()
    for aid in aids:
        date_dic[aid] = art_dic[aid][dao.CREATE_TIME]
    pairs = util.sort_dic_des(date_dic)
    s_aids = []  # [0] is max
    for (v, k) in pairs:
        s_aids.append(k)
    return s_aids


def update_sub_event_record(add_cluster_dic,
                            table=TABLE_NAME):  # add cluster only contains additional articles, not aids in database
    if (len(add_cluster_dic) == 0):
        return
    all_aids = []
    for subid in add_cluster_dic:
        util.migrate_list2_to_list1(all_aids, add_cluster_dic[subid])
    articles = event_dao.get_article_urls(all_aids)
    art_dic = util.get_dic_from_list(articles, dao.ID)
    sub_ids = add_cluster_dic.keys()
    sub_events = h_sub_util.get_sub_util_by_ids(sub_ids, table)
    for sub_event in sub_events:
        sub_id = sub_event[sub_resources.ID]
        aids = add_cluster_dic[sub_id]
        s_aids = sort_aid_by_dates(aids, art_dic)
        last_date = art_dic[s_aids[0]][dao.CREATE_TIME]

        sub_event[sub_resources.LAST_DATE] = str(last_date)
        temp_art_dic = util.str_to_dict(sub_event[sub_resources.ARTICLES])
        temp_art_dic[str(last_date.date())] = s_aids
        sub_event[sub_resources.ARTICLES] = str(temp_art_dic)

        sub_event[sub_resources.TOTAL_ARTICLE_NUM] += len(aids)
        sub_event[sub_resources.TYPICAL_ART] = s_aids[0]
    sub_id_num = len(sub_ids)
    logger.info('delete %d sub_events being updated: %s' % (sub_id_num, str(sub_ids)))
    h_sub_util.delete_sub_with_ids(sub_ids, table)
    logger.info('update %d sub_events: %s' % (sub_id_num, str(sub_ids)))
    h_sub_util.insert_sub_event_wo_event_id(sub_events, table)


def get_cluster_dic_path(trend_id):
    return CLUSTER_DIC_TEMP_FOLDER + '/%d_cluster_dic.dat' % trend_id


def insert_new_sub_event_records(cluster_dic, group_dic, trend_id, table=TABLE_NAME):
    all_ids = []
    for key in cluster_dic:
        util.migrate_list2_to_list1(all_ids, cluster_dic[key])
    articles = event_dao.get_article_urls(all_ids)
    art_dic = util.get_dic_from_list(articles, dao.ID)
    records = []
    for key in cluster_dic:
        aids = cluster_dic[key]
        record = create_sub_event_record(key, trend_id, aids, art_dic)
        record[h_sub_util.GROUP_ID] = group_dic[key]
        records.append(record)
    logger.info('insert %d sub_events into database' % len(records))
    h_sub_util.insert_sub_event_wo_event_id(records, table)


def recover_before_date_sub(date):
    today_records = h_sub_util.get_all_sub_from_today(TABLE_NAME, date)
    if (len(today_records) == 0):
        return
    inter_records = []
    new_record_ids = []
    for record in today_records:
        start_date = str(record[sub_resources.START_DATE].date())
        if start_date == date:
            new_record_ids.append(record[sub_resources.ID])
        else:
            inter_records.append(record)
    if (len(new_record_ids) > 0):
        print 'remove totaly new records of today: %s' % str(new_record_ids)
        h_sub_util.delete_sub_with_ids(new_record_ids, TABLE_NAME)
    sub_ids = []
    for record in inter_records:
        recover_record_from_today(record)
        sub_ids.append(record[sub_resources.ID])
    if (len(inter_records) > 0):
        print 'recover %d to previous last_date from today' % len(inter_records)
        sub_id_num = len(sub_ids)
        logger.info('delete %d sub_events being updated: %s' % (sub_id_num, str(sub_ids)))
        h_sub_util.delete_sub_with_ids(sub_ids, TABLE_NAME)
        logger.info('update %d sub_events: %s' % (sub_id_num, str(sub_ids)))
        h_sub_util.insert_sub_event_wo_event_id(inter_records, TABLE_NAME)


def recover_today_record(trend_id, date):
    today_records = h_sub_util.get_sub_from_today(TABLE_NAME, trend_id, date)
    if (len(today_records) == 0):
        return
    inter_records = []
    new_record_ids = []
    for record in today_records:
        start_date = str(record[sub_resources.START_DATE].date())
        if start_date == date:
            new_record_ids.append(record[sub_resources.ID])
        else:
            inter_records.append(record)
    if (len(new_record_ids) > 0):
        print 'remove totaly new records of today: %s' % str(new_record_ids)
        h_sub_util.delete_sub_with_ids(new_record_ids, TABLE_NAME)
    sub_ids = []
    for record in inter_records:
        recover_record_from_today(record)
        sub_ids.append(record[sub_resources.ID])
    if (len(inter_records) > 0):
        print 'recover %d to previous last_date from today' % len(inter_records)
        sub_id_num = len(sub_ids)
        logger.info('delete %d sub_events being updated: %s' % (sub_id_num, str(sub_ids)))
        h_sub_util.delete_sub_with_ids(sub_ids, TABLE_NAME)
        logger.info('update %d sub_events: %s' % (sub_id_num, str(sub_ids)))
        h_sub_util.insert_sub_event_wo_event_id(inter_records, TABLE_NAME)


def recover_record_from_today(record):
    last_date = str(record[sub_resources.LAST_DATE].date())
    art_dic = util.str_to_dict(record[sub_resources.ARTICLES])
    remove_aid_num = len(art_dic[last_date])
    del art_dic[last_date]
    dates = art_dic.keys()
    max_date = max(dates)
    last_aids = art_dic[max_date]
    articles = event_dao.get_article_urls(last_aids)
    art_re_dic = util.get_dic_from_list(articles, sub_resources.ID)
    s_aids = sort_aid_by_dates(last_aids, art_re_dic)
    end_datetime = art_re_dic[s_aids[0]][dao.CREATE_TIME]

    record[sub_resources.LAST_DATE] = str(end_datetime)
    record[sub_resources.ARTICLES] = str(art_dic)
    record[sub_resources.TOTAL_ARTICLE_NUM] -= remove_aid_num
    record[sub_resources.TYPICAL_ART] = s_aids[-1]


def connect_in_trend(t_trend_id, events, vocab_map, overwrite=True):
    trend_id = t_trend_id
    if t_trend_id == -1:
        trend_id = events[0][resources.EVENT_ID]
    logger.info('get all aids of trend: %d today' % trend_id)
    all_ids = []
    for event in events:
        aids = util.get_full_article_ids(event[resources.EVENT_ARTICCLE_IDS])
        util.migrate_list2_to_list1(all_ids, aids)
        event_id = event['id']
        print 'event_id = %d' % event_id
    cluster_result_previous_path = get_cluster_dic_path(trend_id)
    if not os.path.exists(cluster_result_previous_path):  # su kien bay gio moi xuat hien
        logger.info(' %d is sub_clustered for the first time' % trend_id)
        print 'all_ids: %s' % str(all_ids)
        (cluster_dic, group_dic) = cluster_event_of_trend(all_ids, vocab_map, -1)  #
        insert_new_sub_event_records(cluster_dic, group_dic, trend_id)
        if overwrite:
            util.save_object(cluster_result_previous_path, cluster_dic)
    else:
        logger.info('start updating trend_id: %d' % trend_id)
        previous_cluster_dic = util.read_object(cluster_result_previous_path)
        pre_aids = []
        for subid in previous_cluster_dic:
            util.migrate_list2_to_list1(pre_aids, previous_cluster_dic[subid])
        max_group_id = h_sub_util.get_max_group_id(trend_id, TABLE_NAME)
        (cluster_dic2, new_cluster_dic, group_dic, old_sub_dic) = merge_events_to_existing_trend(all_ids, vocab_map,
                                                                                                 pre_aids,
                                                                                                 previous_cluster_dic,
                                                                                                 max_group_id)
        logger.info('old_sub_dic: %s' % str(old_sub_dic))
        logger.info('new_sub_dic: %s' % str(new_cluster_dic))
        if (len(new_cluster_dic) > 0):
            insert_new_sub_event_records(new_cluster_dic, group_dic, trend_id)
        if (len(old_sub_dic) > 0):
            update_sub_event_record(old_sub_dic)
        if overwrite:
            util.save_object(cluster_result_previous_path, cluster_dic2)
    logger.info('successfully updating %d' % trend_id)


def merge_events_to_existing_trend(current_aids, vocab_map, pre_aids, pre_cluster_dic, max_group_id):
    corpus = sub_data_reader.get_corpus_rep_of_articles(current_aids, vocab_map)
    pre_corpus = sub_data_reader.get_corpus_rep_of_articles(pre_aids, vocab_map)
    trend_comparator = Trend_Comparator(pre_corpus)
    logger.info('connect with old clusters')
    (merge_dic, new_corpus) = connect_new_article_to_old_clusters2(pre_cluster_dic, trend_comparator, corpus)
    old_sub = util.get_label2id_dic(merge_dic)
    cluster_dic2 = old_sub.copy()

    logger.info('cluster new_corpus')
    trend_comparator2 = Trend_Comparator(corpus)
    comp2 = Content_Focus_Comparator(trend_comparator2)
    (new_cluster_dic, new_group_dic) = decompose_event(trend_comparator2, comp2, new_corpus.keys(),
                                                       max_group_id)  # cluster_doc2.cluster_doc_with_comparator(new_corpus.keys(),comp2,BINARY_THRESHOLD)
    util.migrate_dic2_to_dic1(cluster_dic2, new_cluster_dic)
    return (cluster_dic2, new_cluster_dic, new_group_dic, old_sub)


def cluster_event_of_trend(all_ids, vocab_map, max_group_id):
    corpus = sub_data_reader.get_corpus_rep_of_articles(all_ids, vocab_map)
    comparator = Trend_Comparator(corpus)
    comp2 = Content_Focus_Comparator(comparator)
    #    aid1 = 68576577
    #    aid2 = 68611898
    #    print 'SIM ++++++++++++++++++++= %f'%comparator.get_similarity(aid1,aid2)
    #    sys.exit(1)
    (cluster_dic, group_dic) = decompose_event(comparator, comp2, all_ids, max_group_id)
    return (cluster_dic, group_dic)


def sub_clusters(date1, date2):
    logger.info('process sub_cluster from %s to %s' % (date1, date2))
    start = date1
    t1 = time.time()
    while (True):
        logger.info('sub_cluster on %s' % start)
        connect_on_date(start)
        if start == date2:
            break
        start = util.get_ahead_day(start, 1)
    t2 = time.time()
    logger.info('time for processing from %s to %s is %s' % (date1, date2, t2 - t1))


def get_gid_2_cluster(group_doc):
    result = dict()
    for key in group_doc:
        gid = group_doc[key]
        if gid in result:
            result[gid].append(key)
        else:
            result[gid] = [key]
    return result


def gen_html(all_ids, cluster_dic, coherence_dic, group_doc, save_path):
    f = codecs.open(save_path, 'w', encoding='utf8')
    f.write('<html lang="vi" xmlns="http://www.w3.org/1999/xhtml">')
    f.write('<meta http-equiv="Content-Type" content="text/html; charset=utf-8" />')
    f.write('<table border=1>')
    articles = event_dao.get_article_urls(all_ids)
    art_dic = util.get_dic_from_list(articles, 'id')
    count = 0
    gid2cluster = get_gid_2_cluster(group_doc)
    for gid in gid2cluster:
        for key in gid2cluster[gid]:
            aids = cluster_dic[key]
            count += 1
            cohe = 1.0
            if coherence_dic:
                cohe = coherence_dic[key]
            f.write('<tr><td><mark>%d</mark></td> <td>%d......%d____%f</td></tr>\n' % (count, gid, len(aids), cohe))
            s_aids = sorted(aids)
            for aid in s_aids:
                # f.write('<tr><td>%d</td><td><a href = %s>%s</a><br> %s</td></tr>\n'%(aid,art_dic[aid]['url'],art_dic[aid]['title'],art_dic[aid]['description']))
                f.write('<tr><td>%d</td><td><a href = %s>%s</a></td></tr>\n' % (
                    aid, art_dic[aid]['url'], art_dic[aid]['title']))
    f.write('</table>')
    f.close()


def main():
    if (len(sys.argv) != 3):
        print 'usage: python deploy_ver1.py date1/next date2/next'
        sys.exit(1)
    date1 = sys.argv[1]
    date2 = sys.argv[2]
    if (date1 == 'next'):
        date1 = util.get_yesterday_str()
        date2 = date1
    sub_clusters(date1, date2)
    logger.info('finished ...')


# VOCAB_MAP = util.get_word_2_id(trend_util.get_dictionary_path('2016-06-03'))
# DATASET_FOLDER = '/media/khaimai/F8C6516EC6512DDE/khai_folder/projects/trend_py/trendy_ploy1.0/annote/separate'
# PERFORMER = perform.performer(DATASET_FOLDER)

def filter_cluster_dic(cluster_dic, old_id_dic):
    result = dict()
    for key in cluster_dic:
        aids = cluster_dic[key]
        remains = []
        for aid in aids:
            if aid not in old_id_dic:
                remains.append(aid)
        if len(remains) > 0:
            new_id = min(remains)
            result[new_id] = remains
    return result


def connect_event(trend_id, save_option=False):
    print 'process TREND_ID: %d' % trend_id
    event_ids = TREND_IDS[trend_id]
    event_id1 = event_ids[0]
    corpus1 = sub_data_reader.get_data_of_event(event_id1)
    comparator1 = Trend_Comparator(corpus1)
    (cluster_dic, group_dic) = decompose_event(comparator1, corpus1.keys())
    score1 = PERFORMER.measure(event_id1, cluster_dic.values(), PERFORMER.F_SCORE)
    event_id2 = event_ids[1]
    corpus2 = sub_data_reader.get_data_of_event(event_id2)
    (merge_dic, new_corpus) = connect_new_article_to_old_clusters2(cluster_dic, corpus1, comparator1, corpus2)
    for aid in merge_dic:
        subid = merge_dic[aid]
        cluster_dic[subid].append(aid)
    old_sub = util.get_label2id_dic(merge_dic)
    print 'CONNECT RESULT: %s' % str(old_sub)
    logger.info('cluster new_corpus')
    trend_comparator2 = Trend_Comparator(corpus2)
    (new_cluster_dic, group_dic1) = decompose_event(trend_comparator2,
                                                    new_corpus.keys())  # new_corpus.keys()#cluster_doc2.cluster_doc_with_comparator(new_corpus.keys(),comp2,BINARY_THRESHOLD)
    # new_cluster_dic = filter_cluster_dic(new_cluster_dic,merge_dic)
    util.migrate_dic2_to_dic1(cluster_dic, new_cluster_dic)
    fscore = PERFORMER.measure(trend_id, cluster_dic.values(), PERFORMER.F_SCORE)

    util.migrate_dic2_to_dic1(old_sub, new_cluster_dic)
    score2 = PERFORMER.measure(event_id2, old_sub.values(), PERFORMER.F_SCORE)
    print 'FSCORE %d  = %f' % (event_id1, score1)
    print 'FSCORE %d = %f' % (event_id2, score2)
    print 'FSCORE OF %d = %f' % (trend_id, fscore)
    if save_option:
        save_path = get_save_path(trend_id)
        all_ids = []
        util.migrate_list2_to_list1(all_ids, corpus1.keys())
        util.migrate_list2_to_list1(all_ids, corpus2.keys())
        gen_html(all_ids, cluster_dic, None, group_dic, save_path)
        print save_path
    return (fscore, score1, score2)


def get_save_path(sid):
    return '/media/khaimai/F8C6516EC6512DDE/khai_folder/projects/trend_py/trendy_ploy1.0/annote/trend_report/%d.html' % sid


def find_score_of_trend():
    trend_score = 0.0
    score1 = 0.0
    score2 = 0.0
    for trend_id in TREND_IDS:
        (fscore, s1, s2) = connect_event(trend_id)
        trend_score += fscore
        score1 += s1
        score2 += s2
    trend_score = trend_score / len(TREND_IDS)
    score2 = score2 / len(TREND_IDS)
    print 'total trend = %f; score = %f' % (trend_score, score2)
    return (trend_score, score2)


def print_dic_asc(dic):
    pairs = util.sort_dic_asc(dic)
    for (v, k) in pairs:
        print '%f:%f' % (k, v)


def find_optimal_parameters():
    global INTER_CONNECT_THRESHOLD
    pars = [0.29, 0.30, 0.32, 0.34]
    trend_score = dict()
    event_score = dict()
    for par in pars:
        INTER_CONNECT_THRESHOLD = par
        print 'connect with: %f' % INTER_CONNECT_THRESHOLD
        (t1, t2) = find_score_of_trend()
        trend_score[par] = t1
        event_score[par] = t2
    print 'TREND_CONNECT -------------'
    print_dic_asc(trend_score)
    print 'EVENT_CONNECT -------------'
    print_dic_asc(event_score)
    print 'finished ...'


# connect_event(4715,True)
# find_optimal_parameters()
# test_deploy()
if __name__ == '__main__':
    main()
# find_score_of_trend()
# recover_today_record(18469,'2016-08-12')
