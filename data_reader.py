import dao, resources, util, time, pymysql, pickle, trend_util, os, tree, sys, event_dao, logging

sys.path.insert(0, 'bolx')
import e_util
from sets import Set
from gensim import corpora
import numpy as np

CONTENT_THESHOLD = 250  # remove text with short length ()
sys.path.insert(0, 'bolx')
logger = logging.getLogger("data_reader")
logger.setLevel(logging.INFO)
util.set_log_file(logger, resources.GENERAL_LOG)
sys.path.insert(0, 'classification')
import art_category


def get_data(day, logger=None, domain_path=resources.DOMAIN_PATH):
    print 'load classifier to classify catid of articles'
    CAT_CLASSIFIER = art_category.Classifier_ob()
    if logger:
        logger.info('start updating dictionary on %s' % day)
    else:
        print 'start updating dictionary on %s' % day
    stop_set = get_stopwords(resources.STOP_WORD_PATH)
    domains = get_domains(resources.DOMAIN_PATH)

    conn = dao.get_connection()
    cur = conn.cursor(pymysql.cursors.DictCursor)
    query = create_query_get_docs(day)
    if logger:
        logger.info('execute: %s' % query)
    else:
        print 'execute: %s' % query
    d1 = time.time()
    print query
    cur.execute(query)
    rows = cur.fetchall()
    d2 = time.time()
    if logger:
        logger.info('time for fetching data: %d' % (d2 - d1))
    else:
        print 'time for fetching data: %d' % (d2 - d1)
    doc_list = dict()
    bi_list = dict()
    id_list = dict()
    count = 0
    error_count = dict()
    doc_count = dict()
    short_count = dict()
    short_ids = dict()
    error_ids = dict()
    titles = dict()
    catids = dict()
    none_cat_count = 0
    for domain in domains:
        error_count[domain] = 0
        doc_count[domain] = 0
        short_count[domain] = 0
        doc_list[domain] = list()
        bi_list[domain] = list()
        id_list[domain] = list()
        short_ids[domain] = list()
        error_ids[domain] = list()
        titles[domain] = Set()
    iden_count = 0
    for row in rows:
        d = row[dao.DOMAIN]
        domain = get_d_domains(d, domains)
        if (domain):
            temp_id = int(row[dao.ID])
            if (len(row[dao.CONTENT]) < CONTENT_THESHOLD):
                # logger.info('id: %d content is quite short'%temp_id)
                short_ids[domain].append(temp_id)
                short_count[domain] += 1
            else:
                doc = get_doc_from_text(row[dao.TOKEN], stop_set)
                title = row[dao.TITLE]
                if (doc is not None):
                    if title in titles[domain]: # ttt dupplicate title
                        # check whether this doc is identical or not
                        if doc in doc_list[domain]:
                            # print 'identical article: %d'%temp_id
                            # print title
                            iden_count += 1
                        else:
                            doc_list[domain].append(doc)
                            bigram = get_doc_for_2gram(row[dao.TOKEN])
                            bi_list[domain].append(bigram)
                            id_list[domain].append(temp_id)
                            doc_count[domain] += 1
                            titles[domain].add(title)
                            if (row[dao.CATID] != None):
                                catids[temp_id] = row[dao.CATID]
                            else: # ttt tu phan loai catid
                                catids[temp_id] = CAT_CLASSIFIER.get_catid(row[dao.URL], row[dao.TOKEN])
                                none_cat_count += 1
                                #                                if logger:
                                #                                    logger.info('catid of %d is not calculated'%(temp_id))
                                #                                else:
                                #                                    print 'catid of %d is not calculated'%(temp_id)

                    else:
                        doc_list[domain].append(doc)

                        bigram = get_doc_for_2gram(row[dao.TOKEN])
                        bi_list[domain].append(bigram)
                        id_list[domain].append(temp_id)
                        doc_count[domain] += 1
                        titles[domain].add(title)
                        catids[temp_id] = row[dao.CATID]
                else:
                    error_count[domain] += 1
                    error_ids[domain].append(temp_id)
                    # logger.error('id: %d at updating dictionary'%temp_id)
            count = count + 1
            if (count % 1000 == 0):
                print ' count = %d ' % count
    dao.free_connection(conn, cur)
    if logger:
        logger.info('identical count: %d' % iden_count)
        logger.info('none_catid count: %d' % none_cat_count)
    else:
        print 'identical count: %d' % iden_count
        print 'none_catid count: %d' % none_cat_count
    return (doc_list, bi_list, id_list, error_count, doc_count, short_count, short_ids, error_ids, catids)


def get_tokens_of_articles(all_aids, day, logger=None, domain_path=resources.DOMAIN_PATH):
    if logger:
        logger.info('start updating dictionary on %s' % day)
    else:
        print 'start updating dictionary on %s' % day
    stop_set = get_stopwords(resources.STOP_WORD_PATH)
    domains = get_domains(resources.DOMAIN_PATH)

    conn = dao.get_connection()
    cur = conn.cursor(pymysql.cursors.DictCursor)
    wstr = util.get_in_where_str(all_aids)
    query = 'select * from %s where %s in (%s)' % (dao.NEWS_TABLE, dao.ID, wstr)
    if logger:
        logger.info('execute: %s' % query)
    else:
        print 'execute: %s' % query
    d1 = time.time()
    cur.execute(query)
    rows = cur.fetchall()
    d2 = time.time()
    if logger:
        logger.info('time for fetching data: %d' % (d2 - d1))
    else:
        print 'time for fetching data: %d' % (d2 - d1)
    doc_list = dict()
    id_list = dict()
    cat_dic = dict()
    count = 0
    doc_count = dict()
    none_cat_count = 0
    for domain in domains:
        doc_count[domain] = 0
        doc_list[domain] = list()
        id_list[domain] = list()
    iden_count = 0
    for row in rows:
        d = row[dao.DOMAIN]
        domain = get_d_domains(d, domains)
        temp_id = int(row[dao.ID])
        doc = get_doc_from_text(row[dao.TOKEN], stop_set)
        doc_list[domain].append(doc)
        cat_dic[temp_id] = row[dao.CATID]

        id_list[domain].append(temp_id)
        doc_count[domain] += 1
        count = count + 1
        if (count % 1000 == 0):
            print ' count = %d ' % count
    dao.free_connection(conn, cur)
    if logger:
        logger.info('identical count: %d' % iden_count)
        logger.info('none_catid count: %d' % none_cat_count)
    else:
        print 'identical count: %d' % iden_count
        print 'none_catid count: %d' % none_cat_count
    return (doc_list, id_list, doc_count, cat_dic)


def save_data(doc_list, id_list, save_path):
    """
    :param list doc_list:
    :param list id_list:
    :param str save_path:
    :return:
    """
    pair = (doc_list, id_list)
    with open(save_path, 'wb') as f:
        pickle.dump(pair, f)
    print 'save data at: %s' % save_path


def get_domains(domain_path):
    domains = Set()
    f = open(domain_path, 'r')
    for line in f:
        temp = line.strip()
        tg = temp.split(',')
        domains.add(tg[0])
    return domains


def create_query_getdocs(domain, day):
    date_day = day + ' 00:00:00'
    date_next_day = day + ' 23:59:59'
    query = 'SELECT %s,%s,%s \
    FROM %s WHERE (%s >= \'%s\') \
    and (%s < \'%s\') \
    and (%s = \'%s\') \
    and (create_time < get_time)' % (dao.TOKEN, dao.ID, dao.CONTENT, \
                                     dao.NEWS_TABLE, dao.CREATE_TIME, date_day, \
                                     dao.CREATE_TIME, date_next_day, \
                                     dao.DOMAIN, domain)
    return query


def create_query_get_docs(day):
    date_day = day + ' 00:00:00'
    date_next_day = day + ' 23:59:59'
    query = 'SELECT %s,%s,%s,%s,%s,%s,%s \
    FROM %s WHERE (%s between \'%s\' and \'%s\') and (%s < %s)' % ( \
        dao.URL, dao.TOKEN, dao.ID, dao.CONTENT, dao.DOMAIN, dao.TITLE, dao.CATID, \
        dao.NEWS_TABLE, dao.CREATE_TIME, date_day, date_next_day, dao.CREATE_TIME, dao.GET_TIME)
    return query


def get_doc_from_text(text, stop_set):
    """create a document as a list of nomalized tokens, stopwords are removed
    text is the raw tokenized text, separated by ' '.
    """
    try:
        # text.encode('utf-8')
        text = text.replace('\n', ' ')
        text = text.replace('\t', ' ')
        tokens = text.split(' ')
        res = list()
        for tok in tokens:
            temp = util.normalize_token(tok)
            check = util.check_token(temp)
            # print '%d'%check
            if (check == -1):
                return None
            if ((temp not in stop_set) and (check == 1)):  # not a stopword and has at least a Vn character or digit
                res.append(temp)
        return res
    except:
        return None


def get_doc_for_2gram(text):
    try:
        # text.encode('utf-8')
        text = text.replace('\n', ' ')
        text = text.replace('\t', ' ')
        tokens = text.split(' ')
        res = list()
        separator = [-1]
        for tok in tokens:
            temp = util.normalize_token(tok)
            check = util.check_token(temp)
            # print '%d'%check
            if (check == -1):
                return None
            else:
                res.append(temp)
                if (check == 0):  # check is a separator
                    separator.append(len(res) - 1)  # index of separator in res
        bi_gram = []
        separator.append(len(res))
        for i in range(len(separator) - 1):
            i1 = separator[i]
            i2 = separator[i + 1]
            create_bigram_from_chunk(res, i1, i2, bi_gram)
        return bi_gram
    except:
        return None


def create_bigram_from_chunk(l, i1, i2, bigrams):
    if (i2 - i1 >= 3):
        for i in range(i1 + 1, i2 - 1):
            bigrams.append('(%s;%s)' % (l[i], l[i + 1]))


def get_stopwords(path=resources.STOP_WORD_PATH):
    """ load stop_list files into a set
    return stopword set
    """
    stop_set = Set()
    f = open(path, 'r')
    for line in f:
        temp = line.strip()
        if (temp not in stop_set):
            stop_set.add(temp)
    f.close()
    return stop_set


def get_d_domains(d, domains):
    if d in domains:
        return d
    for domain in domains:
        if domain in d:
            return domain
    return None


def save_catid(catids, catid_path):
    with open(catid_path, 'wb') as f:
        pickle.dump(catids, f)
    print 'saving catids at: %s' % catid_path


# def read_bigram_data(day):
#    (df_map,corpus_size) = read_df_corpussize(day)
#    dictionary_path = bi_util.get_dictionary_path(day)
#    dictionary = corpora.Dictionary.load_from_text(dictionary_path)
#    df_map = util.get_dictionary_df(dictionary_path)
#    data_folder = bi_util.get_data_folder(day)
#    
#    corpus = dict()# map id and vector
#    ids = []
#    fnames = os.listdir(data_folder)
#    domains = dict() # need information about domains to cluster docs
#    for fname in fnames:
#        print 'read bigram-data from file: %s'%fname
#        data_path = data_folder + '/'+fname
#        (doc_list,id_list) = read_doc_list(data_path)
#        for i in range(len(doc_list)):
#            doc = doc_list[i]
#            bow = dictionary.doc2bow(doc)
#            (wids,couts) = convert_bow_gensim_wids_counts(bow)
#            tf_idf_doc = get_tf_idf_from_bow(wids,couts,df_map,corpus_size)
#            #corpus.append(tf_idf_doc)
#            corpus[id_list[i]] = tf_idf_doc
#            ids.append(id_list[i])
#            domains[id_list[i]] = fname
#    print 'finish reading all data on day: %s'%day
#    return (ids,corpus,domains)

def read_all_data(day, data_folder=None, idf_day=None):
    print 'read all data from day: %s' % day
    if (not data_folder):
        data_folder = trend_util.get_data_folder(day)
    if (not idf_day):
        (df_map, corpus_size) = read_df_corpussize(day)
        dictionary_path = trend_util.get_dictionary_path(day)
    else:
        (df_map, corpus_size) = read_df_corpussize(idf_day)
        dictionary_path = trend_util.get_dictionary_path(idf_day)
    dictionary = corpora.Dictionary.load_from_text(dictionary_path)

    corpus = dict()  # map id and vector
    ids = []
    fnames = os.listdir(data_folder)
    domains = dict()  # need information about domains to cluster docs
    for fname in fnames:
        print 'read data from file: %s' % fname
        data_path = data_folder + '/' + fname
        (doc_list, id_list) = read_doc_list(data_path)
        for i in range(len(doc_list)):
            doc = doc_list[i]
            bow = dictionary.doc2bow(doc)
            (wids, couts) = convert_bow_gensim_wids_counts(bow)
            tf_idf_doc = get_tf_idf_from_bow(wids, couts, df_map, corpus_size)
            # corpus.append(tf_idf_doc)
            corpus[id_list[i]] = tf_idf_doc
            ids.append(id_list[i])
            domains[id_list[i]] = fname
    print 'finish reading all data on day: %s' % day
    return (ids, corpus, domains)


def real_data_of_articles(day, aids, data_folder=None, idf_day=None):
    print 'read all data from day: %s' % day
    if (not data_folder):
        data_folder = trend_util.get_data_folder(day)
    if (not idf_day):
        (df_map, corpus_size) = read_df_corpussize(day)
        dictionary_path = trend_util.get_dictionary_path(day)
    else:
        (df_map, corpus_size) = read_df_corpussize(idf_day)
        dictionary_path = trend_util.get_dictionary_path(idf_day)
    dictionary = corpora.Dictionary.load_from_text(dictionary_path)

    corpus = dict()  # map id and vector
    fnames = os.listdir(data_folder)
    id_set = Set(aids)
    for fname in fnames:
        print 'read data from file: %s' % fname
        data_path = data_folder + '/' + fname
        (doc_list, id_list) = read_doc_list(data_path)
        for i in range(len(doc_list)):
            aid = id_list[i]
            if aid in id_set:
                doc = doc_list[i]
                bow = dictionary.doc2bow(doc)
                (wids, couts) = convert_bow_gensim_wids_counts(bow)
                tf_idf_doc = get_tf_idf_from_bow(wids, couts, df_map, corpus_size)
                # corpus.append(tf_idf_doc)
                corpus[id_list[i]] = tf_idf_doc
    print 'finish reading all data on day: %s' % day
    return corpus


def read_data_local_tfidf_by_ids(event_path, date, epsilon):
    dictionary_path = trend_util.get_dictionary_path(date)
    dictionary = corpora.Dictionary.load_from_text(dictionary_path)
    # (df_map,local_size) = read_df_corpussize(date)
    res_corpus = util.read_object(event_path)
    df_map = get_idf_map(res_corpus, dictionary)
    local_size = len(res_corpus)
    print 'local_size = %d' % local_size
    result = dict()
    for aid in res_corpus:
        bow = res_corpus[aid]
        (wids, couts) = get_tf_with_penalty(bow, dictionary.token2id)
        tf_idf_doc = get_tf_idf_from_bow(wids, couts, df_map, local_size, epsilon)
        result[aid] = tf_idf_doc
    return (result, df_map)


def read_text_tfidf(text_dic, df_map, date, epsilon, local_size):
    dictionary_path = trend_util.get_dictionary_path(date)
    dictionary = corpora.Dictionary.load_from_text(dictionary_path)
    # (df_map,local_size) = read_df_corpussize(date)
    print 'local_size = %d' % local_size
    result = dict()
    for aid in text_dic:
        doc = text_dic[aid]
        bow = dictionary.doc2bow(doc)
        (wids, couts) = convert_bow_gensim_wids_counts(bow)
        tf_idf_doc = get_tf_idf_from_bow(wids, couts, df_map, local_size, epsilon)
        print  tf_idf_doc
        result[aid] = tf_idf_doc
    return result


def save_articles(aids, save_path, date):
    data_folder = trend_util.get_data_folder(date)
    fnames = os.listdir(data_folder)
    corpus = dict()
    for fname in fnames:
        print 'read data from file: %s' % fname
        data_path = data_folder + '/' + fname
        (doc_list, id_list) = read_doc_list(data_path)
        for i in range(len(doc_list)):
            doc = doc_list[i]
            corpus[id_list[i]] = doc
    res_corpus = dict()

    for aid in aids:
        doc = corpus[aid]
        res_corpus[aid] = doc
    util.save_object(save_path, res_corpus)
    print 'finish saving data at: %s' % save_path


def get_tf_with_penalty(token_list, token2id, max_add=3):
    bow_dic = dict()
    leng = len(token_list)
    sen_num = leng / 20  # assume that each line has 20 words
    for i in range(leng):
        token = token_list[i]
        if token in token2id:
            sent_order = i / 20
            count = 1 + float((sen_num - sent_order)) * max_add / sen_num
            wid = token2id[token]
            if wid in bow_dic:
                bow_dic[wid] += count
            else:
                bow_dic[wid] = count
    wids = []
    counts = []
    for wid in bow_dic:
        wids.append(wid)
        counts.append(bow_dic[wid])
    return (wids, counts)


def get_idf_map(corpus, dictionary):
    df_map = dict()
    for aid in corpus:
        doc = corpus[aid]
        bow = dictionary.doc2bow(doc)
        (wids, couts) = convert_bow_gensim_wids_counts(bow)
        for i in range(len(wids)):
            wid = wids[i]
            # count = couts[i]
            if wid in df_map:
                df_map[wid] += 1
            else:
                df_map[wid] = 1
    return df_map


def read_df_corpussize(day):
    dictionary_path = trend_util.get_dictionary_path(day)
    df_map = util.get_dictionary_df(dictionary_path)
    corpus_size = read_corpus_size(day)
    return (df_map, corpus_size)


def get_tf_idf_from_bow(wids, cts, df_map, corpus_size, epsilon=1.0):
    scores = []
    size = len(wids)
    # print 'size: %d'%size
    for i in range(size):
        wid = wids[i]
        ct = cts[i]
        if (wid not in df_map):
            print '%d not in df_map' % wid
            scores.append(0.0)
        else:
            n_wid = df_map[wid]  # number of docs containing wid
            score = (1 + np.log(float(ct))) * np.log(float(corpus_size) / float(n_wid) * epsilon)
            scores.append(score)
    scores_array = np.array(scores)
    scores_array = util.normalize_square(scores_array)
    res = dict()
    for i in range(len(wids)):
        res[wids[i]] = scores_array[i]
    return res


def convert_bow_gensim_wids_counts(bow):
    wids = []
    couts = []
    for pair in bow:
        wids.append(pair[0])
        couts.append(pair[1])
    return (wids, couts)


def read_doc_list(fpath):
    f = open(fpath, 'r')
    (doc_list, id_list) = pickle.load(f)
    f.close()
    return (doc_list, id_list)


def read_corpus_size(day):
    fpath = trend_util.get_corpus_size_path(day)
    corpus_size = 0
    if os.path.exists(fpath):
        f = open(fpath, 'r')
        line = f.readline()
        corpus_size = int(line)
        f.close()
    else:
        print 'corpus_size path does not exist: %s' % fpath
    return corpus_size


def update_dictionary2(day):
    today_dictionary_path = trend_util.get_dictionary_path(day)
    # if (os.path.exists(today_dictionary_path)):
    #    logger.info('dictionary has been updated, no need to update more')
    #    return None
    yesterday = util.get_previous_day(day)
    logger.info('start updating dictionary on %s' % day)
    today_folder = resources.DAYS_PATH + '/' + day
    util.create_folder(today_folder)
    dictionary = corpora.Dictionary()
    bi_dictionary = corpora.Dictionary()
    domains = get_domains(resources.DOMAIN_PATH)
    t1 = time.time()
    (doc_list, bi_list, id_list, error_count, doc_count, short_count, short_ids, error_ids, catids) = get_data(day,
                                                                                                               logger)
    t2 = time.time()
    logger.info('time for fetching data: %d seconds' % (t2 - t1))
    data_folder = trend_util.get_data_folder(day)
    util.create_folder(data_folder)

    # bi_data_folder = bi_util.get_data_folder(day)
    # util.create_folder(bi_data_folder)

    stat_folder = get_statistic_folder(day)
    util.create_folder(stat_folder)
    write_title_doc_statistics(day)
    total_count = 0
    for domain in domains:
        dictionary.add_documents(doc_list[domain], prune_at=None)
        bi_dictionary.add_documents(bi_list[domain], prune_at=None)
        save_data(doc_list[domain], id_list[domain], data_folder + '/' + domain + '.dat')

        # save_data(bi_list[domain],id_list[domain],bi_data_folder+'/'+domain+'.dat')

        write_statistics(domain, doc_count[domain], error_count[domain], short_count[domain], day)
        total_count += doc_count[domain]
        logger.info('error_count = %d, doc_count = %d, short_count = %d at domain %s' % (
            error_count[domain], doc_count[domain], short_count[domain], domain))
    catid_folder = trend_util.get_catid_folder(day)
    util.create_folder(catid_folder)
    catid_path = trend_util.get_catid_path(day)
    save_catid(catids, catid_path)
    write_total_doc_all_domain(day, total_count)
    stat_folder = resources.STAT_FOLDER + '/' + day
    util.create_folder(stat_folder)
    write_domain_statistics(short_ids, stat_folder + '/short.csv')
    write_domain_statistics(error_ids, stat_folder + '/error.csv')

    logger.info('merging dictionary on %s' % day)
    yesterday_dic_path = trend_util.get_dictionary_path(yesterday)
    temp_dic_path = trend_util.get_dictionary_temp_path(day)
    create_new_dictionary_from_old(dictionary, yesterday_dic_path, temp_dic_path, today_dictionary_path,
                                   resources.STOPWORD_RATIO, \
                                   resources.DF_THRESHOLD, resources.DF_THRESHOLD_STREAM)

    #    logger.info('merging bigrams on %s'%day)
    #    bi_dictionary_folder = bi_util.get_dictionary_folder(day)
    #    util.create_folder(bi_dictionary_folder)
    #    bi_yesterday_dic_path = bi_util.get_dictionary_path(yesterday)
    #    bi_temp_dic_path = bi_util.get_temp_dictionary_path(day)
    #    bi_today_dic_path = bi_util.get_dictionary_path(day)
    #    create_new_dictionary_from_old(bi_dictionary,bi_yesterday_dic_path,bi_temp_dic_path,bi_today_dic_path,bi_resources.STOPWORD_RATIO,\
    #    bi_resources.DF_THRESHOLD,bi_resources.DF_THRESHOLD_STREAM)

    t2 = time.time()
    logger.info('time for updating: %d' % (t2 - t1))
    old_corpus_size = read_corpus_size(yesterday)
    new_corpus_size = old_corpus_size + total_count
    logger.info('day: %s, increase: %d corpus_size: %d' % (day, total_count, new_corpus_size))
    save_corpus_size(new_corpus_size, day)
    logger.info('save ids file')
    ids_folder = e_util.get_ids_folder()
    util.create_folder(ids_folder)
    ids_path = e_util.get_ids_path(day)
    write_id_file(id_list, ids_path)
    logger.info('finish updating dictionary')


def update_dictionary_with_articles(all_ids, day, cat_dic=None):
    today_dictionary_path = trend_util.get_dictionary_path(day)
    # if (os.path.exists(today_dictionary_path)):
    #    logger.info('dictionary has been updated, no need to update more')
    #    return None
    yesterday = util.get_previous_day(day)
    logger.info('start updating dictionary on %s' % day)
    today_folder = resources.DAYS_PATH + '/' + day
    util.create_folder(today_folder)
    dictionary = corpora.Dictionary()

    domains = get_domains(resources.DOMAIN_PATH)
    t1 = time.time()
    (doc_list, id_list, doc_count, catids) = get_tokens_of_articles(all_ids, day, logger)
    t2 = time.time()
    logger.info('time for fetching data: %d seconds' % (t2 - t1))
    data_folder = trend_util.get_data_folder(day)
    util.create_folder(data_folder)

    stat_folder = get_statistic_folder(day)
    util.create_folder(stat_folder)
    write_title_doc_statistics(day)
    total_count = 0
    for domain in domains:
        dictionary.add_documents(doc_list[domain], prune_at=None)
        save_data(doc_list[domain], id_list[domain], data_folder + '/' + domain + '.dat')
        write_statistics(domain, doc_count[domain], 0, 0, day)
        total_count += doc_count[domain]
    catid_folder = trend_util.get_catid_folder(day)
    util.create_folder(catid_folder)
    catid_path = trend_util.get_catid_path(day)
    if cat_dic:
        save_catid(cat_dic, catid_path)
    else:
        save_catid(catids, catid_path)
    write_total_doc_all_domain(day, total_count)
    stat_folder = resources.STAT_FOLDER + '/' + day
    util.create_folder(stat_folder)

    logger.info('merging dictionary on %s' % day)
    yesterday_dic_path = trend_util.get_dictionary_path(yesterday)
    temp_dic_path = trend_util.get_dictionary_temp_path(day)
    create_new_dictionary_from_old(dictionary, yesterday_dic_path, temp_dic_path, today_dictionary_path,
                                   resources.STOPWORD_RATIO, \
                                   resources.DF_THRESHOLD, resources.DF_THRESHOLD_STREAM)

    #    logger.info('merging bigrams on %s'%day)
    #    bi_dictionary_folder = bi_util.get_dictionary_folder(day)
    #    util.create_folder(bi_dictionary_folder)
    #    bi_yesterday_dic_path = bi_util.get_dictionary_path(yesterday)
    #    bi_temp_dic_path = bi_util.get_temp_dictionary_path(day)
    #    bi_today_dic_path = bi_util.get_dictionary_path(day)
    #    create_new_dictionary_from_old(bi_dictionary,bi_yesterday_dic_path,bi_temp_dic_path,bi_today_dic_path,bi_resources.STOPWORD_RATIO,\
    #    bi_resources.DF_THRESHOLD,bi_resources.DF_THRESHOLD_STREAM)

    t2 = time.time()
    logger.info('time for updating: %d' % (t2 - t1))
    old_corpus_size = read_corpus_size(yesterday)
    new_corpus_size = old_corpus_size + total_count
    logger.info('day: %s, increase: %d corpus_size: %d' % (day, total_count, new_corpus_size))
    save_corpus_size(new_corpus_size, day)
    logger.info('save ids file')
    ids_folder = e_util.get_ids_folder()
    util.create_folder(ids_folder)
    ids_path = e_util.get_ids_path(day)
    write_id_file(id_list, ids_path)
    logger.info('finish updating dictionary')


def create_new_dictionary_from_old(dictionary, yesterday_dic_path, temp_dic_path, today_dictionary_path, \
                                   stop_ratio, merge_threshold, noise_threshold):
    if (os.path.exists(yesterday_dic_path)):
        dictionary.filter_extremes(no_below=noise_threshold, no_above=stop_ratio, keep_n=None)
        dictionary.save_as_text(temp_dic_path)
        util.merge_two_vocab_gensim(yesterday_dic_path, temp_dic_path, today_dictionary_path, merge_threshold)
    else:  # the first time
        dictionary.filter_extremes(no_below=noise_threshold, no_above=stop_ratio, keep_n=None)
        dictionary.compactify()
        dictionary.save_as_text(today_dictionary_path)


def write_id_file(id_domains, save_path):
    f = open(save_path, 'w')
    for domain in id_domains:
        for aid in id_domains[domain]:
            f.write('%d\n' % aid)
    f.close()


def get_statistic_folder(day):
    res = resources.STAT_FOLDER + '/' + day
    return res


def write_domain_statistics(dic_list, save_path):
    save_file = open(save_path, 'w')
    for key in dic_list:
        save_file.write('%s:' % key)
        for el in dic_list[key]:
            save_file.write('%d,' % el)
        save_file.write('\n')
    save_file.close()


def write_time_statistics(domain, interval, numbers, day):
    stat_folder = resources.STAT_FOLDER + '/' + day
    util.create_folder(stat_folder)
    save_path = stat_folder + '/time.csv'
    f = open(save_path, 'a')
    f.write('%s,%d,%d\n' % (domain, interval, numbers))
    f.close()


def write_title_doc_statistics(day):
    stat_path = get_statistic_folder(day) + '/' + resources.DOC_STAT_FILE_NAME
    f = open(stat_path, 'w')
    f.write('domain,doc_count,error_count,short_count\n')
    f.close()


def write_total_doc_all_domain(day, total):
    stat_path = get_statistic_folder(day) + '/' + resources.DOC_STAT_FILE_NAME
    f = open(stat_path, 'a')
    f.write('total,%d' % total)
    f.close()


def write_statistics(domain, doc_count, error_count, short_count, day):
    stat_path = get_statistic_folder(day) + '/' + resources.DOC_STAT_FILE_NAME
    f = open(stat_path, 'a')
    f.write('%s,%d,%d,%d\n' % (domain, doc_count, error_count, short_count))
    f.close()


def save_corpus_size(corpus_size, day):
    save_path = trend_util.get_corpus_size_path(day)
    f = open(save_path, 'w')
    f.write('%d' % corpus_size)
    f.close()


def read_catid(catid_path):
    f = open(catid_path, 'r')
    catids = pickle.load(f)
    f.close()
    return catids


def read_cat_tree(fpath):
    node = tree.get_nodes_from_file(fpath)
    return node


def read_cat_tree_and_add_node(fpath):
    node = tree.get_nodes_from_file(fpath)
    cat_tree = tree.get_node_with_id(0)
    last_node = tree.get_node_with_id(-1)
    tree.add_node(cat_tree, node)
    tree.add_node(cat_tree, last_node)  # add -1 node
    return cat_tree


def download_data_between_date(date1, date2):
    logger.info('fetching data from %s to %s' % (date1, date2))
    start = date1
    while (True):
        update_dictionary2(start)
        if (start == date2):
            break
        start = util.get_ahead_day(start, 1)
    logger.info('fetching data from %s to %s' % (date1, date2))


def test_update_dic_with_articles():
    query = 'select id FROM NewsDb.news_time_stat where date(get_time) = \'2016-02-17\''
    conn = dao.get_connection()
    cur = conn.cursor()
    all_ids = []
    cur.execute(query)
    rows = cur.fetchall()
    for row in rows:
        all_ids.append(row[0])
    dao.free_connection(conn, cur)
    update_dictionary_with_articles(all_ids, util.get_yesterday_str())
    print 'finish ...'


def main():
    if (len(sys.argv) != 3):
        print 'usage: python data_reader.py date1/next date2/next'
        sys.exit(1)
    date1 = sys.argv[1]
    date2 = sys.argv[2]
    if (date1 == 'next'):
        current_day = event_dao.get_max_date(resources.EVENT_TABLE)
        next_day = util.get_ahead_day(current_day, 1)
        download_data_between_date(next_day, next_day)
    else:
        download_data_between_date(date1, date2)


if __name__ == '__main__':
    main()
# t1 = time.time()
# test_update_dic_with_articles()
# t2 = time.time()
# print 'time for updating: %d'%(t2-t1)
