import resources, util, tree, cluster_doc2, event_dao, codecs, dao, trend_util, sys, logging
from sets import Set

sys.path.insert(0, 'bolx')
# import n_util, e_data_reader_entity, e_util, n_get_all_data
log_path = resources.LOG_FOLDER + '/SEPARATE_BY_TREND.log'
logger = logging.getLogger("log_separate_event_by_trend")
logger.setLevel(logging.INFO)
util.set_log_file(logger, log_path)


def calculate_coherence(aids, corpus, center):
    close = 0.0
    closes = dict()
    for i in range(len(aids)):
        closes[aids[i]] = util.get_similarity(corpus[aids[i]], center)
        close += closes[aids[i]]
    d_view = util.sort_dic_des(closes)
    a_list = []
    for (v, k) in d_view:
        a_list.append(k)
        # print 'coherence of %d is %f'%(k,v)
    return (close / float(len(aids)), a_list)


def get_entity_centers(clusters, corpus):
    en_centers = dict()
    en_sizes = dict()
    for key in clusters:
        en_sizes[key] = 0
        center_weights = []
        for aid in clusters[key]:
            if aid in corpus:
                center_weights.append((corpus[aid], 1.0))
                en_sizes[key] += 1
        center = util.update_center_with_center_weight_pair(center_weights)
        en_centers[key] = center
    return (en_centers, en_sizes)


def get_coherence_of_cluster(aids, corpus, center):
    close = 0.0
    for i in range(len(aids)):
        close += util.get_similarity(corpus[aids[i]], center)
    return close / float(len(aids))


def remove_small_events(domains, clusters, centers, sizes, remove_threshold):
    keys = clusters.keys()
    count = 0
    for key in keys:
        if (sizes[key] < remove_threshold):
            del sizes[key]
            del clusters[key]
            del centers[key]
            count += 1
        else:
            hold = remove_short_events(domains, clusters[key], centers[key], sizes[key])
            if (not hold):
                del sizes[key]
                del clusters[key]
                del centers[key]
                count += 1
    return count


def get_remove_small_events(domains, clusters, centers, sizes, remove_threshold):
    keys = clusters.keys()
    del_clusters = dict()
    del_centers = dict()
    for key in keys:
        if (sizes[key] < remove_threshold):
            del_clusters[key] = clusters[key]
            del_centers[key] = centers[key]
            del sizes[key]
            del clusters[key]
            del centers[key]
        else:
            hold = remove_short_events(domains, clusters[key], centers[key], sizes[key])
            if (not hold):
                del_clusters[key] = clusters[key]
                del_centers[key] = centers[key]
                del sizes[key]
                del clusters[key]
                del centers[key]
    return (del_clusters, del_centers)


def get_remove_small_events_with_catdic(clusters, centers, sizes, remove_threshold):
    keys = clusters.keys()
    del_clusters = dict()
    del_centers = dict()
    del_sizes = dict()
    for key in keys:
        if (sizes[key] < remove_threshold):
            del_clusters[key] = clusters[key]
            del_centers[key] = centers[key]
            del_sizes[key] = sizes[key]

            del sizes[key]
            del clusters[key]
            del centers[key]
    return (del_clusters, del_centers, del_sizes)


def remove_short_events(domains, cluster, center, size):  # this event that each document in it is quite short
    if (len(center) < resources.EVENT_CENTER_LENGTH_THRESHOLD):
        return False
    # check documents at the same domain
    same_domain = True
    cluster_domains = []
    for docid in cluster:
        if (domains[docid] not in cluster_domains):
            cluster_domains.append(domains[docid])
            if (len(cluster_domains) > 1):
                same_domain = False
                break
    if (same_domain):
        return False
    return True


import detec_trend


def get_ambiguous_cluster(trends, en_corpus, day, clusters, centers, am_trend_id_set):
    print 'ambiguous ids: %s' % str(am_trend_id_set)
    poor_set = Set()
    for key in clusters:
        en_center = get_center(clusters[key], en_corpus)
        (temp_score, max_key, detail) = detec_trend.get_best_trend(trends, centers[key], en_center, day, key,
                                                                   len(clusters[key]))
        connect = detec_trend.connect_trend_with_score(temp_score)
        if (connect):
            print 'event %d belongs to %d' % (key, max_key)
            if max_key in am_trend_id_set:
                print 'EVENT_ID %d BELONG TO AMBIGUOUS TREND_ID %d' % (key, max_key)
                poor_set.add(key)
        else:
            print 'event %d belongs to -1' % (key)
    return poor_set


TREND_ID_GROUP_THRESHOLD = 15  # an events are separated by trend_ids, if small clusters have sizes less than this --> merge


def separate_by_detect_trend(clusters, centers, sizes, corpus, domains, day, am_trend_ids):
    # WE TEMPORARILY STOP THIS FUNCTION
    return dict()
    logger.info('process day: %s' % day)
    logger.info('ambiguous trend_ids: %s' % str(am_trend_ids))
    trends = detec_trend.get_trends_for_connecting(day)
    (ids2, corpus2, domains2, catids2) = e_data_reader_entity.read_all_data(day)
    poor_set = get_poor_clusters(clusters, centers, sizes, corpus, domains, resources.COHERENCE_THRESHOLD)
    logger.info('EVENT WITH LOW COHERENCE: %s' % str(poor_set))
    poor_set2 = get_ambiguous_cluster(trends, corpus2, day, clusters, centers, am_trend_ids)
    logger.info('EVENT WITH AMBIGUOUS TREND_ID: %s' % str(poor_set2))
    for el in poor_set2:
        poor_set.add(el)

    sep_dic = dict()
    for bad_id in poor_set:
        all_ids = clusters[bad_id]
        en_center = get_center(clusters[bad_id], corpus2)
        all_trend_id = -1
        (temp_score, max_key, detail) = detec_trend.get_best_trend(trends, centers[bad_id], en_center, day, bad_id,
                                                                   len(all_ids))
        connect = detec_trend.connect_trend_with_score(temp_score)
        if (connect):
            all_trend_id = max_key
        logger.info('CONSIDER SEPARATING EVENT %d WITH TREND_ID = %d' % (bad_id, all_trend_id))
        print 'CONSIDER SEPARATING EVENT %d WITH TREND_ID = %d' % (bad_id, all_trend_id)
        trend_dic = dict()
        for aid in all_ids:
            if aid in corpus2:
                (temp_score, max_key, detail) = detec_trend.get_best_trend(trends, corpus[aid], corpus2[aid], day, aid,
                                                                           1)
                connect = detec_trend.connect_trend_with_score(temp_score)
                if connect:
                    util.add_element_to_dic(trend_dic, aid, max_key)
                else:
                    util.add_element_to_dic(trend_dic, aid, -1)
            else:
                util.add_element_to_dic(trend_dic, aid, -2)
        logger.info('PROCESS BAD_ID: %d' % bad_id)
        logger.info('trend_dic: %s' % str(trend_dic))
        separate = determine_separation(trend_dic, TREND_ID_GROUP_THRESHOLD, corpus)
        if (separate):
            logger.info('separate result: %s' % str(trend_dic))
            del clusters[bad_id]
            del centers[bad_id]
            del sizes[bad_id]
            group = Set()
            separate_str = ''
            for tid in trend_dic:
                new_id = trend_dic[tid][0]
                # logger.info('separate: %d'%new_id)
                separate_str += '%d,' % new_id
                clusters[new_id] = trend_dic[tid]
                centers[new_id] = get_center(clusters[new_id], corpus)
                sizes[new_id] = len(clusters[new_id])
                group.add(tid)
            sep_dic[bad_id] = group
            logger.info('separate_ids: %s' % separate_str)

        else:
            logger.info('REMERGE AGAIN !!!!!')
    return sep_dic


def determine_separation(trend_dic, select_threshold, corpus):
    selected_trend = Set()
    noise_trend = Set()
    for trend_id in trend_dic:
        trend_size = len(trend_dic[trend_id])
        if (
                    trend_size > select_threshold and trend_id != -2 and trend_id != -1):  # only try to find to separate if both events have been existed, not new
            selected_trend.add(trend_id)
        else:
            noise_trend.add(trend_id)
    if (len(selected_trend) > 1):
        center_dic = dict()
        for tid in selected_trend:
            center_dic[tid] = get_center(trend_dic[tid], corpus)
        for tid in noise_trend:
            aids = trend_dic[tid]
            del trend_dic[tid]
            for aid in aids:
                distances = cluster_doc2.compute_distance(corpus[aid], center_dic)
                max_tid = util.get_max_key_dic(distances)
                trend_dic[max_tid].append(aid)
        return True
    else:
        return False


def separate_clusers_smaller(clusters, centers, sizes, corpus, domains,
                             coherence_threshold=resources.COHERENCE_THRESHOLD,
                             separate_thresholds=resources.SEPARATE_THRESHOLDS,
                             identical_doc_threshold=resources.IDENTICAL_DOC_THRESHOLD,
                             remove_at_insert_threshold=resources.REMOVE_AT_INSERT_THRESHOLD, logger=None):
    poor_set = get_poor_clusters(clusters, centers, sizes, corpus, domains,
                                 coherence_threshold)  # find poor clusters to separate
    if (len(poor_set) == 0):
        return None
    poor_nodes = []
    forest = []
    for el in poor_set:
        temp_node = tree.get_node_with_id(el)
        forest.append(temp_node)
        poor_nodes.append(temp_node)
    while (True):
        if (len(poor_nodes) == 0):
            return forest
        event_node = poor_nodes.pop()
        event_id = event_node[tree.ID]
        if (logger):
            logger.info('separate event_id: %d' % event_id)
        new_clusters = dict()
        new_centers = dict()
        new_sizes = dict()
        print clusters[event_id]
        article_dic = get_similar_urls(clusters[event_id], corpus, identical_doc_threshold)  # duplicate detection,
        unique_arts = get_article_dic_key_aid(article_dic)  # map aid --> duplicate aids
        print unique_arts
        for aid in unique_arts:
            new_clusters[aid] = [aid]
            new_centers[aid] = corpus[aid]
            new_sizes[aid] = 1
        (new_clusters, new_centers, new_sizes) = separate_clustering(domains, event_id, new_clusters, new_centers,
                                                                     new_sizes, separate_thresholds, logger)
        if (len(new_clusters) == 1):  # only this cluster, cannot be reduceable ==> holde initial
            # this cluster cannot be reducable, --> remove
            print '%d remerges again !!!!' % event_id
            (f_ids, f_center, f_size) = remove_low_coherence(clusters[event_id], centers[event_id], sizes[event_id],
                                                             corpus, remove_at_insert_threshold)
            if f_ids:
                clusters[event_id] = f_ids
                centers[event_id] = f_center
                sizes[event_id] = len(f_ids)
        elif (len(new_clusters) > 1):
            del clusters[event_id]
            del sizes[event_id]
            del centers[event_id]
            temp_set = get_poor_clusters(new_clusters, new_centers, new_sizes, corpus, domains, coherence_threshold)
            for temp_id in new_clusters:
                clusters[temp_id] = get_full_articles(new_clusters[temp_id], unique_arts)
                centers[temp_id] = get_center(clusters[temp_id], corpus)
                sizes[temp_id] = len(clusters[temp_id])
                # add edge in tree
                new_node = tree.get_node_with_id(temp_id)
                tree.add_node(event_node, new_node)
                if (temp_id in temp_set):
                    poor_nodes.append(new_node)


def get_full_articles(ids, art_dic):
    result = []
    for key in ids:
        for aid in art_dic[key]:
            result.append(aid)
    return result


def detect_event_by_ids(ids, corpus, catids, domains):
    bucket = util.get_bucket_from_dic(catids)
    merge_thresholds = [resources.DAY_THRESHOLDS[-1]]
    (clusters, centers, sizes) = cluster_doc2.detect_event_with_buckets(bucket, corpus, resources.DAY_THRESHOLDS,
                                                                        merge_thresholds, online_mode=False)
    remove_small_events(domains, clusters, centers, sizes, resources.REMOVE_EVENT_THRESHOLD)
    return clusters


def recluster(clusters, corpus, coherences, day, logger=None):
    if logger:
        logger.info('start recluster event on : %s' % day)
    (e_ids, e_corpus, e_domains, e_catids) = e_data_reader_entity.read_all_data(day)
    r_clusters = dict()
    it = 0
    for ievent in clusters:
        if coherences[ievent] <= 0.55:
            tcorpus = dict()
            tcatids = dict()
            tdomains = dict()
            for i in clusters[ievent]:
                tcorpus[i] = n_get_all_data.add_entity_doc(corpus[i], e_corpus[i], n_util.alpha)
                tcatids[i] = e_catids[i]
                tdomains[i] = e_domains[i]
            sclusters = detect_event_by_ids(clusters[ievent], tcorpus, tdomains, tcatids)
            if len(sclusters) < 2:
                if (logger):
                    logger.info('Gui lai event cu!')
                r_clusters[ievent] = clusters[ievent]
            else:
                if (logger):
                    logger.info('Tach thanh %s event!' % len(sclusters))
                for i_v in sclusters:
                    r_clusters[i_v] = sclusters[i_v]
            it += 1
        else:
            if (logger):
                logger.info('Khong tach event!')
            r_clusters[ievent] = clusters[ievent]
    if (logger):
        logger.info('finish recluster event on: %s' % day)
    return r_clusters


def get_article_dic_key_aid(art_dic):
    """
    change key of dict to the first elements
    """
    result = dict()
    for key in art_dic:
        result[art_dic[key][0]] = art_dic[key]
    return result


def remove_low_coherence(cluster, center, size, corpus, hold_threshold):
    remove_count = 0
    hold_ids = []
    for aid in cluster:
        temp = util.get_similarity(center, corpus[aid])
        if (temp > hold_threshold):
            hold_ids.append(aid)
        else:
            print 'removed ids: %d with distance to center %f' % (aid, temp)
            remove_count += 1
    if (len(cluster) - remove_count < resources.REMOVE_EVENT_THRESHOLD):
        return (None, None, None)
    new_center = get_center(hold_ids, corpus)
    new_size = len(hold_ids)
    return (hold_ids, new_center, new_size)


def get_article_ids_str(url_dict):
    return url_dict


def sort_articles_id(event_id, article_ids, corpus, center, remove_threshold, logger=None):
    """
    compute coherence and sort articles by similarity to center, This function besides computing coherence, we also removing artilces that are so far away
    from the center, after removing these articles, if the number of remaining articles is less than remove_threshold -- not big enough to be event, 
    """
    id_num = len(article_ids)
    closes = dict()
    remove_count = 0
    remove_ids = ''
    coherence = 0.0
    for i in range(id_num):
        close = util.get_similarity(center, corpus[article_ids[i]])
        coherence += close
        closes[article_ids[i]] = close
        if (close < remove_threshold):
            remove_ids += '%d,' % article_ids[i]
            remove_count += 1

    if (id_num - remove_count < resources.REMOVE_EVENT_THRESHOLD):
        return (None, coherence)
    # get argmax
    coherence = coherence / float(id_num)
    d_view = util.sort_dic_des(closes)
    a_list = []
    if (coherence > resources.COHERENCE_THRESHOLD):  # hold this event
        if (logger):
            logger.info('remove: %d from %d: ...%s' % (remove_count, event_id, remove_ids))
        coherence = 0
        for (v, k) in d_view:
            if (v > remove_threshold):
                a_list.append(k)
                coherence += v
        coherence = coherence / len(a_list)
        if (logger):
            logger.info('coherence of %d is %f' % (event_id, coherence))
        return (a_list, coherence)
    else:  # consider separating this event
        if (logger):
            logger.info('coherence of %d is %f, size : %d, need separating' % (event_id, coherence, len(article_ids)))
        return (article_ids, coherence)


def get_center(a_ids, corpus):
    center_weights = []
    for did in a_ids:
        if did in corpus:
            center_weights.append((corpus[did], 1.0))
        else:
            print 'ATTENTION: ID %d HAS NO DICT IN CORPUS' % did
    center = util.update_center_with_center_weight_pair(center_weights)
    return center


def get_poor_clusters(clusters, centers, sizes, corpus, domains,
                      coherence_threshold=resources.COHERENCE_THRESHOLD):  # remove small and reorder articles_ids
    poor_set = Set()
    event_ids = clusters.keys()
    for event_id in event_ids:
        (a_ids, coherence) = sort_articles_id(event_id, clusters[event_id], corpus, centers[event_id], \
                                              resources.REMOVE_AT_INSERT_THRESHOLD)
        if a_ids:
            if (coherence < coherence_threshold):
                poor_set.add(event_id)
                print 'event: %d with coherence: %f < %f needs separating' % (event_id, coherence, coherence_threshold)
            else:
                temp_center = get_center(a_ids, corpus)
                hold = remove_short_events(domains, a_ids, temp_center, len(a_ids))
                if (not hold):
                    del clusters[event_id]
                    del centers[event_id]
                    del sizes[event_id]
                else:
                    sizes[event_id] = len(a_ids)
                    clusters[event_id] = a_ids
                    centers[event_id] = temp_center
        else:  # this event worth deleting
            del clusters[event_id]
            del centers[event_id]
            del sizes[event_id]
    return poor_set


def get_similar_urls(ids, corpus, sim_threshold):
    result = dict()
    id_num = len(ids)
    check = []
    for i in range(id_num):
        check.append(False)
    for i in range(id_num - 1):
        if (not check[i]):
            result[i] = [ids[i]]
            traverse_urls(i, result[i], id_num, ids, corpus, check, sim_threshold)
    return result


def traverse_urls(current_id, group, max_size, ids, corpus, check, sim_threshold):
    for i in range(current_id + 1, max_size):
        if (not check[i]):
            temp = util.get_similarity(corpus[ids[current_id]], corpus[ids[i]])
            if (temp > sim_threshold):
                check[i] = True
                group.append(ids[i])
                traverse_urls(i, group, max_size, ids, corpus, check, sim_threshold)


def separate_clustering(domains, event_id, clusters, centers, sizes, cluster_thresholds, logger=None):
    if (event_id == 62748029):
        print 'hit'
    if (logger):
        logger.info('separate %d ' % event_id)
    else:
        print 'separate %d ' % event_id
    (clusters, centers, sizes) = cluster_doc2.cluster_docs_with_thresholds(clusters, centers, sizes, cluster_thresholds,
                                                                           resources.CHANGE_THRESHOLD)
    if (logger):
        logger.info('after removing first: %d' % len(clusters))
    else:
        print 'after removing first: %d' % len(clusters)
    cluster_doc2.hierarchical_clustering(clusters, centers, sizes, resources.HIERARCHICAL_THRESHOLD_SEPARATE)
    print 'result from separating: %s' % str(clusters)
    (r_clusters, r_centers) = get_remove_small_events(domains, clusters, centers, sizes, 10)
    print 'small events: %s' % str(r_clusters)
    print 'remain events: %s' % str(clusters)
    leng = len(clusters)
    if (leng > 1):
        merge_rev_to_avail(r_centers, r_clusters, clusters, centers, sizes, 0.35)
    sub_ids = ''
    for key in clusters:
        sub_ids += '(%d,%d),' % (key, len(clusters[key]))
    if (logger):
        logger.info('event %d is separated into %d: %s' % (event_id, leng, sub_ids))
    print 'separating result: %s' % str(clusters)
    return (clusters, centers, sizes)


def merge_rev_to_avail(r_centers, r_clusters, clusters, centers, sizes, threshold):
    acc_dic = dict()
    for k2 in clusters:
        acc_dic[k2] = [(centers[k2], sizes[k2])]
    for k1 in r_centers:
        temp = 0
        cid = -1
        for k2 in clusters:
            dis = util.get_similarity(r_centers[k1], centers[k2])
            if (dis > temp):
                temp = dis
                cid = k2
        print temp
        if (temp > threshold):
            clusters[cid].append(k1)
            acc_dic[cid].append((r_centers[k1], len(r_clusters[k1])))
    # update all clusters
    for key in clusters:
        if (len(acc_dic[key]) > 1):
            centers[key] = util.update_center_with_center_weight_pair(acc_dic[key])
            sizes[key] = len(clusters[key])
            # update distance


def merge_rev_to_avail_realtime(r_centers, r_clusters, clusters, centers, sizes, threshold):
    acc_dic = dict()
    for k2 in clusters:
        acc_dic[k2] = [(centers[k2], sizes[k2])]
    for k1 in r_centers:
        temp = 0
        cid = -1
        for k2 in clusters:
            dis = util.get_similarity(r_centers[k1], centers[k2])
            if (dis > temp):
                temp = dis
                cid = k2
        # add small clusters into bigger clusters
        clusters[cid].append(k1)
        acc_dic[cid].append((r_centers[k1], len(r_clusters[k1])))
    # update all clusters
    for key in clusters:
        if (len(acc_dic[key]) > 1):
            centers[key] = util.update_center_with_center_weight_pair(acc_dic[key])
            sizes[key] = len(clusters[key])


def get_catid_of_cluster(aids, catid_dic):
    count_dic = dict()
    for aid in aids:
        catid = catid_dic[aid]
        if catid in count_dic:
            count_dic[catid] += 1
        else:
            count_dic[catid] = 1
    # find max of catid
    arg_max = util.get_max_key_dic(count_dic)
    return arg_max


def fetch_titles(cluster, save_path):
    articles = event_dao.get_articles_by_list_ids(cluster, 'all')
    art_dict = dict()
    for art in articles:
        art_dict[art[dao.ID]] = art
    save_file = codecs.open(save_path, 'w', encoding='utf8')
    for i in range(len(cluster)):
        art = art_dict[cluster[i]]
        save_file.write('%d,%s,%s\n' % (art[dao.ID], art[dao.TITLE], art[dao.URL]))
    save_file.close()


def write_coherences(path, coherences, sizes):
    f = open(path, 'w')
    d_view = util.sort_dic_des(coherences)
    for (v, k) in d_view:
        f.write('%d,%d,%f\n' % (k, sizes[k], v))
    f.close()


def save_tree(root_node, file_path):
    obstr = tree.get_str(root_node)
    f = open(file_path, 'w')
    f.write(obstr)
    f.close()

# corpus_path = '/media/khaimai/F8C6516EC6512DDE/khai_folder/projects/trend_py/trendy_ploy1.0/temp_data/server_emul2/total.dat'
# (corpus,domain_dic,cat_dic) = util.read_object(corpus_path)
