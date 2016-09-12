import detect_event_util,cluster_doc2,logging,util,real_time_resources2,resources
logger = logging.getLogger("real_time_detect_util.py")
logger.setLevel(logging.INFO)
util.set_log_file(logger,real_time_resources2.REAL_TIME_UTIL_LOG_PATH)
SMALL_EVENT_SEPARATE = 14
from sets import Set
def get_poor_cluster(clusters,centers,sizes,corpus,coherence_threshold):
    poor_set = list()
    good_dic = dict()
    for key in clusters:
        coherence = detect_event_util.get_coherence_of_cluster(clusters[key],corpus,centers[key])
        #recal_center = detect_event_util.get_center(clusters[key],corpus)
        #two_center_comp = util.get_similarity(centers[key],recal_center)
        #print '++++++++++++++++%d has the center difference: %f++++++++++++++++++++++++++++++++++++++'%(key,two_center_comp)
        logger.info('event_id: %d has coherence: %f'%(key,coherence))
        if (coherence < coherence_threshold):
            poor_set.append(key)
            good_dic[key] = coherence
            logger.info('need separating: key = %d, coherence = %f, size = %d'%(key,coherence,sizes[key]))
        else:
            logger.info('good-cluster: key = %d, coherence = %f,size = %d'%(key,coherence,sizes[key]))
            good_dic[key] = coherence
    return (poor_set,good_dic)

def separate_bad_cluster(aids,corpus,size_dic,thresholds,hierar_threshold):
    clusters = dict()
    centers = dict()
    sizes = dict()
    for aid in aids:
        clusters[aid] = [aid]
        centers[aid] = corpus[aid]
        sizes[aid] = size_dic[aid]
    if (len(aids) > 100):
        (clusters,centers,sizes) = cluster_doc2.cluster_docs_with_thresholds(clusters,centers,sizes,thresholds,1)
    cluster_doc2.hierarchical_clustering(clusters,centers,sizes,hierar_threshold)
    #remove_small_clusters(clusters,centers,sizes)
    return (clusters,centers,sizes)

def separate_bad_cluster2(aids,corpus,thresholds,hierar_threshold):
    article_dic = detect_event_util.get_similar_urls(aids,corpus,resources.IDENTICAL_DOC_THRESHOLD)
    unique_arts = detect_event_util.get_article_dic_key_aid(article_dic)
    logger.info('unique arts: %s'%str(unique_arts))
    clusters = dict()
    centers = dict()
    sizes = dict()
    for aid in unique_arts:
        clusters[aid] = [aid]
        centers[aid] = corpus[aid]
        sizes[aid] = 1
    #if (len(aids) > 100):
    (clusters,centers,sizes) = cluster_doc2.cluster_docs_with_thresholds(clusters,centers,sizes,thresholds)
    cluster_doc2.hierarchical_clustering(clusters,centers,sizes,hierar_threshold)
    logger.info('separating result: %s'%str(clusters))
    (r_clusters,r_centers,r_sizes) = detect_event_util.get_remove_small_events_with_catdic(clusters,centers,sizes,SMALL_EVENT_SEPARATE)
    logger.info('small cluster: %s'%str(r_clusters))
    logger.info('remain events: %s'%str(clusters))
    leng = len(clusters)
    if (leng > 1):
        detect_event_util.merge_rev_to_avail_realtime(r_centers,r_clusters,clusters,centers,sizes,0.35)
    #remove_small_clusters(clusters,centers,sizes)
    keys = clusters.keys()
    for key in keys:
        clusters[key] = get_full_elements(clusters[key],unique_arts)
        sizes[key] = len(clusters[key])
        centers[key] = detect_event_util.get_center(clusters[key],corpus)
    logger.info('final result: %s'%str(clusters))
    return (clusters,centers,sizes)

def get_full_elements(aids,aid_dic):
    result = Set()
    for aid in aids:
        #util.migrate_list2_to_list1(result,aid_dic[aid])
        for iden_aid in aid_dic[aid]:
            result.add(iden_aid)
    return list(result)

def get_min_sim(cluster,corpus,top_pair):
    distance = dict()
    leng = len(cluster)
    for i in range(leng-1):
        for j in range(i+1,leng):
            id1 = cluster[i]
            id2 = cluster[j]
            distance[(id1,id2)] = util.get_similarity(corpus[id1],corpus[id2])
    sort_pair = util.sort_dic_asc(distance)
    limit = min(top_pair,len(distance))
    return sort_pair[0:limit]

def cut_last_el_list(x):
    res = []
    leng = len(x)
    for i in range(leng-1):
        res.append(x[i])
    return res

def separate_cluster(clusters,centers,sizes,size_dic,corpus,coherence_threshold,threshold_levels,hier_threshold):
    (poor_set,good_dic) = get_poor_cluster(clusters,centers,sizes,corpus,coherence_threshold)
    while(True):
        if (len(poor_set) == 0):
            break
        key = poor_set.pop()
        thresholds = threshold_levels
        #print clusters[key]
        logger.info('separate: %d with aids: %s'%(key,str(thresholds)))
        (n_clusters,n_centers,n_sizes) = separate_bad_cluster(clusters[key],corpus,size_dic,thresholds,hier_threshold)
        logger.info('separating result: %s'%(str(n_clusters)))
        if (len(n_clusters) > 1):# separating is useless, they are merged again
            del clusters[key]
            del centers[key]
            del sizes[key]
            (npoor,ngood) = get_poor_cluster(n_clusters,n_centers,n_sizes,corpus,coherence_threshold)
            for el in ngood:
                clusters[el] = n_clusters[el]
                centers[el] = n_centers[el]
                sizes[el] = n_sizes[el] 
                good_dic[el] = ngood[el]
            for el in npoor:
                clusters[el] = n_clusters[el]
                centers[el] = n_centers[el]
                sizes[el] = n_sizes[el]
                poor_set.append(el)
    return (clusters,centers,sizes,good_dic)

def review_one_cluster(cluster_id,cluster,center,size,corpus,coherence_threshold,threshold_levels,hier_threshold):
    clusters = {cluster_id:cluster}
    centers = {cluster_id:center}
    sizes = {cluster_id:size}
    return review_clusters(clusters,centers,sizes,corpus,coherence_threshold,threshold_levels,hier_threshold)

TREND_ID_GROUP_THRESHOLD = 20#an events are separated by trend_ids, if small clusters have sizes less than this --> merge
def review_one_cluster_by_trend_ids(aids,doc2trend,corpus):
    trend_dic = dict()
    for aid in aids:    
        util.add_element_to_dic(trend_dic,aid,doc2trend[aid])
    logger.info('TREND_DIC RESULT: %s'%str(trend_dic))
    separate = detect_event_util.determine_separation(trend_dic,TREND_ID_GROUP_THRESHOLD,corpus)
    return (separate,trend_dic)
    

def review_clusters(clusters,centers,sizes,corpus,coherence_threshold,threshold_levels,hier_threshold):
    (poor_set,good_dic) = get_poor_cluster(clusters,centers,sizes,corpus,coherence_threshold)
    while(True):
        if (len(poor_set) == 0):
            break
        key = poor_set.pop()
        thresholds = threshold_levels
        #print clusters[key]
        logger.info('separate: %d with aids: %s'%(key,str(thresholds)))
        (n_clusters,n_centers,n_sizes) = separate_bad_cluster2(clusters[key],corpus,thresholds,hier_threshold)
        logger.info('separating result: %s'%(str(n_clusters)))
        if (len(n_clusters) > 1):# separating is useless, they are merged again
            del clusters[key]
            del centers[key]
            del sizes[key]
            (npoor,ngood) = get_poor_cluster(n_clusters,n_centers,n_sizes,corpus,coherence_threshold)
            for el in ngood:
                clusters[el] = n_clusters[el]
                centers[el] = n_centers[el]
                sizes[el] = n_sizes[el] 
                good_dic[el] = ngood[el]
            for el in npoor:
                clusters[el] = n_clusters[el]
                centers[el] = n_centers[el]
                sizes[el] = n_sizes[el]
                poor_set.append(el)
        else:
            logger.info('EVENT:%d IS MERGED AGAIN !!!'%key)
#    f_keys = clusters.keys()
#    ambiguous_trend_ids = Set()
#    for key in f_keys:
#        logger.info('CONSIDER SEPARATING %d BY TREND_ID'%key)
#        (separate,separate_result_dic) = review_one_cluster_by_trend_ids(clusters[key],doc2trend,corpus)
#        if separate:
#            separate_str = ''
#            del clusters[key]
#            del centers[key]
#            del sizes[key]
#            for tid in separate_result_dic:
#                new_id = separate_result_dic[tid][0]
#                #logger.info('separate: %d'%new_id)
#                separate_str += '%d,'%new_id
#                clusters[new_id] = separate_result_dic[tid]
#                centers[new_id] = detect_event_util.get_center(clusters[new_id],corpus)
#                sizes[new_id] = len(clusters[new_id])
#                ambiguous_trend_ids.add(tid)
#            logger.info('trend_dic: %s'%str(separate_result_dic))
#            logger.info('cluster: %d is separated into %s'%(key,separate_str))
#        else:
#            logger.info('cluster %d is merged again !!!!'%key)
    return (clusters,centers,sizes,good_dic)

def get_art_dic(aids,total_art_dic):
    res = dict()
    for aid in aids:
        cluster_doc2.merg_cat2_to_cat1(res,total_art_dic[aid])
    return res