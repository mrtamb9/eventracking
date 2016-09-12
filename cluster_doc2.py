import util
import numpy as np
import dao
import codecs
from sets import Set
import operator
import time
import random
import tree


def get_top_words(center, N, vocab_map):
    d_view = [(v, k) for k, v in center.iteritems()]
    d_view.sort(reverse=True)
    res = []
    count = 0
    for v, k in d_view:
        if (count < N):
            w = vocab_map[k]
            res.append((w, v))
        count += 1
    return res


def merge_clusters2(clusters1, centers1, sizes1, clusters2, centers2, sizes2, cluster_threshold, instant_update=True, alpha=1):
    # alpha = 1.5
    keys = centers2.keys()
    acc_centers = dict()
    for key in centers1:
        acc_centers[key] = [(centers1[key], sizes1[key])]
    for key in keys:
        center = centers2[key]
        distance = compute_distance(center, centers1)
        if (len(distance) > 0):
            max_key = get_max_key_dic(distance)
            if (distance[max_key] > cluster_threshold):
                ids = clusters2.pop(key)
                for element in ids:
                    clusters1[max_key].append(element)
                sizes1[max_key] += sizes2[key]
                acc_centers[max_key].append((centers2[key], sizes2[key]))
            else:
                centers1[key] = centers2[key]
                clusters1[key] = clusters2[key]
                sizes1[key] = sizes2[key]
                acc_centers[key] = [(centers2[key], sizes2[key])]
        else:
            centers1[key] = centers2[key]
            clusters1[key] = clusters2[key]
            sizes1[key] = sizes2[key]
            acc_centers[key] = [(centers2[key], sizes2[key])]
    # update centers clusters1
    print 'update merging '
    for key in clusters1:
        if (len(acc_centers[key]) > 1):
            centers1[key] = util.update_center_with_center_weight_pair(acc_centers[key])


def merge_online_clusters(clusters, centers, sizes, threshold, minibatch):
    total_num = len(clusters)
    permute = np.random.permutation(total_num)
    # partition the data into buckets
    cluster_ids = clusters.keys()
    parts = util.get_partition(total_num, minibatch)
    res_clusters = dict()
    res_centers = dict()
    res_sizes = dict()
    mini = 0
    for (start, finish) in parts:
        print 'merging at minibatch: %d' % mini
        new_clusters = dict()
        new_centers = dict()
        new_sizes = dict()
        for i in range(start, finish):
            cluster_id = cluster_ids[permute[i]]
            new_clusters[cluster_id] = clusters[cluster_id]
            new_centers[cluster_id] = centers[cluster_id]
            new_sizes[cluster_id] = sizes[cluster_id]

        (new_clusters, new_centers, new_sizes) = reduce_clusters(new_clusters, new_centers, new_sizes, threshold)
        add_dictionary(res_clusters, new_clusters)
        add_dictionary(res_centers, new_centers)
        add_dictionary(res_sizes, new_sizes)
        mini += 1
    total_change = total_num - len(res_clusters)
    print 'total change for all minibatches: %d' % total_change
    return (res_clusters, res_centers, res_sizes)


def detect_event_with_buckets(buckets, corpus, thresholds, merge_thresholds, online_mode=True, batch_size=1000,
                              change_threshold=5):
    clusters = dict()
    centers = dict()
    sizes = dict()
    print buckets.keys()
    for key in buckets:
        print '=================merginng bucket: %s with size: %s===============' % (str(key), str(len(buckets[key])))
        n_clusters = dict()
        n_centers = dict()
        n_sizes = dict()
        for aid in buckets[key]:
            n_clusters[aid] = [aid]
            n_centers[aid] = corpus[aid]
            n_sizes[aid] = 1
        if (online_mode):
            (n_clusters, n_centers, n_sizes) = cluster_docs_with_thresholds_online(n_clusters, n_centers, n_sizes,
                                                                                   thresholds, change_threshold,
                                                                                   batch_size)
        else:
            (n_clusters, n_centers, n_sizes) = cluster_docs_with_thresholds(n_clusters, n_centers, n_sizes, thresholds)

        if (len(clusters) == -1):
            print 'merging bucket %s with size %s to total clusters' % (str(key), str(len(n_clusters)))
            print 'before merging size: %d' % (len(clusters))
            merge_clusters2(clusters, centers, sizes, n_clusters, n_centers, n_sizes, merge_thresholds[0])
            print 'after merging size: %d' % (len(clusters))
        else:
            add_dictionary(clusters, n_clusters)
            add_dictionary(centers, n_centers)
            add_dictionary(sizes, n_sizes)
    print 'before final clustering : %d' % (len(clusters))
    (clusters, centers, sizes) = cluster_docs_with_thresholds(clusters, centers, sizes, merge_thresholds)
    print 'after final clustering : %d' % (len(clusters))
    return (clusters, centers, sizes)


def cluster_docs_with_thresholds_online(clusters, centers, sizes, cluster_thresholds, change_threshold=5,
                                        minibatch=1000):
    for i in range(len(cluster_thresholds)):
        threshold = cluster_thresholds[i]
        while (True):
            before_size = len(clusters)
            t1 = time.time()
            (n_clusters, n_centers, n_sizes) = merge_online_clusters(clusters, centers, sizes, threshold, minibatch)
            t2 = time.time()
            after_size = len(n_clusters)
            clusters = n_clusters
            centers = n_centers
            sizes = n_sizes
            change = before_size - after_size
            print '========number clusters: %d with threshold: %f, change = %d, time: %d' % (
            after_size, threshold, change, t2 - t1)
            if (change < change_threshold):
                break
    return (clusters, centers, sizes)


def merge_cluster_by_tree(b_tree, buckets, corpus, thresholds, merge_thresholds, online_mode=False, batch_size=1000,
                          change_threshold=5, root=False):
    # TODO merging buckets (leaves) according to tree
    if (tree.is_leaf(b_tree)):
        print 'clustering at leaf: %s' % (str(b_tree[tree.ID]))
        key = int(b_tree[tree.ID])
        n_clusters = dict()
        n_centers = dict()
        n_sizes = dict()
        if (key not in buckets):  # if bucket is null
            print 'bucket %s has no elements' % (str(key))
            return (None, None, None)
        for aid in buckets[key]:
            n_clusters[aid] = [aid]
            n_centers[aid] = corpus[aid]
            n_sizes[aid] = 1
        if (online_mode):
            (n_clusters, n_centers, n_sizes) = cluster_docs_with_thresholds_online(n_clusters, n_centers, n_sizes,
                                                                                   thresholds, change_threshold,
                                                                                   batch_size)
        else:
            (n_clusters, n_centers, n_sizes) = cluster_docs_with_thresholds(n_clusters, n_centers, n_sizes, thresholds)
        print 'at node: %s: %d' % (str(b_tree[tree.ID]), len(n_clusters))
        return (n_clusters, n_centers, n_sizes)
    else:  # not a leaf
        children = b_tree[tree.CHILDRENT]
        clusters = dict()
        centers = dict()
        sizes = dict()
        for child in children:
            (n_clusters, n_centers, n_sizes) = merge_cluster_by_tree(child, buckets, corpus, thresholds,
                                                                     merge_thresholds, online_mode, batch_size,
                                                                     change_threshold)
            if (n_clusters):
                if (len(clusters) == 0):  # the first child
                    add_dictionary(clusters, n_clusters)
                    add_dictionary(centers, n_centers)
                    add_dictionary(sizes, n_sizes)
                else:
                    print 'merging at child: %s' % (child[tree.ID])
                    if (root):
                        print 'last merging ...'
                        # merge_clusters2(clusters,centers,sizes,n_clusters,n_centers,n_sizes,merge_thresholds[0])
                        add_dictionary(clusters, n_clusters)
                        add_dictionary(centers, n_centers)
                        add_dictionary(sizes, n_sizes)
                    else:
                        migrate_from_cluster_to_cluster(clusters, centers, sizes, n_clusters, n_centers, n_sizes,
                                                        merge_thresholds[0])
        if (root):
            (clusters, centers, sizes) = cluster_docs_with_thresholds(clusters, centers, sizes, merge_thresholds)
        print 'at node: %s: %d' % (str(b_tree[tree.ID]), len(clusters))
        return (clusters, centers, sizes)
    return None


def add_dictionary(dic1, dic2):
    for k2 in dic2:
        dic1[k2] = dic2[k2]


def reduce_clusters(clusters, centers, sizes, threshold):
    n_clusters = dict()
    n_centers = dict()
    n_sizes = dict()
    merge_clusters2(n_clusters, n_centers, n_sizes, clusters, centers, sizes, threshold)
    return (n_clusters, n_centers, n_sizes)


def cluster_docs_with_thresholds(clusters, centers, sizes, cluster_thresholds, change_threshold=5):
    new_clusters = dict()
    new_centers = dict()
    new_sizes = dict()
    for k in range(len(cluster_thresholds)):
        stop_loop = False
        while (not stop_loop):
            t1 = time.time()
            before_count = len(clusters)
            merge_clusters2(new_clusters, new_centers, new_sizes, clusters, centers, sizes, cluster_thresholds[k])
            after_count = len(new_clusters)
            clusters = new_clusters
            centers = new_centers
            sizes = new_sizes
            decrease_count = before_count - after_count
            new_clusters = dict()
            new_centers = dict()
            new_sizes = dict()
            t2 = time.time()
            print 'number of cluster: %d, decrease: %d, time: %d, threshold %f' % (
            after_count, decrease_count, t2 - t1, cluster_thresholds[k])
            if (decrease_count < change_threshold):
                stop_loop = True
    return (clusters, centers, sizes)


def merge_cluster(cluster1, cluster2):
    for item in cluster2:
        cluster1.append(item)


def consider_cluster_graph(aindex, distance, leng, threshold, current_group, check):
    for j in range(aindex):
        if (not check[j]) and (distance[(j, aindex)] > threshold):
            current_group.add(j)
            check[j] = True
            consider_cluster_graph(j, distance, leng, threshold, current_group, check)

    for j in range(aindex + 1, leng):
        if (not check[j]) and (distance[(aindex, j)] > threshold):
            current_group.add(j)
            check[j] = True
            consider_cluster_graph(j, distance, leng, threshold, current_group, check)


def graph_clustering(clusters, centers, sizes, threshold):
    distance = dict()
    ids = clusters.keys()
    leng = len(ids)
    for i in range(leng - 1):
        for j in range(i + 1, leng):
            distance[(i, j)] = util.get_similarity(centers[ids[i]], centers[ids[j]])
    check = dict()
    for i in range(leng):
        check[i] = False
    group = dict()
    for i in range(leng):
        if (not check[i]):
            check[i] = True
            group[i] = Set()
            group[i].add(i)
            consider_cluster_graph(i, distance, leng, threshold, group[i], check)
    result_clusters = dict()
    result_centers = dict()
    result_sizes = dict()
    for key in group:
        o_index = ids[key]
        current_group = group[key]
        center_weights = []
        group_size = 0;
        result_clusters[o_index] = []
        for index in current_group:
            center_weights.append((centers[ids[index]], sizes[ids[index]]))
            group_size += sizes[ids[index]]
            for element in clusters[ids[index]]:
                result_clusters[o_index].append(element)
        result_centers[o_index] = util.update_center_with_center_weight_pair(center_weights)
        result_sizes[o_index] = group_size
    return (result_clusters, result_centers, result_sizes)


def migrate_from_cluster_to_cluster(clusters1, centers1, sizes1, clusters2, centers2, sizes2, threshold):
    if (len(clusters1) == 0):  # cluster1 is empty
        clusters1 = clusters2
        centers1 = centers2
        sizes1 = sizes2
    keys = centers2.keys()
    acc_centers = dict()
    remain_ids = []
    for key in centers1:
        acc_centers[key] = [(centers1[key], sizes1[key])]
    for key in keys:
        center = centers2[key]
        distance = compute_distance(center, centers1)
        if (len(distance) > 0):
            max_key = get_max_key_dic(distance)
            if (distance[max_key] > threshold):
                ids = clusters2.pop(key)
                for element in ids:
                    clusters1[max_key].append(element)
                sizes1[max_key] += sizes2[key]
                acc_centers[max_key].append((centers2[key], sizes2[key]))
            else:
                remain_ids.append(key)
    # update centers clusters1
    print 'update merging '
    for key in clusters1:
        if (len(acc_centers[key]) > 1):
            centers1[key] = util.update_center_with_center_weight_pair(acc_centers[key])
    for key in remain_ids:
        centers1[key] = centers2[key]
        clusters1[key] = clusters2[key]
        sizes1[key] = sizes2[key]


def migrate_cluster_with_remains(clusters1, centers1, sizes1, clusters2, centers2, sizes2, threshold):
    keys = centers2.keys()
    acc_centers = dict()
    remain_ids = []
    for key in centers1:
        acc_centers[key] = [(centers1[key], sizes1[key])]
    for key in keys:
        center = centers2[key]
        distance = compute_distance(center, centers1)
        if (len(distance) > 0):
            max_key = get_max_key_dic(distance)
            if (distance[max_key] > threshold):
                ids = clusters2.pop(key)
                for element in ids:
                    clusters1[max_key].append(element)
                sizes1[max_key] += sizes2[key]
                acc_centers[max_key].append((centers2[key], sizes2[key]))
            else:
                remain_ids.append(key)
    # update centers clusters1
    print 'update merging '
    for key in clusters1:
        if (len(acc_centers[key]) > 1):
            centers1[key] = util.update_center_with_center_weight_pair(acc_centers[key])
    re_clusters = dict()
    re_sizes = dict()
    re_centers = dict()
    for key in remain_ids:
        re_clusters[key] = clusters2[key]
        re_sizes[key] = sizes2[key]
        re_centers[key] = centers2[key]
    return (re_clusters, re_centers, re_sizes)


def hierarchical_clustering_with_catids(clusters, centers, sizes, cat_dic, threshold):
    N = len(clusters)
    if (N < 2):
        return
    keys = clusters.keys()
    keys = sorted(keys)
    distance = dict()
    for i in range(N - 1):
        for j in range(i + 1, N):
            pair = (i, j)
            distance[pair] = util.get_similarity(centers[keys[i]], centers[keys[j]])
    while (True):
        if len(clusters) < 2:
            return
        (k1, k2) = util.get_max_key_dic(distance)
        if (distance[(k1, k2)] < threshold):
            print 'break when similarity: %f' % (distance[(k1, k2)])
            break
        id1 = keys[k1]
        id2 = keys[k2]
        # logger.info('merge: %d,%d: %f'%(id1,id2,distance[(k1,k2)]))
        centers[id1] = update_center(centers[id1], sizes[id1], centers[id2], sizes[id2], 1.0)
        sizes[id1] += sizes[id2]
        print 'sim = %f merge: %s with %s' % (distance[(k1, k2)], str(clusters[id1]), str(clusters[id2]))
        merge_cluster(clusters[id1], clusters[id2])
        merg_cat2_to_cat1(cat_dic[id1], cat_dic[id2])
        del centers[id2]
        del sizes[id2]
        del clusters[id2]
        del cat_dic[id2]

        for i in range(k2):
            pair = (i, k2)
            if pair in distance:
                del distance[pair]
        for i in range(k2 + 1, N):
            pair = (k2, i)
            if pair in distance:
                del distance[pair]
        for i in range(k1):  # update distance of (i,k1)
            pair = (i, k1)
            if pair in distance:  # update
                distance[pair] = util.get_similarity(centers[keys[i]], centers[keys[k1]])
        for i in range(k1 + 1, N):
            pair = (k1, i)
            if pair in distance:
                distance[pair] = util.get_similarity(centers[keys[i]], centers[keys[k1]])


def merg_cat2_to_cat1(cat1, cat2):
    for key in cat2:
        if key in cat1:
            cat1[key] += cat2[key]
        else:
            cat1[key] = cat2[key]


def hierarchical_clustering(clusters, centers, sizes, threshold):
    N = len(clusters)
    if (N < 2):
        return
    keys = clusters.keys()
    keys = sorted(keys)
    distance = dict()
    for i in range(N - 1):
        for j in range(i + 1, N):
            pair = (i, j)
            distance[pair] = util.get_similarity(centers[keys[i]], centers[keys[j]])
    while (True):
        if len(clusters) < 2:
            return
        (k1, k2) = util.get_max_key_dic(distance)
        if (distance[(k1, k2)] < threshold):
            print 'break when similarity: %f' % (distance[(k1, k2)])
            break
        id1 = keys[k1]
        id2 = keys[k2]
        # logger.info('merge: %d,%d: %f'%(id1,id2,distance[(k1,k2)]))
        centers[id1] = update_center(centers[id1], sizes[id1], centers[id2], sizes[id2], 1.0)
        sizes[id1] += sizes[id2]
        print 'sim = %f merge: %s with %s' % (distance[(k1, k2)], str(clusters[id1]), str(clusters[id2]))
        merge_cluster(clusters[id1], clusters[id2])

        del centers[id2]
        del sizes[id2]
        del clusters[id2]

        for i in range(k2):
            pair = (i, k2)
            if pair in distance:
                del distance[pair]
        for i in range(k2 + 1, N):
            pair = (k2, i)
            if pair in distance:
                del distance[pair]
        for i in range(k1):  # update distance of (i,k1)
            pair = (i, k1)
            if pair in distance:  # update
                distance[pair] = util.get_similarity(centers[keys[i]], centers[keys[k1]])
        for i in range(k1 + 1, N):
            pair = (k1, i)
            if pair in distance:
                distance[pair] = util.get_similarity(centers[keys[i]], centers[keys[k1]])


def get_related_clusters(data_dic, centers, threshold, relate_threshold=0.3):
    if (len(centers) == 0):  # for the first time
        return ({}, Set(), data_dic.keys())
    result_dic = dict()
    remain_ids = []
    addition_set = Set()
    for item_id in data_dic:
        v = data_dic[item_id]
        distances = compute_distance(v, centers)
        if (len(distances) > 0):
            for item_key in distances:
                if (distances[item_key] >= relate_threshold):
                    if item_key not in addition_set:
                        addition_set.add(item_key)
            ################################### get max to add
            max_key = get_max_key_dic(distances)
            if (distances[max_key] >= threshold):
                if (max_key in result_dic):
                    result_dic[max_key].append((item_id, distances[max_key]))
                else:
                    result_dic[max_key] = [(item_id, distances[max_key])]
            else:
                remain_ids.append(item_id)
    # remove all items in result_dic in addition_set
    res_addition_set = Set()
    for el in addition_set:
        if el in result_dic:
            res_addition_set.add(el)
    return (result_dic, res_addition_set, remain_ids)


def get_related_clusters_for_data(data_dic, centers, threshold):
    if (len(centers) == 0):  # for the first time
        return ({}, data_dic.keys())
    result_dic = dict()
    remain_ids = []
    for item_id in data_dic:
        v = data_dic[item_id]
        distances = compute_distance(v, centers)
        if (len(distances) > 0):
            max_key = get_max_key_dic(distances)
            if (distances[max_key] >= threshold):
                if (max_key in result_dic):
                    result_dic[max_key].append((item_id, distances[max_key]))
                else:
                    result_dic[max_key] = [(item_id, distances[max_key])]
            else:
                remain_ids.append(item_id)
    return (result_dic, remain_ids)


def update_center(center1, size1, center2, size2, alpha):
    res = dict()
    accumulate_dict(res, center1, size1)
    accumulate_dict(res, center2, size2, alpha)
    normalize_norm2(res, size1 + size2 * alpha)
    return res


def normalize_norm2(vecto, size):
    for key in vecto:
        vecto[key] = np.sqrt(vecto[key] / float(size))


def accumulate_dict(res, center, size, alpha=1):
    for key in center:
        if key not in res:
            res[key] = size * center[key] * center[key] * alpha
        else:
            res[key] += size * center[key] * center[key] * alpha


def compute_distance(v, centers):
    """
    compute the distance from v to all centers
    """
    distance = dict()
    for k in centers:
        center = centers[k]
        temp_dis = get_similarity(center, v)
        distance[k] = temp_dis
    return distance


def create_fetch_query(aid):
    query = 'select %s from %s where %s = %d' % (dao.CONTENT, dao.NEWS_TABLE, dao.ID, aid)
    return query


def fetch_documents_from_db(cluster_ids, save_path):
    conn = dao.get_connection()
    cur = conn.cursor()
    save_file = codecs.open(save_path, 'w', encoding='utf8')
    for did in cluster_ids:
        query = create_fetch_query(did)
        cur.execute(query)
        row = cur.fetchone()
        text = row[0]
        text.encode('utf-8')
        save_file.write('=========================================================: %d \n' % did)
        save_file.write('%s\n' % text)
    dao.free_connection(conn, cur)


def visualize_centre(vocab_map, fpath, center, N=40):
    # print vocab_map
    d_view = [(v, k) for k, v in center.iteritems()]
    d_view.sort(reverse=True)
    save_file = codecs.open(fpath, 'w', encoding='utf8')
    count = 0
    for v, k in d_view:
        if (count < N and k >= 0):
            try:
                w = vocab_map[k]
                save_file.write('%s:%5f\n' % (w, v))
            except:
                print k
            count += 1
        if (count == N):
            break
    save_file.close()


def get_max_key_dic(d):
    key = max(d.iteritems(), key=operator.itemgetter(1))[0]
    return key


def append_file_with_line(fpath, content):
    f = open(fpath, 'a')
    f.write('%s' % content)


def get_doc_vector(wids, scores):
    result = dict()
    for i in range(len(wids)):
        result[wids[i]] = scores[i]
    return result


def update_centers(cluster, vecto_map):
    """
    return a new center for the cluster
    """
    wids = dict()
    size = len(cluster)
    for k in range(size):
        docid = cluster[k]
        v_map = vecto_map[docid]

        for key in v_map:
            if (key not in wids):
                wids[key] = v_map[key] * v_map[key]
            else:
                wids[key] += v_map[key] * v_map[key]
    for key in wids:
        wids[key] = np.sqrt(wids[key] / float(size))
    return wids


def get_similarity(v1, v2):
    res = 0.0
    for k1 in v1:
        if k1 in v2:
            res += v1[k1] * v2[k1]
    return res


def get_close_index(v, centers):
    temp_key = 0
    temp_score = -1
    for key in centers:
        dis = util.get_similarity(v, centers[key])
        if (dis > temp_score):
            temp_key = key
            temp_score = dis
    return temp_key


def hierarchical_clustering_corpus(corpus, threshold):
    clusters = dict()
    sizes = dict()
    centers = dict()
    for aid in corpus:
        clusters[aid] = [aid]
        sizes[aid] = 1
        centers[aid] = corpus[aid].copy()
    hierarchical_clustering(clusters, centers, sizes, threshold)
    return (clusters, centers)


def hierarcical_cluster_doc_with_comparator(aids, comparator, threshold):
    if (len(aids) == 1):
        result = dict()
        result[aids[0]] = aids
        return result
    clusters = dict()
    for aid in aids:
        clusters[aid] = [aid]
    keys = clusters.keys()
    keys = sorted(keys)
    distance = dict()
    N = len(aids)
    for i in range(N - 1):
        for j in range(i + 1, N):
            pair = (i, j)
            distance[pair] = comparator.get_similarity(keys[i], keys[j])
    while (True):
        if len(clusters) < 2:
            return clusters
        (k1, k2) = util.get_max_key_dic(distance)
        if (distance[(k1, k2)] < threshold):
            # print 'break when similarity: %f'%(distance[(k1,k2)])
            break
        id1 = keys[k1]
        id2 = keys[k2]
        # print 'merge: %d,%d: %f'%(id1,id2,distance[(k1,k2)])
        #        centers[id1] = update_center(centers[id1],sizes[id1],centers[id2],sizes[id2],1.0)
        #        sizes[id1] += sizes[id2]
        #        print 'sim = %f merge: %s with %s'%(distance[(k1,k2)],str(clusters[id1]),str(clusters[id2]))
        merge_cluster(clusters[id1], clusters[id2])

        #        del centers[id2]
        #        del sizes[id2]
        del clusters[id2]

        for i in range(k2):
            pair = (i, k2)
            if pair in distance:
                del distance[pair]
        for i in range(k2 + 1, N):
            pair = (k2, i)
            if pair in distance:
                del distance[pair]
        for i in range(k1):  # update distance of (i,k1)
            pair = (i, k1)
            if pair in distance:  # update
                distance[pair] = get_sim_of_two_cluster(clusters[keys[i]], clusters[keys[k1]],
                                                        comparator)  # util.get_similarity(centers[keys[i]],centers[keys[k1]])
        for i in range(k1 + 1, N):
            pair = (k1, i)
            if pair in distance:
                distance[pair] = get_sim_of_two_cluster(clusters[keys[i]], clusters[keys[k1]],
                                                        comparator)  # util.get_similarity(centers[keys[i]],centers[keys[k1]])
    return clusters


def get_sim_of_two_cluster(cluster1, cluster2, comparator):
    count = 0.0
    aver_sum = 0.0
    for aid1 in cluster1:
        for aid2 in cluster2:
            aver_sum += comparator.get_similarity(aid1, aid2)
            count += 1
    return aver_sum / count


def cluster_doc_with_comparator(ids, comparator, threshold):
    leng = len(ids)
    merge_dic = dict()
    for i in range(leng):
        merge_dic[ids[i]] = []
    for i in range(leng - 1):
        for j in range(i + 1, leng):
            aid1 = ids[i]
            aid2 = ids[j]
            distance = comparator.get_similarity(aid1, aid2)
            if (distance > threshold):
                # print 'merging: %d with %d with distance: %f'%(aid1,aid2,distance)
                merge_dic[aid1].append(aid2)
                merge_dic[aid2].append(aid1)
    id_group = get_cluster_from_merge_dic(merge_dic)
    result = dict()
    for key in id_group:
        temp_set = id_group[key]
        temp_list = list(temp_set)
        result[temp_list[0]] = temp_list
    return result


def group_vecto_with_sim_function(corpus, sim_def, threshold):
    ids = corpus.keys()
    leng = len(corpus)
    merge_dic = dict()
    for i in range(leng):
        merge_dic[ids[i]] = []
    for i in range(leng - 1):
        for j in range(i + 1, leng):
            aid1 = ids[i]
            aid2 = ids[j]
            distance = sim_def(corpus[aid1], corpus[aid2])
            if (distance > threshold):
                print 'merging: %d with %d with distance: %f' % (aid1, aid2, distance)
                merge_dic[aid1].append(aid2)
                merge_dic[aid2].append(aid1)
    id_group = get_cluster_from_merge_dic(merge_dic)
    result = dict()
    for key in id_group:
        temp_set = id_group[key]
        temp_list = list(temp_set)
        result[temp_list[0]] = temp_list
    return result


def get_cluster_from_merge_dic(merge_dic):
    keys = merge_dic.keys()
    check = dict()
    for key in keys:
        check[key] = False
    group = dict()
    for i in range(len(keys)):
        if (not check[keys[i]]):
            group[i] = Set()
            group[i].add(keys[i])
            consider_point(keys[i], merge_dic, group[i], check)
    return group


def consider_point(aid, merge_dic, group, check):
    adj_ids = merge_dic[aid]
    for el in adj_ids:
        if (not check[el]):
            group.add(el)
            check[el] = True
            consider_point(el, merge_dic, group, check)


def kmean_cluster(v, k, max_iter=20, change=15):
    ids = v.keys()
    centers = dict()
    ran_center = []
    while (len(ran_center) < k):
        order = random.randint(0, len(ids))
        if order not in ran_center:
            ran_center.append(order)
    for i in range(k):
        centers[i] = v[ids[ran_center[i]]]
    count = 0
    pre_size = dict()
    while (True):
        print '============= iter: %d' % count
        clusters = dict()
        for key in centers:
            clusters[key] = []
        # assign clusters
        for aid in ids:
            max_clus = get_close_index(v[aid], centers)
            clusters[max_clus].append(aid)
        # update centers
        total_change = 0
        for key in clusters:
            centers[key] = update_centers(clusters[key], v)
            curr_leng = len(clusters[key])
            print 'cluster: %d has %d' % (key, curr_leng)
            if (count > 0):
                total_change += abs(curr_leng - pre_size[key])
            pre_size[key] = curr_leng
        print 'total change = %d' % total_change
        if (count > 1 and total_change < change):
            break
        count += 1
        if (count == max_iter):
            break
    return clusters
