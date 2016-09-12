# -*- coding: UTF-8 -*-
import os
import numpy as np
from gensim import corpora
import codecs
import datetime
import ast
from sets import Set
import operator
import logging
import shutil
import pickle

VN_DAU = u' ́ ̀ ̉ ̃ ̣'
VN_STR = [u'a', u'b', u'c', u'd', u'e', u'f', u'g', u'h', u'i', u'j', u'k', u'l', u'm', u'n', u'o', u'p', u'q', u'r',
          u's', u't', u'u', u'v', u'w', u'x', u'y', u'z', u'á', u'à', u'ả', u'ã', u'ạ', u'ă', u'ắ', u'ặ', u'ằ', u'ẳ',
          u'ẵ', u'â', u'ấ', u'ầ', u'ẩ', u'ẫ', u'ậ', u'đ', u'é', u'è', u'ẻ', u'ẽ', u'ẹ', u'ê', u'ế', u'ề', u'ể', u'ễ',
          u'ệ', u'í', u'ì', u'ỉ', u'ĩ', u'ị', u'ó', u'ò', u'ỏ', u'õ', u'ọ', u'ô', u'ố', u'ồ', u'ổ', u'ỗ', u'ộ', u'ơ',
          u'ớ', u'ờ', u'ở', u'ỡ', u'ợ', u'ú', u'ù', u'ủ', u'ũ', u'ụ', u'ư', u'ứ', u'ừ', u'ử', u'ữ', u'ự', u'ý', u'ỳ',
          u'ỷ', u'ỹ', u'ỵ']
VN_SET = Set()
for ch in VN_STR:
    VN_SET.add(ch)


def argsort(seq):
    return sorted(range(len(seq)), key=seq.__getitem__)


def save_object(save_path, obj):
    with open(save_path, 'wb') as f:
        pickle.dump(obj, f)


def read_object(read_path):
    f = open(read_path, 'r')
    obj = pickle.load(f)
    f.close()
    return obj


def get_in_where_str(id_list):
    res = ''
    leng = len(id_list)
    for i in range(leng - 1):
        res += '%d,' % id_list[i]
    res += '%d' % id_list[leng - 1]
    return res


def check_token(token):
    """ Check whether this token contains at least a number or digit
    if not, this token should be removed from vocabulary
    Return true if token is valid, false if not
    """
    if (len(token) == 1 and token in VN_DAU):
        return -1

    try:
        for i in range(len(token)):
            if (not check_valid_character(token[i])):
                return -1
        for i in range(len(token)):
            if ((token[i] in VN_SET)):
                return 1
    except:
        print token
        return -1
    return 0


def check_valid_character(ch):
    if ch in VN_DAU:
        return False
    return True


def read_vocab(vocab_path):
    vocab_map = dict()
    with open(vocab_path) as f:
        for line in f:
            temp = line.strip()
            temp2 = temp.split('\t')
            id = int(temp2[0])
            vocab_map[id] = temp2[1]
    return vocab_map


def normalize_token(token):
    """Normalize the token in VNese (to lower in Vietnam)
    Return the nomalized token
    """
    temp = token.lower()
    # remove nonalphabet or non-digit starting characters
    l = len(temp)
    position = 0
    for k in range(l):
        check = check_alphabet_digit(temp[k])
        if (check):
            position = k
            break
    if (position > 0):
        return temp[position:]
    return temp


def check_alphabet_digit(ch):
    if ch in VN_SET:
        return True
    if ch.isdigit():
        return True
    return False


def str_to_dict(dic_str):
    return ast.literal_eval(dic_str)


def save_text(text, save_path):
    """ write text to a file
    """
    f = open(save_path, 'w')
    f.write(text)
    f.close()
    print 'finish writting file: %s' % save_path


def get_word_frequency(doc):
    """ doc is a list, each element is a  tuple (id:frequency) 
    """
    s = ''
    for i in range(len(doc)):
        s += "%d:%d " % (doc[i])
    return s


def create_folder(directory):
    """
    create a new folder if not existing
    """
    if not os.path.exists(directory):
        os.makedirs(directory)
        print 'create a folder: %s' % directory


def convert_sequence_docfre(fpath, save_path):
    corpus = read_data_sequence(fpath)
    save_file = open(save_path, 'w')
    for doc in corpus:
        for pair in doc:
            save_file.write('%d:%d ' % pair)
        save_file.write('\n')
    save_file.close()


def read_data_sequence(fpath):
    f = open(fpath, 'r')
    corpus = []
    for line in f:
        ids = str_to_int_array(line, ' ')
        doc_fre = create_dic(ids)
        doc = []
        for key in doc_fre:
            doc.append((key, doc_fre[key]))
        corpus.append(doc)
    f.close()
    return corpus


def append_file(path, content):
    """
    append content to the bottom of the file
    """
    f = open(path, 'a')
    f.write('%s\n' % content)
    f.close()


def replace_dot(s):
    """
    replace . with _
    """
    s1 = s.replace('.', '_')
    return s1


def shuffle_file(folder, filename):
    """ shuffle all lines of a file
    """
    original_name = folder + '/' + filename
    temp_name = folder + '/' + 'temp.txt'
    f = open(original_name, 'r')
    res_file = open(temp_name, 'w')
    lines = f.readlines()
    leng = len(lines)
    per = np.random.permutation(leng)
    for i in per:
        line = lines[i]
        line = line.strip()
        res_file.write('%s\n' % line)
    f.close()
    res_file.close()
    os.remove(original_name)
    os.rename(temp_name, original_name)
    print 'finish shuffling file: %s' % original_name


def shuffle_folders(folder):
    """
    shuffle all files in a folder
    """
    for s in os.listdir(folder):
        if os.path.isfile(folder + '/' + s):
            shuffle_file(folder, s)
    print 'finish shuffling folder: %s' % s


def convert_docs_to_Blei_representation(folder, newfolder, fname):
    """
    convert a document from python form to blei's one, with the first number at each line is the number of unique terms
    """
    save_file = open(newfolder + '/' + fname, 'w')
    f = open(folder + '/' + fname, 'r')
    for line in f:
        temp = line.strip()
        leng = len(temp.split(' '))
        save_file.write('%d %s\n' % (leng, temp))
    f.close()
    save_file.close()
    print 'finish converting : %s' % fname


def append_to_bottom(fpath1, fpath2):
    """
    Merge file 2 to file 1
    """
    f1 = open(fpath1, 'a')
    f2 = open(fpath2, 'r')
    for line in f2:
        temp = line.strip()
        f1.write('%s\n' % temp)
    f1.close()
    f2.close()
    print 'complete merging file %s to file %s' % (fpath1, fpath2)


def merge_docs_in_folder(folder, save_path):
    """
    Merge all files in a folder to an ultimate file
    """
    save_file = open(save_path, 'w')
    for s in os.listdir(folder):
        if (os.path.isfile(folder + '/' + s)):
            append_to_bottom(save_path, folder + '/' + s)
    save_file.close()


def read_setting(setting_path):
    """
    read setting file containing parameters in the form: variable=12
    """
    res = dict()
    f = open(setting_path, 'r')
    for line in f:
        temp = line.strip()
        tg = temp.split('=')
        tg0 = tg[0].strip()
        tg1 = tg[1].strip()
        res[tg0] = tg1
    f.close()
    print 'finish reading setting file: %s' % setting_path
    return res


def save_setting(setting, fpath):
    """
    save setting (key-value) to file
    """
    f = open(fpath, 'w')
    for key in setting:
        f.write('%s=%s\n' % (key, setting[key]))
    f.close()
    print 'finished saving setting: ' + fpath


def create_dic(ids):
    res = dict()
    for wid in ids:
        if wid not in res:
            res[wid] = 1
        else:
            res[wid] += 1
    return res


def read_python_docs_file(fpath):
    """
    id word:frequency
    read docs from file as python-format (without length at first)
    the result has the form wordids and countids
    """
    wordids = list()
    countids = list()
    doc_ids = list()
    f = open(fpath, 'r')
    for line in f:
        temp = line.strip()
        if (len(temp) > 0):  # not an empty line
            tg = temp.split(' ')  # a:b
            ids = list()
            cts = list()
            doc_ids.append(int(tg[0]))
            for i in range(1, len(tg)):
                pair = tg[i].split(':')
                id1 = int(pair[0])
                count1 = int(pair[1])
                # print '%d:%d'%(id1,count1)
                ids.append(id1)
                cts.append(count1)
            wordids.append(ids)
            countids.append(cts)
    print 'finish reading doc_file in python representation: %s' % fpath
    f.close()
    return (doc_ids, wordids, countids)


def read_doc_without_id(fpath):
    """
    word:frequency
    read docs from file as python-format (without length at first)
    the result has the form wordids and countids
    """
    wordids = list()
    countids = list()
    f = open(fpath, 'r')
    for line in f:
        temp = line.strip()
        if (len(temp) > 0):  # not an empty line
            tg = temp.split(' ')  # a:b
            ids = list()
            cts = list()
            for i in range(0, len(tg)):
                pair = tg[i].split(':')
                id1 = int(pair[0])
                count1 = int(pair[1])
                # print '%d:%d'%(id1,count1)
                ids.append(id1)
                cts.append(count1)
            wordids.append(ids)
            countids.append(cts)
    print 'finish reading doc_file in python representation: %s' % fpath
    f.close()
    return (wordids, countids)


def read_corpus_without_id(fpath):
    (wordids, ctss) = read_doc_without_id(fpath)
    corpus = []
    for i in range(len(ctss)):
        wids = wordids[i]
        cts = ctss[i]
        doc = []
        for j in range(len(wids)):
            doc.append((wids[j], cts[j]))
        corpus.append(doc)
    return corpus


def read_doc_vector(fpath):
    """
    id word:frequency
    read docs from file as python-format (without length at first)
    the result has the form wordids and countids
    """
    wordids = list()
    countids = list()
    doc_ids = list()
    f = open(fpath, 'r')
    for line in f:
        temp = line.strip()
        if (len(temp) > 0):  # not an empty line
            tg = temp.split(' ')  # a:b
            ids = list()
            cts = list()
            doc_ids.append(int(tg[0]))
            for i in range(1, len(tg)):
                pair = tg[i].split(':')
                id1 = int(pair[0])
                count1 = float(pair[1])
                # print '%d:%d'%(id1,count1)
                ids.append(id1)
                cts.append(count1)
            wordids.append(ids)
            countids.append(cts)
    print 'finish reading doc_file in python representation: %s' % fpath
    f.close()
    return (doc_ids, wordids, countids)


def normalize_vector_inplace(v):
    """
    normalize sum = 1
    """
    sum_v = sum(v)
    for i in range(len(v)):
        v[i] = v[i] / sum_v


def normalize_matrix_in_place(matrix):
    (M, N) = matrix.shape
    for m in range(M):
        row = matrix[m]
        normalize_vector_inplace(row)
    return matrix


def compute_KL(x, y):
    """
    get KL distance between two distribution
    """
    size = len(x)
    result = 0
    for i in range(size):
        result += x[i] * np.log(x[i] / y[i])
    return result


def compute_symmetric_KL(x, y):
    m = (x + y) / 2
    s1 = compute_KL(x, m)
    s2 = compute_KL(y, m)
    return (s1 + s2) / 2


def get_new_normalized_vector(v):
    """
    get a sum-1 normalized vector from v
    """
    fv = np.copy(v)
    normalize_vector_inplace(fv)
    return fv


def save_norm_matrix(gamma, fpath):
    """
    save theta learned from the model to file
    """
    f = open(fpath, 'w')
    (R, C) = gamma.shape
    for r in range(R):
        gammad = get_new_normalized_vector(gamma[r])
        for i in range(len(gammad)):
            f.write(str(gammad[i]) + ' ')
        f.write('\n')
    f.close()
    print('finish saving matrix at: ' + fpath)


def list_to_str(x):
    res = ''
    leng = len(x)
    for i in range(leng):
        res += '%s,' % (str(x[i]))
    return res


def get_sorted_dates(date_strs):
    numb = len(date_strs)
    temp_list = list(date_strs)
    for i in range(numb - 1):
        for j in range(i + 1, numb):
            datei = temp_list[i]
            datej = temp_list[j]
            comp = compare_dates(datei, datej)
            if (comp == SMALLER):
                temp_list[i] = datej
                temp_list[j] = datei
    return temp_list


def migrate_list2_to_list1(list1, list2):
    for el in list2:
        list1.append(el)


def get_date_order(dates):
    numb = len(dates)
    temp_list = list(dates)
    index = [i for i in range(numb)]
    for i in range(numb - 1):
        for j in range(i + 1, numb):
            datei = temp_list[i]
            datej = temp_list[j]
            comp = compare_dates(datei, datej)
            if (comp == SMALLER):
                temp_list[i] = datej
                temp_list[j] = datei
                t_index = index[i]
                index[i] = index[j]
                index[j] = t_index
    return index


# get top - index of array
def top_of_indexarray(aa, k):
    """
    get top indices for an array
    """
    a = np.copy(aa)
    leng = len(a)
    index = range(leng)
    for i in range(k):
        for j in range(i + 1, leng):
            if (a[i] < a[j]):
                temp = a[j]
                a[j] = a[i]
                a[i] = temp
                tindex = index[j]
                index[j] = index[i]
                index[i] = tindex
    return index[0:k]


def get_list_top_indice(beta, top_k):
    """
    get top_k words from beta in each row
    """
    (K, W) = beta.shape
    result = []
    for i in range(0, K):
        top_ind = top_of_indexarray(beta[i], top_k)
        result.append(top_ind)
    return result


def save_2d_list(x, separator, fpath):
    """
    save a 2d_list, each element is separated by: seperator
    """
    f = open(fpath, 'w')
    for i in range(len(x)):
        for j in range(len(x[i])):
            f.write('%s%s' % (x[i][j], separator))
        f.write('\n')
    f.close()


def get_dictionary_map(dictpath):
    """
    read dictionary path, return w2id path
    """
    dic = corpora.Dictionary.load_from_text(dictpath)
    pairs = dic.items()
    vmap = dict()
    for p in pairs:
        vmap[int(p[0])] = p[1]
        # print p
    return vmap


def get_word_2_id(dicpath):
    w2id = dict()
    f = codecs.open(dicpath, 'r', 'utf-8')
    for line in f:
        temp = line.strip()
        tg = temp.split('\t')
        if (len(tg) == 3):
            w2id[tg[1]] = int(tg[0])
    f.close()
    return w2id


def get_today_str():
    return str(datetime.date.today())


def get_yesterday_str():
    yesterday = datetime.datetime.fromordinal(datetime.date.today().toordinal() - 1)
    temp = str(yesterday)
    return temp.split(' ')[0]


def get_previous_day(date_str):
    current_day = datetime.datetime.strptime(date_str, '%Y-%m-%d')
    yesterday = datetime.datetime.fromordinal(current_day.toordinal() - 1)
    yes_str = str(yesterday)
    return yes_str.split(' ')[0]


def get_ahead_day(date_str, day_num):
    current_day = datetime.datetime.strptime(date_str, '%Y-%m-%d')
    target_day = datetime.datetime.fromordinal(current_day.toordinal() + day_num)
    target_str = str(target_day)
    return target_str.split(' ')[0]


def get_past_day(date_str, interval):
    current_day = datetime.datetime.strptime(date_str, '%Y-%m-%d')
    yesterday = datetime.datetime.fromordinal(current_day.toordinal() - interval)
    yes_str = str(yesterday)
    return yes_str.split(' ')[0]


def get_dictionary_df(dic_path):
    """
    get df of words from dic_path
    """
    id2df = dict()
    f = codecs.open(dic_path, 'r', 'utf-8')
    line_count = 0
    for line in f:
        temp = line.encode('utf-8')
        temp = temp.strip()
        tg = temp.split('\t')
        if (len(tg) == 3):
            id1 = int(tg[0])
            count1 = int(tg[2])
            id2df[id1] = count1
        else:
            print 'error at line: %d' % line_count
        line_count += 1
    f.close()
    # print'finish getting id2df at: %s'%dic_path
    return id2df


def read_corpus_withid(data_path):
    (ids, wordids, ctss) = read_doc_vector(data_path)
    corpus = []
    for i in range(len(ids)):
        wids = wordids[i]
        cts = ctss[i]
        doc = []
        for j in range(len(wids)):
            doc.append((wids[j], cts[j]))
            corpus.append(doc)
    return (ids, corpus)


def str_to_int_array(strr, de=' '):
    line = strr.strip()
    array = line.split(de)
    result = []
    for i in range(len(array)):
        a = array[i]
        if (len(a) > 0):
            result.append(int(a))
    return result


def str_to_float_array(str, de=' '):
    line = str.strip()
    array = line.split(de)
    leng = len(array)
    result = [0] * leng
    count = 0
    for a in array:
        result[count] = float(a)
        count += 1
    return result


def import_all_files_to_corpus(folder_path):
    """
    read all text file in folder to a corpus
    """
    corpus = []
    doc_ids = []
    fnames = os.listdir(folder_path)
    fnames.sort()
    for fname in fnames:
        (ids, widss, ctss) = read_python_docs_file(folder_path + '/' + fname)
        doc_num = len(ids)
        for i in range(doc_num):
            doc_ids.append(ids[i])
            wids = widss[i]
            cts = ctss[i]
            doc = []
            for j in range(len(wids)):
                doc.append((wids[j], cts[j]))
            corpus.append(doc)
        print 'finish reading: %s' % fname
    print 'finish reading docs from folder: %s' % folder_path
    return (doc_ids, corpus)


def read_map_path(path):
    setting = read_setting(path)
    w2l = dict()
    l2w = dict()
    for s in setting:
        id_label = int(setting[s])
        w2l[s] = id_label
        l2w[id_label] = s
    return (w2l, l2w)


def get_stopwords(path):
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
    return stop_set;


def normalize_square(x):
    y = np.copy(x)
    sq = np.sqrt(y.dot(y))
    y = y / sq
    return y


def get_time_info():
    return str(datetime.now().time())


def sort_dic_des(dic):
    d_view = [(v, k) for k, v in dic.iteritems()]
    d_view.sort(reverse=True)
    return d_view


def sort_dic_asc(dic):
    d_view = [(v, k) for k, v in dic.iteritems()]
    d_view.sort()
    return d_view


def convert_dic_to_blei_form(vocab_path, save_path):
    f = open(vocab_path, 'r')
    save_file = open(save_path, 'w')
    for line in f:
        temp = line.strip()
        tg = temp.split('\t')
        save_file.write('%s\n' % tg[1])
    save_file.close()
    f.close()


def distance_between_date(date1_str, date2_str):
    date_format = '%Y-%m-%d'
    date1 = datetime.datetime.strptime(date1_str, date_format)
    date2 = datetime.datetime.strptime(date2_str, date_format)
    delta = date2 - date1
    return delta.days


def normalize_sum1_dict(dic):
    items = dic.items()
    item_sum = sum(items)
    for key in dic:
        dic[key] = float(dic[key]) / float(item_sum)


def nomalize_square_dic(dic):
    sum_square = 0.0
    for key in dic:
        sum_square += dic[key] * dic[key]
    for key in dic:
        dic[key] = float(dic[key]) / np.sqrt(sum_square)


def get_similarity(v1, v2):
    """
    compute the cosine between two unit vector v1 and v2, each is a dict()
    """
    res = 0.0
    for k1 in v1:
        if k1 in v2:
            res += v1[k1] * v2[k1]
    return res


def get_max_key_dic(d):
    key = max(d.iteritems(), key=operator.itemgetter(1))[0]
    return key


def merge_two_vocab_gensim(dic1_path, dic2_path, save_path, remove_threshold):
    """
    update dic 2 from dic 1, hold all the id of dic1_path, retain all dic1 words, remove lowfrequency dic2
    """
    vocab_1 = get_word_2_id(dic1_path)
    df_count1 = get_dictionary_df(dic1_path)

    vocab_2 = get_word_2_id(dic2_path)  # word --> id
    df_count2 = get_dictionary_df(dic2_path)  # id --> count
    for word in vocab_2:
        wid = vocab_2[word]
        count = df_count2[wid]
        if (word not in vocab_1):  # new words
            if (count > remove_threshold):
                new_id = len(vocab_1)
                vocab_1[word] = new_id  # new id for new word
                df_count1[new_id] = count
        else:  # old words
            old_id = vocab_1[word]
            df_count1[old_id] += count
    save_dictionary_as_gensim_form(vocab_1, df_count1, save_path)


def save_dictionary_as_gensim_form(vocab, df_count, save_path):
    f = codecs.open(save_path, 'w', encoding='utf8')
    for word in vocab:
        wid = vocab[word]
        word.encode('utf-8')
        count = df_count[wid]
        f.write('%d\t%s\t%d\n' % (wid, word, count))
    f.close()


def set_log_file(logger, file_path, mode='a'):
    handler = logging.FileHandler(file_path, mode)
    handler.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)


def set_log_console(logger):
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    ch.setFormatter(formatter)
    logger.addHandler(ch)


def get_date_from_str(date_str):
    return datetime.datetime.strptime(date_str, '%Y-%m-%d')


def read_top_word_file(fpath):
    result = []
    f = codecs.open(fpath, 'r', encoding='utf8')
    for line in f:
        temp = line.strip()
        tg = temp.split(':')
        tg_leng = len(tg)
        if (tg_leng > 2):
            print '==============%s' % temp
            score = float(tg[tg_leng - 1])
            index = len(temp) - len(tg[tg_leng - 1]) - 1
            word = temp[0:index]
            result.append((word, score))
        else:
            word = tg[0]
            score = float(tg[1])
            result.append((word, score))
    f.close()
    return result


def remove_folder_file(path):
    if (not os.path.exists(path)):
        return
    if (os.path.isfile(path)):
        os.remove(path)
    shutil.rmtree(path)


def get_argmax_list_index(x):
    a = np.array(x)
    return np.argmax(a)


def update_center_with_center_weight_pair(center_weights):
    res = dict()
    leng = 0.0
    for pair in center_weights:
        (center, weight) = pair
        accumulate_dict(res, center, weight)
        leng += weight
    normalize_norm2(res, leng)
    return res


def normalize_norm2(vecto, length):
    for key in vecto:
        vecto[key] = np.sqrt(vecto[key] / float(length))


def accumulate_dict(res, center, weight):
    for key in center:
        if key not in res:
            res[key] = weight * center[key] * center[key]
        else:
            res[key] += weight * center[key] * center[key]


def get_random_permute_list(x):
    size = len(x)
    permute = np.random.permutation(size)
    result = []
    for i in range(size):
        result.append(x[permute[i]])
    return result


def get_partition(leng, chunk_size):
    result = []
    number = leng / chunk_size
    for i in range(number):
        result.append((i * chunk_size, i * chunk_size + chunk_size))
    if (leng % chunk_size != 0):
        result.append((number * chunk_size, leng - 1))
    return result


def parse_id_from_url(url):
    last_index = len(url) - 1
    temp = url[last_index]
    result = ''
    while (True):
        if (not temp.isdigit()):
            last_index = last_index - 1
            temp = url[last_index]
        else:
            break
        if (last_index == -1):
            return ''
    while (True):
        result = url[last_index] + result
        last_index -= 1
        if (last_index == -1):
            return ''
        if (not url[last_index].isdigit()):
            break
    return result


def get_day_list(date1, date2):
    result = []
    temp_day = date1
    if (date1 == date2):
        return [date1]
    while (True):
        result.append(temp_day)
        temp_day = get_ahead_day(temp_day, 1)
        if (temp_day == date2):
            result.append(temp_day)
            break
    return result


def get_bucket_from_dic(dic):
    result = dict()
    for key in dic:
        if dic[key] not in result:
            result[dic[key]] = [key]
        else:
            result[dic[key]].append(key)
    return result


def get_id_from_url(url):
    index = len(url) - 5
    result = ''
    temp = url[index]
    while (True):
        result = temp + result
        index -= 1
        if (index == -1):
            break
        temp = url[index]
        if (not temp.isdigit()):
            break
    return result


def get_full_article_ids(art_dic_str):
    art_dic = str_to_dict(art_dic_str)
    ids = []
    for key in art_dic:
        for aid in art_dic[key]:
            ids.append(aid)
    return ids


def remove_all_files_in_folder(folder_path):
    files = os.listdir(folder_path)
    for f in files:
        os.remove(folder_path + '/' + f)


def convert_dic_article_to_list(art_dic_str):
    art_dic = str_to_dict(art_dic_str)
    orders = art_dic.keys()
    sorted_list = sorted(orders)
    res = ''
    for i in range(len(sorted_list)):
        temp = art_dic[sorted_list[i]]
        res += '%d,' % (temp[0])
    return res


def get_current_date():
    return datetime.datetime.now()


GREATER = 1
SMALLER = -1
EQUAL = 0


def compare_dates(datestr1, datestr2):
    if (datestr1 == datestr2):
        return EQUAL
    date1 = get_date_from_str(datestr1)
    date2 = get_date_from_str(datestr2)
    if (date1 > date2):
        return GREATER
    return SMALLER


def get_top_pair_from_dic(dic, k):
    s_v_k = sort_dic_des(dic)
    return s_v_k[0:k]


def normalize_divide_max_in_place(x):
    e_max = max(x)
    for i in range(len(x)):
        x[i] = float(x[i]) / float(e_max)


def get_nomalized_max(x):
    e_max = max(x)
    res = []
    for i in range(len(x)):
        res.append(float(x[i]) / float(e_max))
    return res


def gen_html_event(url_imgs, save_path, article_nums, url_article_strs):
    # url_article_format = 'http://analytics.admicro.vn/bigdata/vingo/article-event/details/0?article_ids={1}'
    f = codecs.open(save_path, 'w', 'utf-8')
    f.write('<html>\n<body>\n<table  BORDER="1">\n')
    row = '<tr><td><a href="http://analytics.admicro.vn/bigdata/vingo/article-event/details/0?article_ids={0}" target="_blank"> <img src="{1}" style="height: 200px;"></a> </td>  <td> <h1>{2}</h1> </td></tr>'
    for i in range(len(url_imgs)):
        f.write(row.format(url_article_strs[i], url_imgs[i], article_nums[i]))
    f.write('</table>\n</body>\n</html>\n')
    f.close()


def gen_html_event2(coherences, cluster_ids, url_imgs, save_path, article_nums, url_article_strs):
    # url_article_format = 'http://analytics.admicro.vn/bigdata/vingo/article-event/details/0?article_ids={1}'
    f = codecs.open(save_path, 'w', 'utf-8')
    f.write('<html>\n<body>\n<table  BORDER="1">\n')
    row = '<tr><td><a href="http://analytics.admicro.vn/bigdata/vingo/article-event/details/0?article_ids={0}" \
    target="_blank"> <img src="{1}" style="height: 200px;"></a> </td>  <td> <h1>{2}</h1> </td> <td> <h1>{3}</h1> </td>\
    <td> <h1>{4}</h1> </td> </td></tr>'
    for i in range(len(url_imgs)):
        f.write(row.format(url_article_strs[i], url_imgs[i], article_nums[i], cluster_ids[i], coherences[i]))
    f.write('</table>\n</body>\n</html>\n')
    f.close()


def days_between(d1, d2):
    d1 = datetime.datetime.strptime(d1, "%Y-%m-%d")
    d2 = datetime.datetime.strptime(d2, "%Y-%m-%d")
    return (d2 - d1).days


def get_dic_from_list(alist, field):
    result = dict()
    for el in alist:
        key = el[field]
        result[key] = el
    return result


def get_min_datetime(date_dic, keys):
    result = keys[0]
    temp = date_dic[keys[0]]
    for key in keys:
        if (temp > date_dic[key]):
            result = temp
    return result


def remove_element_in_dic(rm_keys, dic):
    for key in rm_keys:
        del dic[key]


def migrate_dic2_to_dic1(dic1, dic2):
    for key in dic2:
        dic1[key] = dic2[key]


def get_max_datetime(date_dic, keys):
    result = keys[0]
    temp = date_dic[keys[0]]
    for key in keys:
        if (temp < date_dic[key]):
            result = temp
    return result


def gen_python_run_sub_cluster(start, count):
    start = '2015-11-01'
    count = 0
    d1 = start
    while (True):
        d2 = get_ahead_day(d1, 1)
        print 'python sub_trend_cluster.py %s %s' % (d1, d2)
        d1 = get_ahead_day(d1, 2)
        count += 1
        if (count == 25):
            break


def list_to_dic_count(list1):
    result = dict()
    for el in list1:
        if el not in result:
            result[el] = 1
        else:
            result[el] += 1
    return result


def add_element_to_dic(res_dic, e_id, dic_id):
    if dic_id in res_dic:
        res_dic[dic_id].append(e_id)
    else:
        res_dic[dic_id] = [e_id]


def get_tfidf_from_tf_corpus(tf_corpus, epsilon):
    """
    get tf-idf representation from tf_corpus
    epsilon = * corpus_size
    """
    idf_map = get_idf_from_tf_corpus(tf_corpus)
    tfidf_dic = get_tf_idf_dic(tf_corpus, idf_map, epsilon, len(tf_corpus))
    return tfidf_dic


def get_tf_idf_dic(corpus, df_map, epsilon, corpus_size):
    """
    get if-idf representation from 
    corpus: tf-dic
    df_map: to calculate idf
    corpus_size: size of corpus
    epsilon: * corpussize
    """
    result = dict()
    for aid in corpus:
        if (len(corpus[aid]) > 0):
            temp_dic = get_tf_idf_rep(corpus[aid], df_map, corpus_size, epsilon)
            result[aid] = temp_dic
    return result


def get_idf_from_tf_corpus(tf_corpus):
    idf_map = dict()
    for aid in tf_corpus:
        aid_tf = tf_corpus[aid]
        for word in aid_tf:
            if word in idf_map:
                idf_map[word] += 1
            else:
                idf_map[word] = 1
    return idf_map


def get_tf_idf_rep(word_tf, df_map, corpus_size, epsilon=1.0):
    result = dict()
    sum_square = 0.0
    for wid in word_tf:
        df_val = 1.0
        if wid in df_map:
            df_val = df_map[wid]
        result[wid] = get_weight(word_tf[wid], df_val, corpus_size, epsilon)
        sum_square += result[wid] * result[wid]
    square = np.sqrt(sum_square)
    for wid in result:
        result[wid] = result[wid] / square
    return result


def get_weight(tf, df, corpus_size, epsilon, ofset=0):
    return (1 + np.log(tf)) * (np.log((corpus_size + ofset) * epsilon / (df + ofset)))


def merge_dic_count_2_to_1(dic1, dic2):
    for wid in dic2:
        if wid in dic1:
            dic1[wid] += dic2[wid]
        else:
            dic1[wid] = dic2[wid]


def get_bin_sim_min(v1, v2):
    count = 0.0
    for k1 in v1:
        if k1 in v2:
            count += 1
    min_leng = min(len(v1), len(v2))
    return (float(count) / float(min_leng))


def get_bin_sim_max(v1, v2):
    count = 0.0
    for k1 in v1:
        if k1 in v2:
            count += 1
    max_leng = max(len(v1), len(v2))
    return (float(count) / float(max_leng))


def get_bin_jaccard(v1, v2):
    count = 0.0
    union_keys = Set()
    for k1 in v1:
        union_keys.add(k1)
    for k2 in v2:
        union_keys.add(k2)
    for k1 in v1:
        if k1 in v2:
            count += 1
    max_leng = len(union_keys)
    return (float(count) / float(max_leng))


def print_list(l):
    result = ''
    for el in l:
        result += ' ' + el
    print result


def get_id2label_dic(l2id_dic):
    result = dict()
    for label in l2id_dic:
        for aid in l2id_dic[label]:
            result[aid] = label
    return result


def get_label2id_dic(id2label_dic):
    result = dict()
    for aid in id2label_dic:
        label = id2label_dic[aid]
        if label in result:
            result[label].append(aid)
        else:
            result[label] = [aid]
    return result


def get_bin_sim(v1, v2):
    if len(v1) == 0 or len(v2) == 0:
        return [0.0, 0.0, 0.0]
    x1 = get_bin_jaccard(v1, v2)
    x2 = get_bin_sim_max(v1, v2)
    x3 = get_bin_sim_min(v1, v2)
    return [x1, x2, x3]


def get_share_count(v1, v2):
    count = 0
    for k1 in v1:
        if k1 in v2:
            count += 1
    return count


def extract_dic(dic, keys):
    result = dict()
    for key in keys:
        result[key] = dic[key].copy()
    return result
