# -*- coding: utf-8 -*-
"""
Created on Fri Nov 13 16:36:14 2015

@author: bolx
"""
import numpy as np
import os
import sys

from gensim import corpora

import e_util

sys.path.insert(0, '../')


def read_all_data(day, data_path=None, idf_day=None):
    print 'read all data from day: %s' % day
    if not data_path:
        data_path = e_util.get_data_path(day)
    if not idf_day:
        idf_day = day
    corpus_size = read_corpus_size(idf_day)
    dictionary_path = e_util.get_dictionary_path(idf_day)
    dictionary = corpora.Dictionary.load_from_text(dictionary_path)
    dictionary.num_docs = corpus_size
    idfs = get_idf_of_diction(dictionary)
    corpus = dict()  # map id and vector
    # ids = []
    (docs, ids, domains, catids) = read_docs(data_path)
    for idoc in ids:
        doc = docs[idoc]
        bow = dictionary.doc2bow(doc)
        tf_idf_doc = dict()
        for w in bow:
            score = (1 + np.log(float(w[1]))) * idfs[w[0]]
            tf_idf_doc[w[0]] = score
        corpus[idoc] = normalize_square(tf_idf_doc)
    print 'finish reading all data on day: %s' % day
    return ids, corpus, domains, catids


# def read_realtime():
#     return read_all_data(util.get_today_str(), e_util.get_real_data(), util.get_yesterday_str())


def normalize_square(vec):
    sum_q = 0
    for i in vec:
        sum_q += vec[i] * vec[i]
    y = dict()
    for i in vec:
        y[i] = vec[i] / np.sqrt(sum_q)
    return y


def read_corpus_size(day):
    fpath = e_util.get_corpus_size_path(day)
    corpus_size = 0
    if os.path.exists(fpath):
        f = open(fpath, 'r')
        line = f.readline()
        corpus_size = int(line)
        f.close()
    else:
        print 'corpus_size path does not exist: %s' % fpath
    return corpus_size


# def save_corpus_size(num, day):
#     save_corpus_size_f(num, e_util.get_corpus_size_path(day))


# def save_corpus_size_f(num, fpath):
#     f = open(fpath, 'w')
#     f.write(str(num))
#     f.close()


def read_docs(fpath):
    f = open(fpath, 'r')
    id_list = []
    docs = dict()
    domains = dict()
    catids = dict()
    for line in f:
        s = line.strip().split('\t')
        idd = int(s[0].strip())
        if len(s) < 4:
            #        print line
            continue
        words = [w.strip() for w in s[3].split(',')]  # .decode('utf-8').upper()
        #      print words[0]
        id_list.append(idd)
        docs[idd] = words
        domains[idd] = s[1]
        catids[idd] = s[2]
    f.close()
    return docs, id_list, domains, catids


def get_idf_of_diction(dictionary):
    idfs = dict()
    df = dictionary.dfs
    for w in df:
        idfs[w] = np.log(float(dictionary.num_docs) / float(df[w]))
    return idfs

# (ids, corpus, domains) = read_all_data('2015-08-01')
# for i in ids:
#    print i, '\t', domains[i], '\n',corpus[i]


if __name__ == "__main__":
    read_all_data("2016-08-25")