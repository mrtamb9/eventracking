# -*- coding: utf-8 -*-
import event_dao,util,resources,data_reader,os,dao,urllib
import trend_util
from sets import Set
TABLE_NAME  = 'real_sub_trend'
data_path = '/media/khaimai/F8C6516EC6512DDE/khai_folder/projects/trend_py/trendy_ploy1.0/test/sub_cluster' + '/data2'

optimal_epsilon = 1.0#52.8
en_optimal_epsilon = 2#46.2
no_entity_penalty = 0.90# set a penalty for article not containing any entity
MIN_MISS = 0.05

hierarchical_threshold = 0.65
hierarchical_threshold2 = 0.50
coherence_threshold = 0.65
COHERENCE_THRESHOLD = 0.5
ratio_threshold = 1.15
coherence_threshold2 = 0.3
COHERENCE_THRESHOL_RATIO = 3.0

one_threshold = 0.37
threshold_levels = [0.8,0.75,0.70,0.675,0.65,0.625,0.60,0.575,0.55]
threshold_levels2 = [0.7,0.6,0.5,0.4,0.3,0.25]

MAX_ADD = 0
optimal_alpha = 0.15

title_weight = 1.0
description_weight = 1.0
en_title_weight = 1.0
en_descript_weight = 1.0

A_COHE = 1.8116
#B_COH = 0.056
B_COH = -0.084
CHECK_SIZE_A =0.47
CHECK_SIZE_B = 0.1
ONE_RATIO_MAX = 5
ONE_RATIO_AVG = 5
ONE_SIZE_THRESHOLD = 5
ONE_THRESHOLD = 1.0#0.30
MAX_DOC_SEN_NUM = 4000

tit_url_template = 'http://10.3.24.83:8080/trending/api/tokenizer/title/?id'
ent_title_template = 'http://10.3.24.83:8080/trending/api/entity/title/?id'
ent_description_template = 'http://10.3.24.83:8080/trending/api/entity/description/?id'
des_url_template = 'http://10.3.24.83:8080/trending/api/tokenizer/description/?id'
sentence_url_template = 'http://10.3.24.83:8080/trending/api/entity/content/?id'
BLACK_SET = Set('vtv.vn,vntinnhanh.vn,tpo,vov.vn,đspl,tto,bizlive,ktđt,giadinhnet,antđ,dân_việt,plo,ihay,thethaovanhoa.vn,bongda.com.vn,cadn.com.vn'.split(','))
TITLE_TYPE = 'title'
DESCRIPTION_TYPE = 'description'
class Document:
    def __init__(self,aid,title,title_prune,description,sentences,en_sentences,domain):#,en_title,en_descript
        self.aid = aid
        self.title = title
        self.description = description
        self.sentences  = sentences
        self.en_sentences = en_sentences
        
        self.con_tf = get_context_document_tf(sentences)
        self.en_tf  = get_entity_document_tf(en_sentences)
        self.title = util.list_to_dic_count(title)
        self.title_prune = util.list_to_dic_count(title_prune)
        self.description = util.list_to_dic_count(description)     
        self.title_word = get_all_tokens(self.title)
        #print '==================AID: %d ==================='%aid
        temp = expand_title_and_description2(title_prune,description,sentences)#expand_title(title_prune,sentences)
        self.expan_title = util.list_to_dic_count(temp)
        self.expan_title2 = get_filter_content_tf_with_title(title_prune,description,sentences)
        self.domain = domain
        self.sen_dic = get_sentence_dic(sentences,aid)
#        self.en_title = en_title
#        self.en_descript = en_descript
#        prune_en_title = get_entity_from_sen(en_title)
#        prune_en_descpt = get_entity_from_sen(en_descript)
#        prune_sen_ens = prune_entities(en_sentences)
#        selected_indexs = expand_tile_ens(prune_en_title,prune_en_descpt,sentences)
#        print '==========================extracted sentences from %d=================='%aid
#        util.print_list(title)
#        util.print_list(en_title)
#        util.print_list(en_descript)
#        print 'extract:'
#        for index in selected_indexs:
#            util.print_list(sentences[index])
    def get_title_id(self):
        return get_sentence_id(self.aid,0)

def get_sentence_dic(sentences,doc_id):
    sen_dic = dict()
    for i in range(len(sentences)):
        sen_id = get_sentence_id(doc_id,i)
        sen_dic[sen_id] = sentences[i]
    return sen_dic
    
def get_sentence_id(doc_id,sen_order):
    sen_id = doc_id * MAX_DOC_SEN_NUM + sen_order
    return sen_id
    
def get_doc_id_and_sen_order(sen_id):
    doc_id = sen_id/MAX_DOC_SEN_NUM
    sen_order = sen_id - doc_id*MAX_DOC_SEN_NUM
    return (doc_id,sen_order)

def expand_tile_ens(en_title,en_descrpt,sentences):
    en_set = Set()
    for ens in en_title:
        en_set.add(ens)
    for ens in en_descrpt:
        en_set.add(ens)
    sen_num = len(sentences)
    selected_indexs = []
    for i in range(sen_num):
        for emp_en in sentences[i]:
            for r_en in en_set:
                if emp_en in r_en:
                    selected_indexs.append(i)
                    break
    return selected_indexs

def prune_entities(en_sens):
    result = []
    sen_num = len(en_sens)
    for i in range(sen_num):
        temp = get_entity_from_sen(en_sens[i])
        result.append(temp)
    return result

def expand_title(title_wid, sentences):
    #print '==========================================================================='
    #util.print_list(title_wid)
    result = []
    util.migrate_list2_to_list1(result,title_wid)
    word_set = Set(title_wid)
    for i in range(1,len(sentences)):
        wids = sentences[i]
        for wid in wids:
            if wid in word_set:
                util.migrate_list2_to_list1(result,wids)
                #util.print_list(wids)
                break
    return result

def accumulate_dic_count(dic_cout,word_count,weight=1.0):
    for word in word_count:
        if word in dic_cout:
            dic_cout[word] += word_count[word] * weight
        else:
            dic_cout[word] = word_count[word]*weight

def get_filter_content_tf_with_title(title_wid,description,sentences):
    result = []
    util.migrate_list2_to_list1(result,title_wid)
    util.migrate_list2_to_list1(result,description)
    word_count = util.list_to_dic_count(result)
    res_dic = dict()
    accumulate_dic_count(res_dic,word_count,1.0)
    for i in range(len(sentences)):
        wids = sentences[i]
        temp_wc = util.list_to_dic_count(wids)
        share_count = util.get_share_count(word_count,temp_wc)
        if share_count > 0:
            weight = float(share_count)/float(len(word_count))
            accumulate_dic_count(res_dic,temp_wc,weight)
    return res_dic

def expand_title_and_description2(title_wid,description,sentences,threshold = 0.1):
    #print '==========================================================================='
    #util.print_list(title_wid)
    result = []
    util.migrate_list2_to_list1(result,title_wid)
    util.migrate_list2_to_list1(result,description)
    word_count = util.list_to_dic_count(result)
    util.nomalize_square_dic(word_count)
    #print_word_dic(word_count)
    for i in range(1,len(sentences)):
        wids = sentences[i]
        temp_wc = util.list_to_dic_count(wids)
        util.nomalize_square_dic(temp_wc)
        sim = util.get_similarity(word_count,temp_wc)
        if sim > threshold:
            util.migrate_list2_to_list1(result,wids)
            #print 'score = %f with sentence: '%sim
            #util.print_list(wids)
    return result

def expand_title_and_description(title_wid,description,sentences):
    #print '==========================================================================='
    #util.print_list(title_wid)
    result = []
    util.migrate_list2_to_list1(result,title_wid)
    util.migrate_list2_to_list1(result,description)
    word_set = Set()
    for word in title_wid:
        word_set.add(word)
    if description:
        for word in description:
            word_set.add(word)
    for i in range(1,len(sentences)):
        wids = sentences[i]
        for wid in wids:
            if wid in word_set:
                util.migrate_list2_to_list1(result,wids)
                #util.print_list(wids)
                break
    return result

def get_corpus_rep_of_articles(aids,vocab_map):
    #vocab_map = util.get_word_2_id(trend_util.get_dictionary_path(event_date))
    article_ids = aids
    (sentence_dic,domain_dic) = read_doc_from_db(article_ids,vocab_map)
    en_sentence_dic = get_online_entity(aids)
    #en_title_dic = get_online_entity_tit_des(aids,ent_title_template)
    #en_desc_dic = get_online_entity_tit_des(aids,ent_description_template)
    title_dic = get_online_title(article_ids,vocab_map)
    #title_wid_dic = get_title_wids(vocab_map,title_dic)
    description_dic = get_online_title_or_description(article_ids,vocab_map,DESCRIPTION_TYPE)
    title_prune_dic = get_online_title_or_description(article_ids,vocab_map,TITLE_TYPE)
    corpus = dict()
    for aid in sentence_dic:
        title = None
        if aid in title_dic:
            title = title_dic[aid]
        title_prune = None
        if aid in title_prune_dic:
            title_prune = title_prune_dic[aid]
        description = None
        if aid in description_dic:
            description = description_dic[aid]
        en_sen = None
        if aid in en_sentence_dic:
            en_sen = en_sentence_dic[aid]
        doc = Document(aid,title,title_prune,description,sentence_dic[aid],en_sen,domain_dic[aid])#,en_title_dic[aid],en_desc_dic[aid]
        corpus[aid] = doc
    return corpus

def get_title_wids(vocab_map,title_dic):
    result = dict()
    for aid in title_dic:
        title = title_dic[aid]
        wids = []
        for word in title:
            if word in vocab_map:
                wids.append(vocab_map[word])
        result[aid] = wids
    return result

def get_data_of_event_online(event_id):
    event = event_dao.get_event_by_id(event_id,'event2')
    event_date = str(event[resources.EVENT_DATE])
    vocab_map = util.get_word_2_id(trend_util.get_dictionary_path(event_date))
    article_ids = util.get_full_article_ids(event[resources.EVENT_ARTICCLE_IDS])
    return get_corpus_rep_of_articles(article_ids,vocab_map)

def get_data_of_event(event_id):
    event = event_dao.get_event_by_id(event_id,'event2')
    event_date = str(event[resources.EVENT_DATE])
    vocab_map = util.get_word_2_id(trend_util.get_dictionary_path(event_date))
    article_ids = util.get_full_article_ids(event[resources.EVENT_ARTICCLE_IDS])
    (sentence_dic,domain_dic) = read_doc_from_db(article_ids,vocab_map)
    en_sentence_dic = get_entity(event_id)
    title_dic = get_title(event_id,vocab_map,TITLE_TYPE)
    description_dic = get_title_or_description(event_id,vocab_map,DESCRIPTION_TYPE)
    title_prune_dic = get_title_or_description(event_id,vocab_map,TITLE_TYPE)
    corpus = dict()
    for aid in sentence_dic:
        title = None
        if aid in title_dic:
            title = title_dic[aid]
        title_prune = None
        if aid in title_prune_dic:
            title_prune = title_prune_dic[aid]
        description = None
        if aid in description_dic:
            description = description_dic[aid]
        en_sen = None
        if aid in en_sentence_dic:
            en_sen = en_sentence_dic[aid]
        doc = Document(aid,title,title_prune,description,sentence_dic[aid],en_sen,domain_dic[aid])
        corpus[aid] = doc
    return corpus

def get_online_title_or_description(aids,vocab_map,data_type=TITLE_TYPE):
    #save_path = get_title_path(event_id)
    #if data_type == DESCRIPTION_TYPE:
    #    save_path = get_description_path(event_id)
    #if (not os.path.exists(save_path)):
    #    event = event_dao.get_event_by_id(event_id,'event2')
    article_ids = aids#util.get_full_article_ids(event[resources.EVENT_ARTICCLE_IDS])
    url_template = tit_url_template
    if data_type == DESCRIPTION_TYPE:
        url_template = des_url_template
    titles = get_entity_corpus(article_ids,url_template)
    #util.save_object(save_path,titles)
    #titles = util.read_object(save_path)
    corpus = dict()
    stopword = data_reader.get_stopwords()
    for aid in titles:
        temp = unicode(titles[aid],'utf-8')
        sen_token = data_reader.get_doc_from_text(temp,stopword)
        filter_sen = filter_list(sen_token,vocab_map)
        corpus[aid] = filter_sen
    return corpus

def get_title_or_description(event_id,vocab_map,data_type=TITLE_TYPE):
    save_path = get_title_path(event_id)
    if data_type == DESCRIPTION_TYPE:
        save_path = get_description_path(event_id)
    if (not os.path.exists(save_path)):
        event = event_dao.get_event_by_id(event_id,'event2')
        article_ids = util.get_full_article_ids(event[resources.EVENT_ARTICCLE_IDS])
        url_template = tit_url_template
        if data_type == DESCRIPTION_TYPE:
            url_template = des_url_template
        titles = get_entity_corpus(article_ids,url_template)
        util.save_object(save_path,titles)
    titles = util.read_object(save_path)
    corpus = dict()
    stopword = data_reader.get_stopwords()
    for aid in titles:
        temp = unicode(titles[aid],'utf-8')
        sen_token = data_reader.get_doc_from_text(temp,stopword)
        filter_sen = filter_list(sen_token,vocab_map)
        corpus[aid] = filter_sen
    return corpus

def get_connect_word_from_list(tokens):
    result = ''
    for i in range(len(tokens)):
        result += tokens[i] + ' '
    return result

def get_online_title(aids,vocab_map):
#    save_path = get_title_path(event_id)
#    if data_type == DESCRIPTION_TYPE:
#        save_path = get_description_path(event_id)
#    if (not os.path.exists(save_path)):
#        event = event_dao.get_event_by_id(event_id,'event2')
    article_ids = aids#util.get_full_article_ids(event[resources.EVENT_ARTICCLE_IDS])
    url_template = tit_url_template
    titles = get_entity_corpus(article_ids,url_template)
#    util.save_object(save_path,titles)
#    titles = util.read_object(save_path)
    corpus = dict()
    stopword = data_reader.get_stopwords()
    for aid in titles:
        temp = unicode(titles[aid],'utf-8')
        #temp = remove_until_haicham(temp)
        sen_tokens = data_reader.get_doc_from_text(temp,stopword)
        #filter_sen = filter_list(sen_tokens,vocab_map)
        corpus[aid] = sen_tokens#filter_sen
    return corpus

def get_title(event_id,vocab_map,data_type=TITLE_TYPE):
    save_path = get_title_path(event_id)
    if data_type == DESCRIPTION_TYPE:
        save_path = get_description_path(event_id)
    if (not os.path.exists(save_path)):
        event = event_dao.get_event_by_id(event_id,'event2')
        article_ids = util.get_full_article_ids(event[resources.EVENT_ARTICCLE_IDS])
        url_template = tit_url_template
        if data_type == DESCRIPTION_TYPE:
            url_template = des_url_template
        titles = get_entity_corpus(article_ids,url_template)
        util.save_object(save_path,titles)
    titles = util.read_object(save_path)
    corpus = dict()
    stopword = data_reader.get_stopwords()
    for aid in titles:
        temp = unicode(titles[aid],'utf-8')
        #temp = remove_until_haicham(temp)
        sen_tokens = data_reader.get_doc_from_text(temp,stopword)
        #filter_sen = filter_list(sen_tokens,vocab_map)
        corpus[aid] = sen_tokens#filter_sen
    return corpus

def remove_until_haicham(text):
    size  = len(text)
    index = -1
    for i in range(size):
        if text[i] == ':':
            index = i
    if index == -1:
        return text
    if (index == size - 1):
        return text
    return text[index+1:]
        
def get_entity_corpus(aids,url_template,size=150):
        leng  = len(aids)
        corpus = dict()
        offset = 0
        while(True):
            offset = get_entity_corpus_in_chunk(aids,offset,size,corpus,url_template)
            if (offset == leng):
                break
        #for key in corpus:
        #    corpus[key] = unicodedata.normalize('NFC',corpus[key])
        return corpus
def get_entity_corpus_in_chunk(aids,offset,size,corpus,url_template):
        last = min(offset+size,len(aids))
        temp_aids = aids[offset:last]
        temp_corpus = get_entity_corpus_with_aids(temp_aids,url_template)
        for key in temp_corpus:
            corpus[key] = temp_corpus[key]
        return last
def get_entity_corpus_with_aids(aids,url_template):
        if (len(aids) == 0):
            return {}
        wstr = util.get_in_where_str(aids)
        url = '%s=%s'%(url_template,wstr)
        #url = 'http://10.3.24.83:8080/getentity/api/getentity/?id=%s'%wstr
        print url
        response = urllib.urlopen(url)
        corpus = util.str_to_dict(response.read())
        return corpus

def get_online_entity_tit_des(aids,url_template):
    article_ids = aids
    #if (not os.path.exists(content_entity_path)):
    corpus = get_entity_corpus(article_ids,url_template)
    keys = corpus.keys()
    for key in keys:
        corpus[key] = refine_tokens(corpus[key])
    return corpus

def get_online_entity(aids,url_template = sentence_url_template):
    article_ids = aids
    #if (not os.path.exists(content_entity_path)):
    corpus = get_entity_corpus(article_ids,url_template)
    keys = corpus.keys()
    for key in keys:
            sentences = corpus[key]
            new_sens = []
            for i in range(len(sentences)):
                sen = refine_tokens(sentences[i])
                new_sens.append(sen)
            corpus[key] = new_sens
        #util.save_object(content_entity_path,corpus)
   # print 'get entity of titles of event: %d'%event_id
    #en_sentence_dic = util.read_object(content_entity_path)
    return corpus

def get_entity(event_id):
    content_entity_path = get_entity_path(event_id)
    event = event_dao.get_event_by_id(event_id,'event2')
    article_ids = util.get_full_article_ids(event[resources.EVENT_ARTICCLE_IDS])
    if (not os.path.exists(content_entity_path)):
        corpus = get_entity_corpus(article_ids,sentence_url_template)
        keys = corpus.keys()
        for key in keys:
            sentences = corpus[key]
            new_sens = []
            for i in range(len(sentences)):
                sen = refine_tokens(sentences[i])
                new_sens.append(sen)
            corpus[key] = new_sens
        util.save_object(content_entity_path,corpus)
   # print 'get entity of titles of event: %d'%event_id
    en_sentence_dic = util.read_object(content_entity_path)
    return en_sentence_dic
    
def convert_word_tf_2_id_tf(word_tf,w2id):
    result = dict()
    for word in word_tf:
        if word in w2id:
            result[w2id[word]] = word_tf[word]
    return result

def read_doc_from_db(aids,vocab_map):
    result = dict()
    domain_dic = dict()
    articles = event_dao.get_article_urls(aids)
    stopword = data_reader.get_stopwords()
    
    #print 'number of stopwords: %d'%len(stopword)
    for art in articles:
        content = art[dao.TOKEN]
        sentences = content.split(' . ')
        doc  = []
        doc_id = art[dao.ID]
        domain_dic[doc_id] = art[dao.DOMAIN]
        for i in range(len(sentences)):
            if (len(sentences[i]) > 0):
                sen_token = data_reader.get_doc_from_text(sentences[i],stopword)
                filter_sen = filter_list(sen_token,vocab_map)
                if (len(filter_sen) > 0):
                    doc.append(filter_sen)
        result[doc_id] = doc
    return (result,domain_dic)

def filter_list(word_list,vocab_map):
    result = []
    for word in word_list:
        if word in vocab_map:
            #result.append(vocab_map[word])
            result.append(word)
    return result

def save_data(corpus,path):
    util.save_object(path,corpus)

def print_word_dic(word_dic):
    for w in word_dic:
        print '%s: %s'%(w,str(word_dic[w]))

def get_context_document_tf(sentences):
    tf = dict()
    sen_num = float(len(sentences))
    for i in range(len(sentences)):
        sen = sentences[i]
        for token in sen:
            tf_score = 1 + MAX_ADD*(sen_num-i)/(sen_num)
            if token in tf:
                tf[token] += tf_score
            else:
                tf[token] = tf_score
    return tf

def get_entity_document_tf(sentences):
    tf = dict()
    sen_num = float(len(sentences))
    for i in range(len(sentences)):
       sen = sentences[i]
       tokens = get_entity_from_sen(sen)
       for tok in tokens:
           tf_score = 1 + MAX_ADD*(sen_num-i)/(sen_num)
           if tok in tf:
               tf[tok] += tf_score
           else:
               tf[tok] = tf_score
    return tf

def get_all_tokens(title_tf):
    words = []
    for token in title_tf:
        ws = token.split('_')
        util.migrate_list2_to_list1(words,ws)
    bow = util.list_to_dic_count(words)
    return bow

def get_entity_from_sen(sen):
    result = []
    for biword in sen:
        temp = biword.replace('-lrb-','')
        temp = temp.replace('-rrb-','')
        if (len(temp) > 1):
            words = temp.split(' ')
            word = words[-1]
            tokens = word.split('_')
            if tokens[-1] not in BLACK_SET:
                result.append(tokens[-1])
            #util.migrate_list2_to_list1(result,tokens)
    return result

def refine_corpus(corpus):
    for key in corpus:
        corpus[key] = refine_tokens(corpus[key])
        
def refine_tokens(tokens):
    result = []
    for i in range(len(tokens)):
        token = unicode(tokens[i],'utf-8')
        result.append(token)
    return result
    
def refine_dic_list(corpus):
    for key in corpus:
        corpus

def get_vecto_rep(event_id):
    corpus = get_data_of_event(event_id)
    tf_corpus = dict()
    for aid in corpus:
        tf_corpus[aid] = corpus[aid].con_tf
    con_corpus = util.get_tfidf_from_tf_corpus(tf_corpus,2.0)
    en_tf_corpus = dict()
    for aid in corpus:
        en_tf_corpus[aid] = corpus[aid].en_tf
    en_corpus = util.get_tfidf_from_tf_corpus(en_tf_corpus,2.0)
    return (con_corpus,en_corpus)

def get_entity_path(event_id):
    return data_path +'/%d_entity.dat'%event_id
def get_title_path(event_id):
    return data_path + '/%d_title.dat'%event_id
def get_description_path(event_id):
    return data_path + '/%d_description.dat'%event_id

#get_data_of_event_online(68889697)

