#!/usr/bin/python
# -*- coding: utf-8 -*-
import util,logging,resources,time,data_reader,event_dao,dao,real_time_resources2,sys,trend_util,urllib
import unicodedata
from gensim import corpora
sys.path.insert(0,'classification')
import art_category
CONTENT_THESHOLD = 600
from sets import Set
logger = logging.getLogger("real_time_reader")
logger.setLevel(logging.INFO)
util.set_log_file(logger,real_time_resources2.READ_DATA_LOG_PATH)
ENTITY_URL_TEMPLATE = 'http://10.3.24.83:8080/trending/api/entity/entity/?id'
TITLE_URL_TEMPLATE = 'http://10.3.24.83:8080/trending/api/tokenizer/title/?id'
DESCRIPTION_TEMPLATE = 'http://10.3.24.83:8080/trending/api/tokenizer/description/?id'

class Data_Reader:
    
    def __init__(self,date=None):
        self.max_aid = 0
        if (date):
            self.date = date
        else:
            self.date = util.get_today_str()
        logger.info('initializing data on day: %s'%self.date)
        self.yesterday = util.get_past_day(self.date,1)
        self.domains = dict()
        self.dic_path = trend_util.get_dictionary_path(self.yesterday)
        logger.info('reading dictionary on day: %s'%self.dic_path)
        self.dictionary = corpora.Dictionary.load_from_text(self.dic_path)
        (self.df_map,self.corpus_size) = data_reader.read_df_corpussize(self.yesterday)
        self.classifier = art_category.Classifier_ob()
        self.read_domains()
        self.event_trace_dic = dict()# to check when to update eventevent or trend_id

#    def ini_bigram(self):
#        bi_dic_path = bi_util.get_dictionary_path(self.yesterday)
#        self.bi_dictionary = corpora.Dictionary.load_from_text(bi_dic_path)
#        self.bi_df_map = util.get_dictionary_df(bi_dic_path)
        
    def get_bigram_tfidf(self,text):
        tokens = get_doc_from_text(text)
        bigrams = []
        for i in range(len(tokens)-1):
            biword = '(%s;%s)'%(tokens[i],tokens[i+1])
            bigrams.append(biword)
        bow = self.bi_dictionary.doc2bow(bigrams)
        (wids,couts) = data_reader.convert_bow_gensim_wids_counts(bow)
        tf_idf_doc = data_reader.get_tf_idf_from_bow(wids,couts,self.bi_df_map,self.corpus_size) 
        return tf_idf_doc
    
    def get_bigram_title_tfidf(self,aids):
        titles = get_entity_corpus(aids,TITLE_URL_TEMPLATE)
        corpus = dict()
        for aid in titles:
            if (len(titles[aid]) > 15):# title is longer this threshol
                temp = unicode(titles[aid],'utf-8')
                try:
                    corpus[aid] = self.get_bigram_tfidf(temp)
                except:
                    print 'title of %d is errorous'%aid
                    print temp
        return corpus
    
    def get_bigram_description(self,aids):
        descriptions = get_entity_corpus(aids,DESCRIPTION_TEMPLATE)
        corpus = dict()
        for aid in descriptions:
            if (len(descriptions[aid]) > 30):# title is longer this threshold
                temp = unicode(descriptions[aid],'utf-8')
                try:
                    corpus[aid] = self.get_bigram_tfidf(temp)
                except:
                    print 'description of %d is errorous'%aid
                    print temp
        return corpus
        
    def read_domains(self):
        domain_path = resources.DOMAIN_PATH
        f = open(domain_path,'r')
        count = 0
        for line in f:
            temp = line.strip()
            if (len(temp) > 3):
                tg = temp.split(',')
                self.domains[tg[0]] = count
                count += 1
        f.close()
        
    def get_tfidf_of_text(self,text):
        temp = unicodedata.normalize('NFC', text)
        doc = get_doc_from_text(temp)
        bow = self.dictionary.doc2bow(doc)
        (wids,couts) = data_reader.convert_bow_gensim_wids_counts(bow)
        tf_idf_doc = data_reader.get_tf_idf_from_bow(wids,couts,self.df_map,self.corpus_size) 
        return tf_idf_doc
        
    def get_title_tfidf(self,aids):
        titles = get_entity_corpus(aids,TITLE_URL_TEMPLATE)
        corpus = dict()
        for aid in titles:
            if (len(titles[aid]) > 15):# title is longer this threshol
                temp = unicode(titles[aid],'utf-8')
                try:
                    corpus[aid] = self.get_tfidf_of_text(temp)
                except:
                    print 'title of %d is errorous'%aid
                    print temp
        return corpus
    def get_description_text_sequence(self,aids):
        descriptions = get_entity_corpus(aids,DESCRIPTION_TEMPLATE)
        corpus = dict()
        for aid in descriptions:
            if (len(descriptions[aid]) > 30):# title is longer this threshold
                temp = unicode(descriptions[aid],'utf-8')
                try:
                    temp = unicodedata.normalize('NFC',temp)
                    corpus[aid] = get_doc_from_text(temp)
                except:
                    print 'description of %d is errorous'%aid
                    print temp
        return corpus

    def get_title_text_sequence(self,aids):
        titles = get_entity_corpus(aids,TITLE_URL_TEMPLATE)
        corpus = dict()
        for aid in titles:
            if (len(titles[aid]) > 15):# title is longer this threshol
                temp = unicode(titles[aid],'utf-8')
                try:
                    temp = unicodedata.normalize('NFC',temp)
                    corpus[aid] = get_doc_from_text(temp)
                except:
                    print 'title of %d is errorous'%aid
                    print temp
        return corpus
    
    def get_description(self,aids):
        descriptions = get_entity_corpus(aids,DESCRIPTION_TEMPLATE)
        corpus = dict()
        for aid in descriptions:
            if (len(descriptions[aid]) > 30):# title is longer this threshold
                temp = unicode(descriptions[aid],'utf-8')
                try:
                    corpus[aid] = self.get_tfidf_of_text(temp)
                except:
                    print 'description of %d is errorous'%aid
                    print temp
        return corpus

    
    def get_data(self):
        rows = event_dao.read_articles_in_date(self.date,self.max_aid)
        titles = dict()
        doc_list = dict()
        cat_dic = dict()
        domain_dic = dict()
        article_dic = dict()
        tf_dic = dict()
        for domain in self.domains:
            titles[domain] = Set()
            doc_list[domain] = dict()
        iden_count  = 0
        count = 0
        temp_max_aid = 0
        for row in rows:
            aid = row[dao.ID]
            if (aid > temp_max_aid):
                temp_max_aid = aid
            d = row[dao.DOMAIN]
            domain = get_d_domains(d,self.domains)
            if (domain):
                temp_id = int(row[dao.ID])
                if (len(row[dao.CONTENT]) > CONTENT_THESHOLD):
                    del row[dao.CONTENT]
                    doc = get_doc_from_text(row[dao.TOKEN])
                    if (doc is None):
                        print 'article: %d has problem tokening'%temp_id
                    title = row[dao.TITLE]
                    if (doc is not None):
                        if title in titles[domain]:# check whether this article is absolutely identical to the previous
                        #check whether this doc is identical or not 
                            if doc in doc_list[domain].values():
                                iden_count += 1
                            else:
                                catid = self.classifier.get_catid(row[dao.URL],row[dao.TOKEN])
                                cat_dic[temp_id] = catid
                                domain_dic[temp_id] = domain
                                doc_list[domain][temp_id] = doc
                                titles[domain].add(title)
                                article_dic[temp_id] = row
                                count += 1
                        else:
                            catid = self.classifier.get_catid(row[dao.URL],row[dao.TOKEN])
                            cat_dic[temp_id] = catid
                            domain_dic[temp_id] = domain
                            doc_list[domain][temp_id] = doc
                            titles[domain].add(title)
                            article_dic[temp_id] = row
                            count += 1
            if (count % 500 == 0):
                logger.info('count = %d'%count)
        if (count >=  real_time_resources2.MIN_BATCH_SIZE):
            self.max_aid = temp_max_aid
            corpus = dict()
            for domain in doc_list:
                for doc_id in doc_list[domain]:
                    doc = doc_list[domain][doc_id]
                    bow = self.dictionary.doc2bow(doc)
                    (wids,couts) = data_reader.convert_bow_gensim_wids_counts(bow)
                    tf_dic[doc_id] = get_tf_dic(wids,couts)
                    tf_idf_doc = data_reader.get_tf_idf_from_bow(wids,couts,self.df_map,self.corpus_size) 
                    corpus[doc_id] = tf_idf_doc
            return (tf_dic,corpus,domain_dic,cat_dic,article_dic)
        logger.info('No of documents are %d, smaller than %d'%(count,real_time_resources2.MIN_BATCH_SIZE))
        print 'No of documents are %d, smaller than %d'%(count,real_time_resources2.MIN_BATCH_SIZE)
        return (None,None,None,None,None)

def get_tf_dic(wids,counts):
    result = dict()
    for i in range(len(wids)):
        result[wids[i]] = counts[i]
    return result

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

def get_entity_corpus(aids,url_template=ENTITY_URL_TEMPLATE,size=150):
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

def get_doc_from_text(text):
    """create a document as a list of nomalized tokens, stopwords are removed
    text is the raw tokenized text, separated by ' '.
    """
    try:
        #text.encode('utf-8')
        
        text = text.replace('\n',' ')
        text = text.replace('\t',' ')
        tokens = text.split(' ')
        res = list()
        for tok in tokens:
            temp = util.normalize_token(tok)
            check = util.check_token(temp)
            #print '%d'%check
            if (check == -1):
                return None
            res.append(temp)
        return res
    except Exception, err:
        print err
        return None

def get_d_domains(d,domains):
    if d in domains:
        return d
    for domain in domains:
        if domain in d:
            return domain
    return None
