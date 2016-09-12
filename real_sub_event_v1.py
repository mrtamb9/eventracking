import resources,logging,util,sub_data_reader,trend_util,cluster_doc2
from real_sub_event import Real_Event_Listener
import deploy_hier_v2,h_sub_util
logger = logging.getLogger("real_time_detect_events3.py")
logger.setLevel(logging.INFO)
util.set_log_file(logger,resources.LOG_FOLDER+'/sub_event_real_time.log','w')
util.set_log_console(logger)
RE_CALCULATE_THRESHOLD = 10
ADD_CONNECT_THRESHOLD = 0.25
REMOVED_DOMAINS  = set(['xaluan.com'])
DUPLICATE_THRESHOLD = 0.85

class Article_Reader:
    """
    This class is used to get and cache article records from database, when a request for article record, this checked by id whether this article is fetched or not
    if fetched --> return, else fetch it from database
    """
    def __init__(self):
        self.corpus = dict()
        yesterday = util.get_yesterday_str()
        self.vocab_map = util.get_word_2_id(trend_util.get_dictionary_path(yesterday))
        
    def get_doc_dic(self,aids):
        """
        return a dic map aid and records ()
        """
        unfetch_aids = []
        for aid in aids:
            if aid not in self.corpus:
                unfetch_aids.append(aid) 
        if len(unfetch_aids) > 0:
            self.fetch_new_data(unfetch_aids)
        result = dict()
        for aid in aids:
            result[aid] = self.corpus[aid]
        return result
    
    def fetch_new_data(self,n_aids):
        temp_corpus  = sub_data_reader.get_corpus_rep_of_articles(n_aids,self.vocab_map)
        for aid in temp_corpus:
            self.corpus[aid] = temp_corpus[aid]

class Connect_Pre_Trend:
    """
    This class is used to connect subs of an event to its subs in the past, it caches and loads information for connecting subs
    """
    def __init__(self):
        self.trend_dic = dict()
        self.trend_aid_dic = dict()# map from (trend_id,aid) --> True/False
        
    def get_connect_result(self,trend_id,corpus,connect_threshold = deploy_hier_v2.INTER_CONNECT_THRESHOLD):
        """
        connect aids in corpus to trend_id
        return merge_dic: aid ---> sub_id
               new_corpus: aid --> doc (aids that cannot be connected to previous trend)
        """
        if (trend_id == -1):
            return (dict(),corpus)
        self.connect_doc_2_trend(trend_id,corpus,connect_threshold)
        merge_dic = dict()
        new_corpus = dict()
        for aid in corpus:
            sub_id = self.trend_aid_dic[(aid,trend_id)]
            if sub_id:
                merge_dic[aid] = sub_id
            else:
                new_corpus[aid] = corpus[aid]
        
        return (merge_dic,new_corpus)
    def connect_doc_2_trend(self,trend_id,corpus,connect_threshold):
        """
        compute whether we can connect articles in corpus to trend_id, if this was not computed, compute new articles
        """
        if trend_id not in self.trend_dic:
            self.load_trend(trend_id)
        uncompute_corpus = dict()
        for aid in corpus:
            if (aid,trend_id) not in self.trend_aid_dic:
                uncompute_corpus[aid] = corpus[aid]
                
        (pre_cluster_dic,trend_comp) = self.trend_dic[trend_id]
        (merge_dic,new_corpus) = deploy_hier_v2.connect_new_article_to_old_clusters2(pre_cluster_dic,trend_comp,uncompute_corpus)
        for aid in merge_dic:
            sub_id = merge_dic[aid]
            self.trend_aid_dic[(aid,trend_id)] = sub_id# connect aid --> sub_id in trend_id
        for aid in new_corpus:
            self.trend_aid_dic[(aid,trend_id)] = None # cannot connect aid --> trend_id
            
    def load_trend(self,trend_id):
        trend_path = deploy_hier_v2.get_cluster_dic_path(trend_id)
        previous_cluster_dic = util.read_object(trend_path)
        pre_aids = []
        for subid in previous_cluster_dic:
            util.migrate_list2_to_list1(pre_aids,previous_cluster_dic[subid])
        pre_corpus = article_Reader.get_doc_dic(pre_aids)
        trend_comp = deploy_hier_v2.Trend_Comparator(pre_corpus)
        self.trend_dic[trend_id] = (previous_cluster_dic,trend_comp)
        

article_Reader = Article_Reader()        
connect_pre_trend = Connect_Pre_Trend()

class Event_Hanlder:
    def __init__(self,event_id,trend_id,date,sub_table,day_sub_table):
        self.event_id = event_id
        self.date = date
        self.sub_table = sub_table
        self.trend_id = trend_id
        self.corpus = dict()
        self.old_cluster_dic = dict()# sub connected to subs in the past
        self.new_cluster_dic = dict()# new subs for today
        self.new_group_dic = dict() # group_dic map sub_id --> group_id for new_subs
        self.old_group_dic = dict()# group_dic map sub_id --> group_id for subs in the past
        self.max_group_id = 0 # max_group_id, as new sus comes, its group_id would be max-group_id + 1
        self.doc_increase = 0
        self.comparator = None#background for connect additional articles 
        self.day_sub_table = day_sub_table
    
    def filter_removed_domain(self,aids):
        temp_corpus  = article_Reader.get_doc_dic(aids)
        result = []
        for aid in aids:
            if temp_corpus[aid].domain not in REMOVED_DOMAINS:
                result.append(aid)
        return result

    def remove_duplicate_doc(self,aids):
        # duplicate within aids first
        #first remove aid with black_domain
        iden_comp = self.comparator.get_Iden_comp()
        logger.info('find duplicate articles within %s'%str(aids))
        iden_dic = cluster_doc2.cluster_doc_with_comparator(aids,iden_comp,DUPLICATE_THRESHOLD)# cluster duplicate document()
        result = []
        for aid in iden_dic:
            if self.check_duplicate_in_corpus(aid):# check duplicate with saved article of event
                result.append(aid)
        return result
    
    def check_duplicate_in_corpus(self,aid):
        """
        Find in corpus whether there is an article duplicate with this article
        """
        iden_comp = self.comparator.get_Iden_comp()
        for t_aid in self.corpus:
            if t_aid != aid:
                sim = iden_comp.get_similarity(aid,t_aid)
                if sim >= DUPLICATE_THRESHOLD:
                    return True
        return False
    
    def new_articles_to_trend(self,add_aids,trend_id):
        logger.info('%s are added to %d of trend_id %d'%(str(add_aids),self.event_id,self.trend_id))
        """
        a list of articles being added to trend_id. If trend_id = 1, this is a new event (event starts at this day)
        """
        aids = self.filter_removed_domain(add_aids)#remove xaluan.com
        new_corpus = article_Reader.get_doc_dic(aids)
        util.migrate_dic2_to_dic1(self.corpus,new_corpus)#import new_corpus ---> corpus
        
        if trend_id != self.trend_id:# If trend_id has been changed, we need to re-run the whole event
            self.trend_id = trend_id
            logger.info('EVENT %d CHANGES TREND_ID FROM %d TO %d, WE NEED TO RE_RUN'%(self.event_id,self.trend_id,trend_id))
            self.remove_all_sub_event()
            self.rerun_whole_event(self.corpus.keys(),trend_id)
            return
            
        self.doc_increase += len(aids)
        if self.doc_increase > RE_CALCULATE_THRESHOLD:# too many new articles, to guarantee the quality we need to recalculate 
            logger.info('RE-SUB_EVENT as DOC_INCREASE = %d > %d'%(self.doc_increase,RE_CALCULATE_THRESHOLD))
            self.remove_all_sub_event()
            self.rerun_whole_event(self.corpus.keys(),trend_id)
        else:# not enough doc, we temporarily add to current cluster_dic
            logger.info('DOC_INCREASE = %d < %d, WE TEMPORARILY ADD DOCS TO AVAILABLE SUBS'%(self.doc_increase,RE_CALCULATE_THRESHOLD))
            self.add_new_article_2_avail_sub(aids)
    
    def add_new_article_2_avail_sub(self,add_aids,connect_threshold = ADD_CONNECT_THRESHOLD):
        """
        new articles may be connected to new_cluster_dic, old_cluster_dic we must consider all cases. 
        Also, in each case, we need to update associated new_group_dic and old_group_dic
        """
        logger.info('consider merging new aids to old_cluster')
        aids = self.remove_duplicate_doc(add_aids)#remove duplicate articles within add_aids
        add_corpus = article_Reader.get_doc_dic(aids)
        self.comparator.update_new_doc_with(add_corpus)# add new articles to comparator --> can use this to compute similarity between new articles
        (merge_dic,new_corpus) = connect_pre_trend.get_connect_result(self.trend_id,add_corpus)
        logger.info('connecting to trend_id %d result: %s'%(self.trend_id,str(merge_dic)))
        save_sub_ids = set()#sub_id need being updated or inserted
        new_sub_old_ids = set()# new sub_id from previous subs
        for aid in merge_dic:
            sub_id = merge_dic[aid]
            save_sub_ids.add(sub_id)
            if sub_id in self.old_cluster_dic:
                self.old_cluster_dic[sub_id].append(aid)
            else:
                new_sub_old_ids.append(sub_id)
                self.old_cluster_dic[sub_id] = [aid]  
        #update new self.old_group_dic
        if (len(new_sub_old_ids) > 0):
            temp_group_dic = h_sub_util.get_sub_id_group_id(self.sub_table,new_sub_old_ids)
            util.migrate_dic2_to_dic1(self.old_group_dic,temp_group_dic)
            self.insert_or_update(list(new_sub_old_ids))
            
        if len(new_corpus) > 0:
            #TO-DO connect new articles to new_cluster_dic
            for aid in new_corpus:
                if (len(self.new_cluster_dic) > 1):# in case, new_cluster_dic doesn't contain any new sub (all articles ---> old_sub_cluster)
                    (max_sub_id,max_score) = deploy_hier_v2.get_closest_cluster(self.new_cluster_dic,new_corpus[aid],self.comparator)
                    if max_score >= connect_threshold:
                        self.new_cluster_dic[max_sub_id].append(aid)
                        logger.info('add %d to cluster %s'%(aid,str(self.new_cluster_dic[max_sub_id])))
                        save_sub_ids.add(max_sub_id)
                    else:
                        self.new_cluster_dic[aid] = [aid]
                        logger.info('new group dic: %s'%str(self.new_group_dic))
                        logger.info('max group id: %s'%str(self.max_group_id))
                        self.new_group_dic[aid] = self.max_group_id + 1# new group 
                        self.max_group_id += 1
                        logger.info('New group is formed: %d'%self.new_group_dic[aid])
                        logger.info('%d form a new sub'%aid)
                        save_sub_ids.add(aid)
                else:
                    self.new_cluster_dic[aid] = [aid]
                    logger.info('new group dic: %s'%str(self.new_group_dic))
                    logger.info('max group id: %s'%str(self.max_group_id))
                    self.new_group_dic[aid] = self.max_group_id + 1# new group 
                    self.max_group_id += 1 # increase group_id
                    save_sub_ids.add(aid)
        if (len(save_sub_ids) > 0):
            self.insert_or_update(save_sub_ids)
                    
    def insert_or_update(self,sub_ids):
        """
        insert sub_id in both new_cluster_dic and old_cluster_dic to database
        """
        logger.info('inserting sub_ids: %s'%str(sub_ids))
        if (len(sub_ids) == 0):
            return
        trend_id = self.trend_id
        if self.trend_id == -1:
            trend_id = self.event_id
        insert_cluster_dic = dict()
        insert_group_dic = dict()
        for sub_id in sub_ids:
            if sub_id in self.new_cluster_dic:
                insert_cluster_dic[sub_id] = self.new_cluster_dic[sub_id]
                insert_group_dic[sub_id] = self.new_group_dic[sub_id]
            else:
                insert_cluster_dic[sub_id] = self.old_cluster_dic[sub_id]
                insert_group_dic[sub_id] = self.old_group_dic[sub_id]
        deploy_hier_v2.insert_new_sub_event_records(insert_cluster_dic,insert_group_dic,trend_id,self.sub_table)
        logger.info('finish inserting or updating %d new_sub_id into: %s'%(len(sub_ids),self.sub_table))
        

    def rerun_whole_event(self,add_aids,trend_id):
        """
        cluster a list of articles (all from an event) belonging to trend_id (event )
        this includes: connecting aids to previous subs (for example, sub of yesterday) and forming new subs, in each case, we have to update associated group_dic
        If trend_id == -1 --> this is a new event
        After running this function, new_cluster_dic, old_cluster_dic, new_group_dic, old_group_dic and max_group_id are computed
        """
        logger.info('re-run sub-event for the whole event %d of trend_id %d, like the first time we run'%(self.event_id,trend_id))
        aids = self.filter_removed_domain(add_aids)
        self.trend_id = trend_id
        self.corpus = article_Reader.get_doc_dic(aids)
        if (trend_id == -1):# this is a totally new event
            self.sub_event_first_time()
        else:#this event was started, not the first time
            self.sub_existing_event()
        self.doc_increase = 0#start calculate doc_increase to consider whether re-run or not
    
    def sub_event_first_time(self):
        """
        An event starts from now, there is no need to connect to previous subs
        Only need to compute: new_cluster_dic, new_group_dic, max_group_id
        """
        comp1 = deploy_hier_v2.Trend_Comparator(self.corpus)
        self.comparator = comp1
        comp2 = deploy_hier_v2.Content_Focus_Comparator(comp1)
        (cluster_dic,group_dic) = deploy_hier_v2.decompose_event(comp1,comp2,self.corpus.keys(),0)# cluster this 
        self.new_cluster_dic = cluster_dic
        self.new_group_dic = group_dic
        self.max_group_id = get_max_key_in_dic(self.new_group_dic)# group_id for the next group
        self.insert_or_update(self.new_cluster_dic.keys())
        
    def sub_existing_event(self):
        """
        There is a need to connect to previous subs
        Need to compute: old_cluster_dic, old_group_dic, new_cluster_dic, new_group_dic and max_group_id
        """
        max_group_id = h_sub_util.get_max_group_id(self.trend_id,self.day_sub_table)#get max_group_id to increase for the next
        self.max_group_id = max_group_id
        #read existing sub
        logger.info('connect %d to %d'%(self.event_id,self.trend_id))
        (merge_dic,new_corpus) = connect_pre_trend.get_connect_result(self.trend_id,self.corpus)
        logger.info('merge_dic: %s'%str(merge_dic))
        self.old_cluster_dic = util.get_label2id_dic(merge_dic)
        save_sub_ids = list(self.old_cluster_dic.keys())#insert to database
        if (len(self.old_cluster_dic) > 0):
            self.old_group_dic = h_sub_util.get_sub_id_group_id(self.day_sub_table,self.old_cluster_dic.keys())#get group_id of old_group
            #logger.info('finish inserting %d old_sub_id into %s'%(len(self.old_cluster_dic),self.sub_table))
            #ADD old record to real_time clusters
        if (len(new_corpus) > 0):
            logger.info('cluster new_corpus of event_id %d: %s'%(self.event_id,str(new_corpus.keys())))
            trend_comparator2 = deploy_hier_v2.Trend_Comparator(self.corpus)
            self.comparator = trend_comparator2
            comp2 = deploy_hier_v2.Content_Focus_Comparator(trend_comparator2)
            (new_cluster_dic,new_group_dic) =  deploy_hier_v2.decompose_event(trend_comparator2,comp2,new_corpus.keys(),self.max_group_id)#cluster_doc2.cluster_doc_with_comparator(new_corpus.keys(),comp2,BINARY_THRESHOLD)
            logger.info('new_corpus is clustered into: %s'%str(new_cluster_dic))
            self.new_cluster_dic = new_cluster_dic
            self.new_group_dic = new_group_dic
            self.max_group_id = get_max_key_in_dic(new_group_dic)
            #logger.info('finish inserting %d new_sub_id into: %s'%(len(self.new_cluster_dic),self.sub_table))
            for sub_id in self.new_cluster_dic:
                save_sub_ids.append(sub_id)
        logger.info('new_cluster_dic: %s'%str(self.new_cluster_dic))
        logger.info('new_group_dic: %s'%str(self.new_group_dic))
        logger.info('old_cluster_dic: %s'%str(self.old_cluster_dic))
        logger.info('old_group_dic: %s'%str(self.old_group_dic))
        self.insert_or_update(save_sub_ids)
        
        #util.migrate_dic2_to_dic1(cluster_dic2,new_cluster_dic)
        
    def remove_all_sub_event(self):
        """
        removing all subs of this event (maybe some event ---> into several smaller even)
        """
        all_sub_ids = []
        for subid in self.old_cluster_dic:
            all_sub_ids.append(subid)
        for subid in self.new_cluster_dic:
            all_sub_ids.append(subid)
        if len(all_sub_ids) > 0:
            h_sub_util.delete_sub_with_ids(all_sub_ids)
            logger.info('delete %d of event %d'%(len(all_sub_ids),self.event_id))
        logger.info('remove the whole event, in case one event ---> smaller one, we need to remove all')

def get_good_key(cluster,domain_dic):
    """
    choose an article to represent cluster, in this case, choose article with domain different from xaluan.com
    """
    new_key = cluster[0]
    for aid in cluster:
        if domain_dic[aid] != 'xaluan.com':
            return new_key
    return new_key
REAL_SUB_TABLE = deploy_hier_v2.REAL_TABLE_NAME
DAY_SUB_TABLE = deploy_hier_v2.TABLE_NAME
import event_dao

class Hierarchical_Sub_V1(Real_Event_Listener):
    def __init__(self,sub_table,day_sub_table):
        Real_Event_Listener.__init__(self)
        self.sub_table = sub_table
        self.day_sub_table = day_sub_table
        self.event_dic = dict()
    
    def start_new_day(self):
        logger.info('removing all subs in real_time table: %s'%self.sub_table)
        event_dao.delete_all_record_in_table(self.sub_table)
    
    def new_articles_to_event(self,aids,event_id,trend_id):
        """
        a list of articles being added to trend_id. If trend_id = 1, this is a new event (event starts at this day)
        """
        logger.info('when new articles %s being added to event_id %d of trend_id %d'%(str(aids),event_id,trend_id))
        self.event_dic[event_id].new_articles_to_trend(aids,trend_id)
        

    def detect_new_event(self,aids,event_id,trend_id):
        """
        cluster a list of articles (all from an event) belonging to trend_id (event )
        """
        logger.info('when new articles %s being added to event_id %d of trend_id %d'%(str(aids),event_id,trend_id))
        self.event_dic[event_id] = Event_Hanlder(event_id,trend_id,self.date,self.sub_table,self.day_sub_table)
        self.event_dic[event_id].rerun_whole_event(aids,trend_id)

    def remove_event(self,event_id):
        """
        removing all subs of this event (maybe some event ---> into several smaller even)
        """
        self.event_dic[event_id].remove_all_sub_event()
        print 'remove the whole event, incase one event ---> smaller one, we need to remove all'

def get_max_key_in_dic(dic):
    max_key = -1
    for key in dic:
        if dic[key] > max_key:
            max_key = dic[key]
    return max_key
