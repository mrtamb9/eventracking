import dao,event_dao,logging,util,real_time_resources2,real_time_reader2,cluster_doc2,resources,detect_event_util,online_resources
import datetime,time,real_time_detect_util,online_dao,os,trend_util,top_word_img,sys,detec_trend
import urllib,data_reader
import real_sub_event_v1,real_sub_event,news_time_stat
#from connect_sub_trend_by_doc_new import Sub_trend
#from real_sub_event import Real_Event_Listener
#import i_real_time,thread
sys.path.insert(0,'visualize')
sys.path.insert(1,'news_visualize')
sys.path.insert(2,'bolx')
#import fill_news_visual
import hashtag
from sets import Set
logger = logging.getLogger("real_time_detect_events.py")
logger.setLevel(logging.INFO)
util.set_log_file(logger,real_time_resources2.LOG_PATH,'w')#log to file 
util.set_log_console(logger)#log to console 

now = datetime.datetime.now()
TODAY = util.get_today_str()
YESTERDAY = util.get_yesterday_str()
#YESTERDAY_ENTITY_VOCAB = util.read_vocab(e_util.get_dictionary_path(YESTERDAY))
YESTERDAY_VOCAB = util.read_vocab(trend_util.get_dictionary_path(YESTERDAY))
 # if an event receiving more than this threshold, we would re-calculate trend_id of this event
TIME_STAMP = '%d_%d_%d_%d_%d_%d'%(now.month,now.day,now.hour,now.minute,now.second,now.microsecond)
# add listeners for real-time detection and tracking
listener_collection = real_sub_event.Real_Event_Listenter_Collection()# manage all listeners for event-detection and tracking real-time
sub_event_listener =  real_sub_event_v1.Hierarchical_Sub_V1(real_sub_event_v1.REAL_SUB_TABLE,real_sub_event_v1.DAY_SUB_TABLE)
listener_collection.add_listener(sub_event_listener)

def get_size_level(size):
    return size/10

def sort_article_by_date(aids,art_dic):
    date_dic = dict()
    for aid in aids:
        date_dic[aid] = art_dic[aid][dao.CREATE_TIME]
    order = util.sort_dic_des(date_dic)
    sort_aids = []
    for (v,k) in order:
        sort_aids.append(k)
    return sort_aids
    
def get_bin_sim_max(v1,v2):
    if (not v1 or not v2):
        return 0
    count = 0.0
    for k1 in v1:
        if k1 in v2:
            count += 1
    max_leng = max(len(v1),len(v2))
    return (float(count)/float(max_leng))


class Event:
    def __init__(self,event_id,date):
        self.articles = []
        self.trend_id = -1
        self.id = event_id
        self.size = 0
        self.date = date
        self.coherence = 1.0
        self.catid = -1
        self.time_stamp = 0
        self.title_corpus = dict()
        self.old_dic = []
        self.new_ids = []
        self.iden_dic = None
        self.doc_increase = 0#if doc_increase > a threshold we recalculate trend_id
        self.refind_trend = True# flag for decide to calculate trend_id or not
        
    def set_articles(self,articles):
        self.articles = articles
        self.size = len(articles)
        
    def add_new_articles(self,articles):
        self.size += len(articles)
        util.migrate_list2_to_list1(self.articles,articles)
        self.doc_increase += len(articles)
        if self.doc_increase > real_time_resources2.RECALCULATE_TREND_ID:
            self.refind_trend = True
        
    def set_center(self,center):
        self.center = center
        
    def set_timestamp(self,stamp):
        self.time_stam = stamp
             
    def set_catid(self,catid):
        self.catid = catid
        
    def set_coherence(self,coherence):
        self.coherence = coherence
        
    def set_trend_id(self,trend_id):
        self.trend_id = trend_id
        self.doc_increase = 0
        self.refind_trend = False
    
class Real_Updater:
    def __init__(self):
        self.batch_size = 1000
        self.duplicate_dic = dict()
        self.today = util.get_today_str()
        #save_path = '/media/khaimai/F8C6516EC6512DDE/khai_folder/projects/trend_py/trendy_ploy1.0/temp_data/server_emu/2016-02-04'
        #self.ob_reader = real_time_data_emulator.Stream_emulator(save_path,0) 
        #self.ob_reader = real_time_reader2.Data_Reader()
        self.reader = real_time_reader2.Data_Reader()
        self.clusters = dict()# current clusters, each is a set of articles, there are no containments and nearly duplicated articles in each cluster
        self.centers = dict()# centers of clusters
        self.time_stam = 0 
        self.time_stam_dic = dict()
        self.corpus = dict()
        self.catdic = dict()
        self.db_event_trend = dict()
        self.trends = []#if we don't care about the past, this is empty
        logger.info('Load previous trends for connecting: ')
        self.trends = detec_trend.get_trends_for_connecting(self.today)#trends centers within a windows size to connect        
        logger.info('Number of previous trends: %d'%len(self.trends))
        self.batch_count = 0
        self.events = dict() # map event_id --> object event
        self.entities = dict()
        self.tf_corpus = dict()# tf-idf for corpus
        self.title_corpus = dict() 
        self.domain_dic = dict()# map aid -->domain
        self.articles = dict()
        self.read_cache()
        self.domain_distr = dict() # dictionary map domain --> list of artilces in domains
    
    def save_cache_data(self):
        entity_path = get_cached_entity_path()
        util.save_object(entity_path,self.entities)
        title_path = get_catched_title_path()
        util.save_object(title_path,self.title_corpus)
    def read_cache(self):
        entity_path = get_cached_entity_path()
        if (os.path.exists(entity_path)):
            self.entities = util.read_object(entity_path)
        title_path = get_catched_title_path()
        if (os.path.exists(title_path)):
            self.title_corpus = util.read_object(title_path)
        
    def get_day(self):
        return self.today
    
    def log_time(self,mini_seconds):
        log_time_path = real_time_resources2.BATCH_LOG_FOLDER+'/%s.csv'%TODAY
        if self.batch_count == 1:
            f = open(log_time_path,'w')
            f.write('%d,%d\n'%(self.batch_count,mini_seconds))
            f.close()
        else:
            f = open(log_time_path,'a')
            f.write('%d,%d\n'%(self.batch_count,mini_seconds))
            f.close()
    def sort_articles_by_date(self,aids):
        return aids
            
    def calculate_new_data(self):
        logger.info('read new data at: %s'%(str(datetime.datetime.now())))
        t1 = datetime.datetime.now()
        (tf_corpus,corpus,domain_dic,cat_dic,article_dic) = self.reader.get_data()
        t2 = datetime.datetime.now()
        logger.info('time for getting new data: %s'%str(t2-t1))
        if (corpus):
            #if (self.ob_reader.mini_index == 300):
#                print self.clusters
            #    sys.exit(1)
	    new_aids = corpus.keys()
            logger.info('start handling new articles: %d'%(len(corpus)))
	    logger.info('new batch: %s'%str(new_aids))
            self.batch_count += 1
            logger.info('batch_count = %d'%self.batch_count)
            self.handle_new_data2(tf_corpus,corpus,domain_dic,cat_dic,article_dic)
            logger.info('save processing time  into db, to calculate how long we process new article')
            process_dic = dict()
            p_datetime = str(datetime.datetime.now())
            for aid in new_aids:
                process_dic[aid] = p_datetime
            news_time_stat.insert_record(process_dic)
        else:
            logger.info('there are not enough data to handle')

    def write_event_order(self):
        save_path = real_time_resources2.EVENT_ORDER_PATH
        util.save_object(save_path,self.time_stam_dic)
    
    def find_identical_article_within_domain(self,tf_idf,title,create_time,domain_list):
        start_list = domain_list
        start_list = filter_by_create_time(create_time,start_list,self.articles)
        start_list = filter_by_title_sim(title,start_list,self.title_corpus)
        start_list = filter_by_corpus_sim(tf_idf,start_list,self.corpus)
        return start_list
        
    def import_new_data(self,temp_corpus,tf_corpus,domain_dic,article_dic):
        """
        find entities, check duplicate titles if duplicate titles with previous article, we ignore docs 
        """
        all_ids = temp_corpus.keys()
        logger.info('total new data: %d'%len(temp_corpus))
        t1 = datetime.datetime.now()
        temp_title_corpus = self.reader.get_title_text_sequence(all_ids)
        t2 = datetime.datetime.now()
        logger.info('time for fetching titles of: %s is : %s'%(str(all_ids),str(t2-t1)))
        logger.info('number of titles being fetched: %d'%len(temp_title_corpus))
        remain_ids = []
        iden_ids = []
        for aid in all_ids:
            if aid not in temp_title_corpus:
                self.title_corpus[aid] = None
                remain_ids.append(aid)
            else:
                self.title_corpus[aid] = temp_title_corpus[aid]
        logger.info('non_title ids: %s'%str(remain_ids))
        (inner_remain,inner_removed ) = find_duplicate(temp_title_corpus.keys(),temp_corpus,\
        domain_dic,article_dic,temp_title_corpus) # find duplicate within clusters# (all_title_ids,temp_corpus,domain_dic,article_dic,titles)
        logger.info('removed duplicates: %s'%str(inner_removed))
        logger.info('remain_identical within: %d'%len(inner_remain))
        for aid in inner_remain:# duplicate with previous data
            domain = domain_dic[aid]
            domain_arts = []
            if domain in self.domain_distr:
                domain_arts = self.domain_distr[domain]
            iden = self.find_identical_article_within_domain(temp_corpus[aid],temp_title_corpus[aid],article_dic[aid][dao.CREATE_TIME],domain_arts)
            if (len(iden) > 0):
                logger.info('article: %d is duplicate in same domain with: %s'%(aid,str(iden)))
                iden_ids.append(aid)
            else:
                remain_ids.append(aid)
                self.title_corpus[aid] = temp_title_corpus[aid]
                logger.info('article: %d is a new article'%aid)
        t21 = datetime.datetime.now()
        logger.info('time for checking duplicate: %s'%str(t21-t2))
        logger.info('artiles duplicate with the pre: %s'%str(iden_ids))
        util.migrate_list2_to_list1(iden_ids,inner_removed)
        logger.info('add: %d, remove_duplicate: %d'%(len(remain_ids),len(iden_ids)))
        #logger.info('importing new data: %s'%str(temp_corpus.keys()))
        for key in remain_ids:
            self.corpus[key] = temp_corpus[key]
            self.tf_corpus[key] = tf_corpus[key]
            self.domain_dic[key] = domain_dic[key]
            util.add_element_to_dic(self.domain_distr,key,domain_dic[key])
        for aid in iden_ids:
            del temp_corpus[aid]
        util.migrate_dic2_to_dic1(self.articles,article_dic)
        # remove identical 
        aids = remain_ids                    
        t1 = datetime.datetime.now()
        en_corpus = real_time_reader2.get_entity_corpus(aids)
        for aid in en_corpus:
            self.entities[aid] = en_corpus[aid]
        for aid in aids:
            if aid not in self.entities:
                logger.info('aid: %d does not has entitties '%aid)
        t2 = datetime.datetime.now()
        logger.info('time for fetching entities: %s'%str(t2-t1))
            
    def import_dic_info(self,temp_cat_dic):
        for key in temp_cat_dic:
            self.catdic[key] = temp_cat_dic[key]
    
    def get_dominant_catid(self,aids):
        total_cat = dict()
        for aid in aids:
            catid = self.catdic[aid]
            if (catid in total_cat):
                total_cat[catid] += 1
            else:
                total_cat[catid] = 1
        max_key = util.get_max_key_dic(total_cat)
        return max_key#total_cat[max_key]
        #get related clusters
    def get_new_event(self,art_ids,event_id,center):
        #TO-DO new event are formed 
        """
        A new event comes into being, either by completely new event or being separated from a big event or merging two events 
        """
        date = util.get_today_str()
        event = Event(event_id,date)
        event.set_articles(art_ids)
        coherence = detect_event_util.get_coherence_of_cluster(art_ids,self.corpus,center)
        event.set_coherence(coherence)
        catid = self.get_dominant_catid(art_ids)
        event.set_catid(catid)
        event.set_timestamp(self.time_stam)
        event.set_center(center)
        self.find_trend_of_event(event)#check whether this event is connected to previous trend
        logger.info('SUB_EVENT: %d'%event_id)
        #self.sub_event(event)
        #event.fire_output(get_sub_event_path(event_id,self.batch_count))
        logger.info('RUN SUB_EVENT_HIER FOR EVENT_ID %d WITH TREND_ID %d'%(event_id,event.trend_id))
        listener_collection.detect_new_event(event.articles,event.id,event.trend_id)
        return event
        
    def add_event_from_separate(self,clusters,centers,sizes,remove_event_set):
        """
        clusters is a cluster_dic, these cluster may contain id of event
        remove_event_set to add removed events into it
        """
        #remove small events first
        (del_clusters2,del_centers2,del_size2)  = detect_event_util.get_remove_small_events_with_catdic(\
        clusters,centers,sizes,real_time_resources2.SAVE_DB_THRESHOLD)
        remove_eids = self.update_new_clusters(del_clusters2,del_centers2)#these small events may contains event_ids
	logger.info('remove_id in db: %s'%str(remove_eids))
        for eid in remove_eids:
            remove_event_set.add(eid)
        event_ids = Set()
        keys = clusters.keys()
        except_db_ids = Set()
        remove_event_ids = []
        for key in keys:# what if two events share the same max_key?
            #util.migrate_list2_to_list1(event_articles,clusters[key])
            in_db_ids = self.get_in_db_idlist(clusters[key])#get_intersection_list_dic(clusters[key],self.events)
            logger.info('SEPARATE OR MERGED %s in db'%str(in_db_ids))
            for r_eid in in_db_ids:#whether event is merged or separated, event_ids are always REMOVED
                logger.info('REMOVE SUB_EVENTS OF EVENT: %d'%r_eid)
                #sub_util2.delete_sub_of_event(r_eid)
                listener_collection.remove_event(r_eid)
            new_key = key # find id for event_id, we prefer event_id of previous event
            if (len(in_db_ids) > 0):
                logger.info('in_db_ids: %s'%str(in_db_ids))
                new_key = choose_new_key(centers[key],except_db_ids,in_db_ids,self.centers)#choose new event_id from in_db_list (previous event_id) by getting similarity of centers
                if new_key == -1:
                    new_key = key
                else:
                    except_db_ids.add(new_key)
                except_db_ids.add(new_key)
                for db_id in in_db_ids:
                    remove_event_ids.append(db_id)
                if (len(in_db_ids) > 1):
                    logger.info('IMPORTANT -------------------- MERGING TWO EVENTS ON DB -------------: %s'%str(in_db_ids))
                else:#only one 
                    logger.info('UPDATE(INCREASE OR DECREASE) FOR EVENT: %s'%str(in_db_ids))
                change_key_dic(clusters,key,new_key)
                change_key_dic(centers,key,new_key)
                #change_key_dic(good_dic,key,new_key)
            else:
                logger.info('A NEW EVENT HAS BEEN DETECTED: %d: %s'%(key,str(clusters[key])))
            # remove sub_ids of merged event

            t_event = self.get_new_event(clusters[new_key],new_key,centers[new_key])
            t_event.set_timestamp(self.time_stam)
            self.events[new_key] = t_event#add new events
            event_ids.add(new_key)
        for eid in remove_event_ids:
            #sub_util2.delete_sub_of_event(eid)# when 2 events are merged or separated, we have to delete all sub_records
            remove_event_set.add(eid)
            if eid not in event_ids: # if some events are merged, remove events that are not retained
                del self.events[eid]
        return list(event_ids)
    def update_event_infor_for_model(self,event_ids):
        for event_id in event_ids:
            self.centers[event_id] = self.events[event_id].center
            self.clusters[event_id] = list(self.events[event_id].articles)
    def update_coherence_events(self,event_ids):
        for event_id in event_ids:
            event = self.events[event_id]
            event_path = get_sub_event_coherence_path(event_id)
            f = open(event_path,'a')
            f.write('%d,%d,%f\n'%(self.batch_count,event.size,event.coherence))
            f.close()

    def handle_new_data2(self,tf_corpus,d_corpus,domain_dic,cat_dic,article_dic):
        self.time_stam += 1
        logger.info('import new data-representation to global corpus')
        self.import_new_data(d_corpus,tf_corpus,domain_dic,article_dic)
        
        self.import_dic_info(cat_dic)
        t1 = datetime.datetime.now()
        # add new document in corpus to current cluster
        logger.info('find related clusters in accumulating clusters, with %d new documents'%len(d_corpus))
        p_t1 = datetime.datetime.now()
        #(related_dic,remain_ids) = cluster_doc2.get_related_clusters_for_data(corpus,self.centers,real_time_resources2.RELATED_CLUSTER_THRESHOLD)
        self.get_count_statistics()
        related_set = get_related_clusters_for_data(d_corpus,self.centers,\
        real_time_resources2.RELATED_CLUSTER_THRESHOLD)
        p_t2 = datetime.datetime.now()
        logger.info('time for finding related clusters: %s'%str(p_t2-p_t1))
        if (self.batch_count == 300):
            print 'hit'
        self.get_count_statistics()
        p_t1 = datetime.datetime.now() 
        bucket = dict()
        logger.info('extract %s related clusters from program'%str(related_set))
        #logger.info(self.clusters.keys())
        #logger.info(self.centers.keys())
        (centers,sizes,clusters) = self.copy_clusters(related_set,bucket)#bucket to hold catid, ---> create merge_buket
        #logger.info('extracting result: %s,%s'%(str(sizes),str(clusters)))
        p_t2 = datetime.datetime.now()
        logger.info('time for copying related clusters: %s'%str(p_t2-p_t1))
        
        logger.info('add new articles to clusters')
        add_isolated_new_data(d_corpus,cat_dic,clusters,centers,sizes,bucket)#add doc as a cluster 
        
        del_clusters = dict()#after being clustered, there might be some clusters still don't have enough size to become event
        del_centers = dict()
        if (len(clusters) > 200):
            logger.info('start clustering with bucket, clusters_size: %d'%(len(clusters)))
            p_t1 = datetime.datetime.now()
            (clusters,centers,sizes) = cluster_with_buckets(clusters,centers,sizes,bucket,resources.DAY_THRESHOLDS)
            logger.info('RESULT AFTER CLUSTERING WITH BUCKETS: ')
            logger.info(clusters)
            logger.info('================================================')
            p_t2 = datetime.datetime.now()
            logger.info('time for bucket clustering new data: %s'%(p_t2-p_t1))
                
            logger.info('removeing small clusters for the first time')
            remove_threshold = real_time_resources2.HIERARCHICAL_REMOVE_THRESHOLD
            #get_remove_small_events_with_catdic to remove clusters that cannot become events (mainly check size) 
            (del_clusters,del_centers,del_sizes)  = detect_event_util.get_remove_small_events_with_catdic(clusters,centers,sizes,remove_threshold)
            logger.info('number of removed_clusters: %d, remain: %d clusters'%(len(del_clusters),len(clusters)))
            logger.info('RETAINING RESULTS: %s'%str(clusters))
            #remove_id_db_set = self.update_new_clusters(del_clusters,del_centers)
        p_t1 = datetime.datetime.now()
        logger.info('start hierarchical clustering')
        cluster_doc2.hierarchical_clustering(clusters,centers,sizes,real_time_resources2.HIERARCHICAL_THRESHOLD)#resources.HIERARCHICAL_THRESHOLD        
        p_t2 = datetime.datetime.now()
        logger.info('result from hierarchical clustering: %s'%str(clusters))
        logger.info('time for hierarchical clustering: %s'%(p_t2-p_t1))
        logger.info('removing small clustes after hierarchical clustering: ')
        #get_remove_small_events_with_catdic to remove clusters that cannot become events (mainly check size) 
        (del_clusters2,del_centers2,del_sizes2) = detect_event_util.get_remove_small_events_with_catdic(clusters,centers,sizes,real_time_resources2.SAVE_DB_THRESHOLD)
        util.migrate_dic2_to_dic1(del_clusters,del_clusters2)
        util.migrate_dic2_to_dic1(del_centers,del_centers2)
        logger.info('separating, finding bad cluster')
        p_t1 = datetime.datetime.now()
        
        logger.info('start considering each cluster after hierarchical clustering')
        keys = clusters.keys()
        save_event_ids = []# events that need to be saved (include events being updated and completely new event)
        #event_articles = [] #list of related articles in events to find entities
        remove_event_ids = self.update_new_clusters(del_clusters,del_centers)#includes totally new events and events being updated
        pt1 = datetime.datetime.now()
        for key in keys:
            #clusters[key] = self.recover_full_cluster(clusters[key])#get full articles to decide 
             eids = self.get_in_db_idlist(clusters[key])#get list of event_ids from this cluster
             if (len(eids) == 0):# this cluster doesn't contain any event. a totally new event
                 logger.info('NEW EVENT HAS BEEN DETECTED: %d : %s'%(key,str(clusters[key])))
                 event = self.get_new_event(clusters[key],key,centers[key])
                 self.events[key] = event
                 save_event_ids.append(key)
             elif (len(eids) == 1) and len(clusters[key]) > 1 :#This cluster contains 1 event_id, we need to check whether this cluster is good or bad, 
                 full_articles = self.recover_full_cluster(clusters[key])#get full articles (merge articles from event with new artiles)
                 #print '=========FULL ARTICLE==============='
                 #print full_articles
                 (t_clusters,t_centers,t_sizes,t_good_dic) = real_time_detect_util.review_one_cluster(key,full_articles,centers[key],sizes[key],self.corpus,\
                 resources.COHERENCE_THRESHOLD,resources.SEPARATE_THRESHOLDS,resources.HIERARCHICAL_THRESHOLD_SEPARATE) #cluster further this cluster
                 if (len(t_clusters) ==1):#this event is not separated and new articles being added
                     event_id = eids[0]
                     self.add_new_article_to_event(event_id,clusters[key],centers[key])# add new articles to event, update cluster and centers
                     save_event_ids.append(event_id)
                     remove_event_ids.add(event_id)
                     #util.migrate_list2_to_list1(event_articles,full_articles)
                 else:#event is separated into several smaller events
                     #delete all sub_id in this event, we delete previous events and form new one
                     for r_id in eids:
                         listener_collection.remove_event(r_id)
                         #sub_util2.delete_sub_of_event(r_id)
                     logger.info('%s ARE SEPARATED'%eids[0])
                     # this includes removing eid in clusters and add new event_ids
                     temp_event_ids = self.add_event_from_separate(t_clusters,t_centers,t_sizes,remove_event_ids)# process clusters containing event_ids
                     util.migrate_list2_to_list1(save_event_ids,temp_event_ids)
                     
             elif(len(eids) > 1):
                 for r_id in eids:
                     listener_collection.remove_event(r_id)
                     #sub_util2.delete_sub_of_event(r_id)
                 logger.info('%s HAS BEEN MERGED INTO %s'%(str(eids),str(clusters[key])))
                 full_articles = self.recover_full_cluster(clusters[key])
		 logger.info('full_articles: %s'%str(full_articles))
                 (t_clusters,t_centers,t_sizes,t_good_dic) = real_time_detect_util.review_one_cluster(key,full_articles,centers[key],sizes[key],self.corpus,\
                 resources.COHERENCE_THRESHOLD,resources.SEPARATE_THRESHOLDS,resources.HIERARCHICAL_THRESHOLD_SEPARATE)
		 logger.info('RESULT FROM THIS MERGED CLUSTERS: %s'%str(t_clusters))
                 temp_event_ids = self.add_event_from_separate(t_clusters,t_centers,t_sizes,remove_event_ids)
                 util.migrate_list2_to_list1(save_event_ids,temp_event_ids)
                         
        self.remove_events(remove_event_ids)#remove event_ids from dic
        logger.info('Removing events: %s'%str(remove_event_ids))
        self.update_event_infor_for_model(save_event_ids)
        #self.add_events(clusters,centers)
        #if (len(save_event_ids) > 0):
        #    self.update_coherence_events(save_event_ids)
        pt2 = datetime.datetime.now()
        logger.info('time for considering each cluster: %s'%str(pt2-pt1))
        #now clusters,centers,sizes are from those being inserted to db
        # we need to get old_id to set to new events, 
        if (len(remove_event_ids) > 0):
            logger.info('remove ids for update or insert new: %s'%str(remove_event_ids))
            self.remove_event_in_db(remove_event_ids)
            
        if (len(save_event_ids) > 0):
            #self.write_event_order()
            logger.info('inserting events: %s to table: %s'%(str(save_event_ids),resources.EVENT_CURRENT_TABLE))
            t12a = datetime.datetime.now()
            self.save_event_to_db(save_event_ids,resources.INSERT_EVENT_DB)
            t12 = datetime.datetime.now()
            logger.info('time for saving events: %s'%(t12-t12a))       

        t2 = datetime.datetime.now()
        interval = t2-t1
        logger.info('time for handling new data: %s'%str(interval))
        self.log_time(interval.microseconds)
        #get related clusters

    def recover_full_cluster(self,cluster):
        result = []
        for key in cluster:
            if key in self.events:
                util.migrate_list2_to_list1(result,self.clusters[key])
            else:
                result.append(key)
        return result
    
    def get_db_id_of_cluster(self,clusters):
        """
        return event_id being contained by clusters to remove these events
        """
        result = Set()
        for key in clusters:
            in_els = self.get_in_db_idlist(clusters[key]) #get_intersection_list_dic(clusters[key],self.events)
            for el in in_els:
                result.add(el)
        return result
    
    def add_new_article_to_event(self,event_id,cluster,center):
        """
        this event is added with new articles (in cluster and center is new center for event)
        """
        t_event = self.events[event_id]
        additional_aids = []#articles being added to event
        for aid in cluster:
            if (aid != event_id):
                self.clusters[event_id].append(aid)
                additional_aids.append(aid)
        t_event.add_new_articles(additional_aids)#add new articles to event
        self.centers[event_id] = center#update center when new articles are added
        coherence = detect_event_util.get_coherence_of_cluster(self.clusters[event_id],self.corpus,center)#calculate coherence
        t_event.set_coherence(coherence)
        t_event.set_timestamp(self.time_stam)
        t_event.set_center(center)
        # find old and new articles
        logger.info('ADD NEW ARTICLES: %s TO EVENT: %d'%(str(additional_aids),event_id))
        logger.info('UPDATE SUB_EVENT: %d'%event_id)
        self.find_trend_of_event(t_event)#re-compute trend_id, whether trend_id of this event changes
        listener_collection.new_articles_to_event(additional_aids,event_id,t_event.trend_id)
        #self.update_sub_event(t_event,additional_aids)
        #t_event.fire_output(get_sub_event_path(event_id,self.batch_count))
    #save to nsub_trend_folder

    def update_sub_event(self,event,aids):
        #TO-DO an event being added with new articles
        """
        this event are being added with new aids, we need to 
        """
        new_new_ids = []
        old_update_sub_ids = Set()
        for aid in aids:
            sub_id = self.check_connect_sub_ornot(event.trend_id,event.tf_corpus[aid])
            if (sub_id):
                self.old_dic[aid] = sub_id
                event.update_new_old_article(aid,sub_id,self.articles)
                old_update_sub_ids.add(sub_id)
            else:
                new_new_ids.append(aid)
                event.new_ids.append(aid)
        event.update_changed_sub_event_in_db(old_update_sub_ids,event.old_clusters)
        if event.rerun_sub:
            logger.info('RERUN_SUB EVENT: %d'%event.id)
            self.sub_event(event)
            #update_article_represent(event)
            #event.corpus_size = event.size
            #event.rerun_sub = False
            #update_sub_event_by_add(event,aids)
        else:
            self.update_sub_event_by_add(event,new_new_ids)
    
    def get_in_db_idlist(self,ids):
        """
        return event_ids within ids
        """
        result = []
        for eid in ids:
            if (eid in self.events):
                result.append(eid)
        return result
    def get_additional_clusters(self,cluster_id_set,size_dic,bucket,clusters,sizes,centers,cluster_cat_dic,total_cat_dic,corpus):
        for eid in cluster_id_set:
            clusters[eid] = [eid]
            catid = get_dominated_catid(self.cluster_cat_dic[eid])
            add_aid_to_bucket(bucket,eid,catid)
            sizes[eid] = self.sizes[eid]
            size_dic[eid] = sizes[eid]
            cluster_cat_dic[eid] = self.cluster_cat_dic[eid].copy()
            centers[eid] = self.centers[eid].copy()
            corpus[eid] = self.centers[eid].copy()
            total_cat_dic[eid] = self.cluster_cat_dic[eid].copy()
    
    def copy_clusters(self,cluster_ids,bucket):
        """
        this function extract cluster_id in cluster_ids, including both small clusters and big clusters (considered as event)
        """
        sizes = dict()
        clusters = dict()
        centers = dict()
        for eid in cluster_ids:
            if eid in self.events:#if cluster being extracted is an event, we still hold centers and sizes
                clusters[eid] = [eid]
                sizes[eid] = len(self.clusters[eid])
                centers[eid] = self.centers[eid].copy()
                catid = self.get_dominant_catid(self.clusters[eid])# get catid of the cluster
                add_aid_to_bucket(bucket,eid,catid)#add this cluster to bucket
            else:# IF cluster being extracted is not an event, this might be a small cluster (not enough to form event) or event cluster size = 1
                clusters[eid] = self.clusters[eid]
                sizes[eid] = len(clusters[eid])
                centers[eid] = self.centers[eid].copy()
                catid = self.get_dominant_catid(clusters[eid])
                add_aid_to_bucket(bucket,eid,catid)
                del self.centers[eid] # pop it, considered as new data
                del self.clusters[eid]
            #del self.cluster_cat_dic[eid]
        return (centers,sizes,clusters)
    def get_count_statistics(self):
        print 'cluster_count = %d'%len(self.clusters)
        print 'centers_count = %d'%len(self.centers)
    
    def update_new_clusters(self,clusters,centers):# return removed_events from db
        """
        these clusters and centers are added to clusters and centers of the class, there may be some cases in which clusters.keys() contains event_ids of detected events and
        we need to remove these events because this happens when, for example, one event with additional articles become separated into small clusters, and the cluster containing 
        event_id is not big enough to become event, we need to remove.
        and unfortunately, these small clusters, once become event_id, now no longer be event_id
        """
        db_remove_events = self.get_db_id_of_cluster(clusters)#find ids in db that are in these clusters
        util.migrate_dic2_to_dic1(self.clusters,clusters)
        util.migrate_dic2_to_dic1(self.centers,centers)
        return db_remove_events
        
    def add_new_clusters(self,clusters,centers):
        """
        if the key exists, replace old, if not new clusters are formed
        """
        for key in clusters:
            util.migrate_dic2_to_dic1(self.clusters,clusters)
            util.migrate_dic2_to_dic1(self.centers,centers)
    
    def remove_events(self,event_ids):
        for event_id in event_ids:
            del self.clusters[event_id]
            del self.centers[event_id]
            #del self.db_event_trend[event_id]
            
    def add_events(self,clusters,centers):
        self.add_new_clusters(clusters,centers)
        
    def find_trend_of_event(self,event):
        today = util.get_today_str()
        event_id = event.id
        if (event.refind_trend):
            logger.info('RE CALCULATE TREND_ID FOR EVENT: %d'%event_id)
            #TO-DO  temporarily set trend_id = -1
            #event.set_trend_id(-1)
            #return;
            aids = event.articles
            logger.info('compute entity center for aids: %s'%str(aids))
            en_center = compute_entity_for_aids(self.entities,aids)# compute entity center from aids
            event.en_center = en_center
            (temp_score,max_key,detail) = detec_trend.get_best_trend(self.trends,event.center,en_center,today,event_id,len(event.articles))
            logger.info('detail for finding trend_id of %d: %s'%(event_id,str(detail)))
            connect = detec_trend.connect_trend_with_score(temp_score)
            trend_id = -1
            if (connect):
                trend_id = max_key
            event.set_trend_id(trend_id)
    
    def remove_event_in_db(self,event_ids):
        if (event_ids):
            remove_redundant_events(event_ids)
            
    def save_event_to_db(self,event_ids,insert_to_db=True):
        if len(event_ids) == 0:
            return 
        yesterday = util.get_yesterday_str()
        vocab_map = util.get_dictionary_map(trend_util.get_dictionary_path(yesterday))
        cluster_ids = []
        aid_urls = []
        article_nums = []
        if (insert_to_db):
            conn = dao.get_connection()
            cur = conn.cursor()
        num_record = 0
        event_tag_dic = dict()
        day = util.get_today_str()
        for event_id in event_ids:
            event = self.events[event_id]
            top_img_path = real_time_resources2.IMG_FOLDER+ '/top_%d.png'%event_id
            trend_id = event.trend_id
            articles = event.articles
            aid_urls.append(util.get_in_where_str(articles))            
            debug_docs(articles,event.center,event_id,vocab_map)
            if (insert_to_db):
                #sizes[key] = len(clusters[key])
                catid = event.catid
                url_dict = dict()
                url_dict[0] = articles
                insert_events(event_id,day,url_dict,event.center,len(articles),top_img_path,event.coherence,cur,catid,trend_id)
                event_tag = get_hashtag_for_event(event_id,articles,real_time_resources2.TEXT_FOLDER+'/top_%d.txt'%(event_id))
                event_tag_dic[event_id] = event_tag
            num_record += 1
            
            cluster_ids.append(event_id)
            article_nums.append(len(articles))
        if (insert_to_db):
            logger.info('save events on db')
            conn.commit()
            dao.free_connection(conn,cur)
            logger.info('Number of events (new or old) on batch is %d'%(len(event_ids)))
        report_path = real_time_resources2.HTML_REPORT_PATH
        gen_html_report(cluster_ids,aid_urls,article_nums,report_path)
        insert_event_hashtag(event_tag_dic)
    def finish_today(self):
        #TODO update at night
        logger.infor('update dictionary')
        t1 = datetime.datetime.now()
        data_reader.update_dictionary_with_articles(self.corpus.keys(),TODAY,self.catdic)
        t2 = datetime.datetime.now()
        logger.info('time for updating dictionary and saving data: %s'%str(t2-t1))
        logger.info('copy events from %s to %s'%(resources.EVENT_CURRENT_TABLE,resources.EVENT_TABLE))
        t1 = datetime.datetime.now()
        event_dao.migrate_from_table2_to_table1(resources.EVENT_TABLE,resources.EVENT_CURRENT_TABLE)
        t2 = datetime.datetime.now()
        logger.info('time for copying event: %s'%str(t2-t1))
        logger.info('saving events\'s centers')
        centers = dict()
        en_centers = dict()
        sizes = dict()
        for event in self.events:
            centers[event.id] = event.center
            en_centers[event.id] = event.en_center
            sizes[event.id] = event.size
        logger.info('save context centers for events')
        trend_util.save_event_centers_context(centers,sizes,TODAY)
        logger.info('save en_centers for events')
        trend_util.save_event_centers_entity(en_centers,sizes,TODAY)  
        logger.info('connect trend for new day')
        detec_trend.compute_trend(TODAY,True)
        #TODO copy sub_trend & update parameters of sub_trend
#def create_thread_finding_imgs(aids):
#    thread.start_new_thread(process_find_imgs,(aids,))
#    
#def process_find_imgs(aids):
#    print 'start finding img thread'
#    i_real_time.process(aids)
#    print 'finish finding img thread'

def get_intersection_list_dic(l,dic):
    res = []
    for el in l:
        if el in dic:
            res.append(el)
    return res

def get_aids_from_cluster(clusters,ids):
    result = []
    for key in ids:
        for el in clusters[key]:
            result.append(el)
    return result

def get_center_from_cluster_dic(corpus,cluster_dic):
    centers = dict()
    for key in cluster_dic:
        centers[key] = corpus[key]
    return centers

def get_art_map_from_cluster_dic(cluster_dic):
    aids = []
    for key in cluster_dic:
        for aid in cluster_dic[key]:
            aids.append(aid)

def gen_html_report(cluster_ids,aid_urls,article_nums,save_path):
    event_imgs = []
    for i in range(len(cluster_ids)):
        key = cluster_ids[i]
        img_path = real_time_resources2.IMG_FOLDER+ '/top_%d.png'%key
        event_imgs.append(img_path)
    util.gen_html_event(event_imgs, save_path, article_nums, aid_urls)

def insert_event_hashtag(event_tag_dic):
    logger.info('update hashtag for events: %d'%len(event_tag_dic))
    event_dao.update_hash_tag_for_events(event_tag_dic,resources.EVENT_CURRENT_TABLE)

def insert_events(event_id,date,url_dicts,center,size,img_path,coherence,cur,catid,trend_id):
    article_ids_str = detect_event_util.get_article_ids_str(url_dicts)
    center_str = str(center)
    query = online_dao.create_insert_events_query(event_id,date,article_ids_str,center_str,size,img_path\
    ,coherence,catid,trend_id,resources.EVENT_CURRENT_TABLE)
    logger.info('insert event: %d'%event_id)
    #logger.info('%s'%query)
    #print 'query: %s'%query
    cur.execute(query)
    
def debug_docs(cluster,center,key,vocab_map):
    text_folder = real_time_resources2.TEXT_FOLDER
    detect_event_util.fetch_titles(cluster,text_folder+'/%d'%(key))
    top_path = text_folder+'/top_%d.txt'%(key)
    cluster_doc2.visualize_centre(vocab_map,top_path,center)
    top_img_path = real_time_resources2.IMG_FOLDER+ '/top_%d.png'%key
    top_word_img.generate_img(top_path,top_img_path)

def get_hashtag_for_event(event_id,aids,top_path):
    result = hashtag.gen_hashtag_from_articles(aids)
    if (len(result) > 3):
        result = result.replace('\'','\\\'')
        return result
    result = hashtag.gen_hashtag_from_top_file(top_path)
    result = result.replace('\'','\\\'')
    return result


def remove_temp_events():
    event_dao.delete_all_record_in_table(resources.EVENT_CURRENT_TABLE)
    #event_dao.delete_all_record_in_table(sub_util2.TABLE_NAME)
    listener_collection.start_new_day()
    #event_dao.delete_all_record_in_table(fill_news_visual.TABLE_NAME)
    #event_dao.delete_all_record_in_table(news_time_stat.TABLE_NAME)
    #event_dao.delete_all_record_in_table(real_sub_trend.REAL_TABLE)

def remove_pre_data():
    img_folder = real_time_resources2.IMG_FOLDER
    if (os.path.exists(img_folder)):
        print 'remove all image files in %s'%img_folder
        util.remove_all_files_in_folder(img_folder)
    text_folder = real_time_resources2.TEXT_FOLDER
    if (os.path.exists(text_folder)):
        print 'remvoe all text files in %s'%text_folder
        util.remove_all_files_in_folder(text_folder)

def change_key_dic(dic,old_key,new_key):
    dic[new_key] = dic.pop(old_key)
    
def get_cat_dic(clusters,total_cat_dic):
    cat_dic_last = dict()
    for key in clusters:
        cat_dic_last[key] = real_time_detect_util.get_art_dic(clusters[key],total_cat_dic) 
    return cat_dic_last

def get_ids_in_dbid(ids,id_db_set):
    a_list = []
    for i in range(len(ids)):
        key = ids[i]
        if key in id_db_set:
            a_list.append(i)
    return a_list

def migrate_clusters(clusters1,centers1,sizes1,clusters2,centers2,sizes2,threshold):
    keys = centers2.keys()
    acc_centers = dict()
    remain_ids = []
    for key in centers1:
        acc_centers[key] = [(centers1[key],sizes1[key])]
    for key in keys:
        center = centers2[key]
        distance = cluster_doc2.compute_distance(center,centers1)
        if (len(distance) > 0):
            max_key = cluster_doc2.get_max_key_dic(distance)
            if(distance[max_key] > threshold):
                ids = clusters2.pop(key)
                for element in ids:
                    clusters1[max_key].append(element)
                sizes1[max_key] += sizes2[key]
                acc_centers[max_key].append((centers2[key],sizes2[key]))
            else:
                remain_ids.append(key)
    #update centers clusters1
    print 'update merging '
    for key in clusters1:
        if (len(acc_centers[key]) > 1):
            centers1[key] = util.update_center_with_center_weight_pair(acc_centers[key])
    for key in remain_ids:
        clusters1[key] = clusters2[key]
        sizes1[key] = sizes2[key]
        centers1[key] = centers2[key]
        

def get_dominated_catid(cat_size_dic):
    keys = cat_size_dic.keys()
    max_catid = keys[0]
    temp = cat_size_dic[max_catid]
    for catid in cat_size_dic:
        if (temp < cat_size_dic[catid]):
            temp = cat_size_dic[catid]
            max_catid = catid
    return max_catid

def add_aid_to_bucket(bucket,aid,catid):
    if catid not in bucket:
        bucket[catid] = [aid]
    else:
        bucket[catid].append(aid)

def update_cluster_and_insert_with_new_doc(size_dic,centers,sizes,cluster_cat_dic,res_score,corpus,cat_dic,merge_threshold):
    """
    this function has several roles:
        reduce clusters --> typical and output a mapper from typical to all
        corpus for current clusters + new
        centers for current clusters + new 
    """
    bucket = dict()
    clusters = dict()
    total_cat_dic = dict()
    for cluster_id in res_score:
        size_dic[cluster_id] = sizes[cluster_id]
        corpus[cluster_id] = centers[cluster_id]
        clusters[cluster_id] = [cluster_id]
        total_cat_dic[cluster_id] = cluster_cat_dic[cluster_id]
        doc_infor = res_score[cluster_id]
        center_weight_pairs = [(centers[cluster_id],sizes[cluster_id])]
        for pair in doc_infor:
            (aid,score) = pair
            catid = cat_dic[aid]
            total_cat_dic[aid] = {catid:1}
            size_dic[aid] = 1
            if (score >= merge_threshold):
                center_weight_pairs.append((corpus[aid],1))
                sizes[cluster_id] += 1
                clusters[cluster_id].append(aid)
                cluster_doc2.merg_cat2_to_cat1(cluster_cat_dic[cluster_id],total_cat_dic[aid])
            else:# add a new clusters
                clusters[aid] = [aid]
                centers[aid] = corpus[aid]
                sizes[aid] = 1
                catid = cat_dic[aid]
                cluster_cat_dic[aid] = {catid:1}
                add_aid_to_bucket(bucket,aid,catid)
        dominated_catid = get_dominated_catid(cluster_cat_dic[cluster_id])
        add_aid_to_bucket(bucket,cluster_id,dominated_catid)
        new_center = util.update_center_with_center_weight_pair(center_weight_pairs)
        centers[cluster_id] = new_center
    return (bucket,clusters,sizes,total_cat_dic)

def cluster_with_buckets(clusters,centers,sizes,bucket,thresholds):
    logger.info('before merging size: %d'%len(clusters))
    res_clusters = dict()
    res_centers = dict()
    res_sizes = dict()
    keys = bucket.keys()
    merge_threshold = thresholds[-1]
    for i in range(len(keys)):
        key = keys[i]
        logger.info('merging bucket: %s with size: %d'%(str(key),len(bucket[key])))
        n_clusters = dict()
        n_centers = dict()
        n_sizes = dict()
        for cluster_id in bucket[key]:
            n_clusters[cluster_id] = clusters[cluster_id]
            n_centers[cluster_id] = centers[cluster_id]
            n_sizes[cluster_id] = sizes[cluster_id]
        (n_clusters,n_centers,n_sizes) = cluster_doc2.cluster_docs_with_thresholds(n_clusters,\
        n_centers,n_sizes,thresholds,3)
        logger.info('bucket size change: %d'%(len(bucket[key]) - len(n_clusters) ) )
        if (i == 0):
            res_centers = n_centers
            res_clusters  = n_clusters
            res_sizes = n_sizes
        else:
            logger.info('migrating from bucket: %s'%(str(key)))
            before_migrate_size = len(res_clusters)
            add_size = len(n_clusters)
            migrate_clusters(res_clusters,res_centers,res_sizes,n_clusters,n_centers,n_sizes,merge_threshold)
            after_migrate_size = len(res_clusters)
            logger.info('before migrate: %d, add_size = %d, after_migrate = %d, reduce = %d'%(before_migrate_size,\
            add_size,after_migrate_size,before_migrate_size+add_size-after_migrate_size))
            
    logger.info('final clusters size: %d'%len(res_clusters))
    return (res_clusters,res_centers,res_sizes)

def cluster_docs_with_thresholds(clusters,centers,sizes,cat_dic,cluster_thresholds,change_threshold=5):
    new_clusters = dict()
    new_centers = dict()
    new_sizes = dict()
    new_cat_dic = dict()
    for k in range(len(cluster_thresholds)):        
        stop_loop = False
        while(not stop_loop):
            t1 = time.time()
            before_count = len(clusters)
            merge_clusters2(new_clusters,new_centers,new_sizes,new_cat_dic,clusters,centers,sizes,cat_dic,cluster_thresholds[k])
            
            after_count = len(new_clusters)            
            clusters = new_clusters
            centers = new_centers
            sizes = new_sizes
            cat_dic = new_cat_dic
            
            decrease_count = before_count - after_count
            new_clusters = dict()
            new_centers = dict()
            new_sizes = dict()
            new_cat_dic = dict()
            
            t2 = time.time()
            print 'number of cluster: %d, decrease: %d, time: %d, threshold %f'%(after_count,decrease_count,t2-t1,cluster_thresholds[k])
            if (decrease_count < change_threshold):
                stop_loop = True
    return(clusters,centers,sizes,cat_dic) 

def merge_clusters2(clusters1,centers1,sizes1,cat_dic1,clusters2,centers2,sizes2,cat_dic2,cluster_threshold):
    #alpha = 1.5
    keys = centers2.keys()
    acc_centers = dict()
    for key in centers1:
        acc_centers[key] = [(centers1[key],sizes1[key])]
    for key in keys:
        center = centers2[key]
        distance = cluster_doc2.compute_distance(center,centers1)
        if (len(distance) > 0):
            max_key = cluster_doc2.get_max_key_dic(distance)
            if(distance[max_key] > cluster_threshold):
                #print 'merge: %d,%d with %f'%(max_key,key,distance[max_key])
                ids = clusters2.pop(key)
                for element in ids:
                    clusters1[max_key].append(element)
                #centers1[max_key] = update_center(centers1[max_key],sizes1[max_key],centers2[key],sizes2[key],alpha)
                sizes1[max_key] += sizes2[key]
                acc_centers[max_key].append((centers2[key],sizes2[key]))
                cluster_doc2.merg_cat2_to_cat1(cat_dic1[max_key],cat_dic2[key])
            else:
                centers1[key] = centers2[key]
                clusters1[key] = clusters2[key]
                sizes1[key] = sizes2[key]
                cat_dic1[key] = cat_dic2[key].copy()
                acc_centers[key] = [(centers2[key],sizes2[key])]
        else:
            centers1[key] = centers2[key]
            clusters1[key] = clusters2[key]
            sizes1[key] = sizes2[key]
            cat_dic1[key] = cat_dic2[key].copy()
            acc_centers[key] = [(centers2[key],sizes2[key])]
    #update centers clusters1
    print 'update merging '
    for key in clusters1:
        if (len(acc_centers[key]) > 1):
            centers1[key] = util.update_center_with_center_weight_pair(acc_centers[key])

def get_top_words_str(v,N,vocab_map):
    word_pairs = cluster_doc2.get_top_words(v,N,vocab_map)
    res_str = ''
    for pair in word_pairs:
        (w,weight) = pair
        res_str += '%s:%f '%(w,weight)
    return res_str
    
def remove_redundant_events(removed_ids):
    if (len(removed_ids) == 0):
        return
    real_list = []
    for el in removed_ids:
        real_list.append(el)
    logger.info('remove redundant events: %s'%str(removed_ids))
    conn = dao.get_connection()
    cur = conn.cursor()
    wstr = util.get_in_where_str(real_list)
    query = 'delete from %s where %s in (%s)'%(resources.EVENT_CURRENT_TABLE,resources.EVENT_ID,wstr)
    cur.execute(query)
    conn.commit()
    dao.free_connection(conn,cur)
    #remove texts and png

def get_entity_corpus_with_aids(aids):
    wstr = util.get_in_where_str(aids)
    url = 'http://10.3.24.83:8080/getentity/api/getentity/?id=%s'%wstr
    print url
    response = urllib.urlopen(url)
    corpus = util.str_to_dict(response.read())
    return corpus

def get_entity_corpus(aids,size=150):
    leng  = len(aids)
    corpus = dict()
    offset = 0
    while(True):
        offset = get_entity_corpus_in_chunk(aids,offset,size,corpus)
        if (offset == leng):
            break
    return corpus

def get_entity_corpus_in_chunk(aids,offset,size,corpus):
    last = min(offset+size,len(aids))
    temp_aids = aids[offset:last]
    temp_corpus = get_entity_corpus_with_aids(temp_aids)
    for key in temp_corpus:
        corpus[key] = temp_corpus[key]
    return last

def compute_entity_for_aids(corpus,aids):
    pairs = []
    for aid in aids:
        if aid in corpus:
            pairs.append((corpus[aid],1.0))
            logger.info('%d: %s'%(aid,str(corpus[aid])))
    en_center = util.update_center_with_center_weight_pair(pairs)
    return en_center

def get_aids_from_clusters(clusters):
    result = []
    for key in clusters:
        for el in clusters[key]:
            result.append(el)
    return result

def get_related_clusters_for_data(data_dic,centers,threshold):
    if (len(centers) == 0):#for the first time
        return ([])
    result = Set()
    for item_id in data_dic:
        v = data_dic[item_id]
        distances = cluster_doc2.compute_distance(v,centers)
        for key in distances:
            if (distances[key] >= threshold):
                result.add(key)
    return result

def get_sub_event_path(event_id,minibatch):
    real_sub_folder =  '/media/khaimai/F8C6516EC6512DDE/khai_folder/projects/trend_py/trendy_ploy1.0/test/sub_cluster/2016-02-04/%s'%TIME_STAMP
    util.create_folder(real_sub_folder)
    event_folder = real_sub_folder+'/%d'%(event_id)
    util.create_folder(event_folder)
    return event_folder+'/%d.html'%minibatch

def get_sub_event_coherence_path(event_id):
    real_sub_folder = '/media/khaimai/F8C6516EC6512DDE/khai_folder/projects/trend_py/trendy_ploy1.0/test/sub_cluster/2016-02-04/coherence_trace'
    event_path = real_sub_folder+'/%d.txt'%event_id
    return event_path
    

def get_merged_event(remove_ids,hold_ids):
    result = []
    for eid in remove_ids:
        if eid not in hold_ids:
            result.append(eid)
    return result

def choose_new_key(center,except_db_ids,in_db_ids,db_centers):
    temp = 0
    new_key = -1
    for key in in_db_ids:
        if (key not in except_db_ids):
            sim = util.get_similarity(center,db_centers[key])
            if (sim > temp):
                temp = sim
                new_key = key
    return new_key

def find_duplicate(all_title_ids,temp_corpus,domain_dic,article_dic,titles):
    domain_distr = dict()
    all_ids = list(all_title_ids)
    all_ids.sort(reverse= True) 
    remain_ids = []
    removed_ids = []
    
    for i in range(len(all_ids)):
        aid = all_ids[i]
        if aid in [63038101,63036212,63031705] :
            print 'hit'
        domain = domain_dic[aid]
        if domain not in domain_distr:
            util.add_element_to_dic(domain_distr,aid,domain_dic[aid])
            remain_ids.append(aid)
        else:#domain da co bai
            domain_list = domain_distr[domain]
            create_time = article_dic[aid][dao.CREATE_TIME]
            iden_list = find_identical_article_within_domain(temp_corpus[aid],titles[aid],create_time,domain_list,temp_corpus,titles,article_dic)
            if (len(iden_list) == 0):
                remain_ids.append(aid)
                util.add_element_to_dic(domain_distr,aid,domain_dic[aid])
            else:
                removed_ids.append(aid)
                logger.info('%d is duplicate with %s'%(aid,str(iden_list)))
    return (remain_ids,removed_ids)
                
def find_identical_article_within_domain(tf_idf,title,create_time,domain_list,corpus,titles,art_dic):
        start_list = list(domain_list)
        start_list = filter_by_create_time(create_time,start_list,art_dic)
        start_list = filter_by_title_sim(title,start_list,titles)
        start_list = filter_by_corpus_sim(tf_idf,start_list,corpus)
        return start_list

def filter_by_create_time(create_time,alist,art_dic):
        result  = []
        create_time_str = str(create_time)
        for aid in alist:
            if str(art_dic[aid][dao.CREATE_TIME]) == create_time_str:
                result.append(aid)
        return result
    
def filter_by_title_sim(title,alist,title_corpus,threshold = 0.8):
        result = []
        for aid in alist:
            temp = get_bin_sim_max(title,title_corpus[aid])
            if temp >= threshold:
                result.append(aid)
        return result
        
def filter_by_corpus_sim(tf_idf,alist,corpus,threshold = 0.98):
    result = []
    for aid in alist:
        temp = util.get_similarity(corpus[aid],tf_idf)
        if temp > threshold:
            result.append(aid)
    return result

def add_isolated_new_data(corpus,cat_dic,clusters,centers,sizes,bucket):
    for aid in corpus:
        clusters[aid] = [aid]
        centers[aid] = corpus[aid]
        sizes[aid]  = 1
        catid = cat_dic[aid]
        add_aid_to_bucket(bucket,aid,catid)

def get_cached_entity_path():
    return online_resources.CACHE_FOLDER + '/%s_entity.dat'%(TODAY)
def get_catched_title_path():
    return online_resources.CACHE_FOLDER+'/%s_title.dat'%TODAY

#def get_cached_entity():
    
        
def main():
    #try:
        updater = Real_Updater()
        # run at the first time
        logger.info('removing real_events and imgs,texts on previous day')
        remove_temp_events()
        remove_pre_data()
        while(True):
            day = util.get_today_str()
            run_day = updater.get_day()
            if (day != run_day):
                logger.info('stop running on %s'%run_day)
                sys.exit(1)
                #event_dao.delete_all_record_in_table(sub_util2.TABLE_NAME)
            #remove_temp_events()
#            try:
            updater.calculate_new_data()
#            except:
            #logger.info('SOME ERROR HAPPENED, entities and titles have been cachedd')
                #updater.save_cache_data()
                
        #print updater.clusters
            time.sleep(3)
   # except Exception, e:
   #     logger.info('The whole program stoped unexpectedly')
    #    logger.info('=================================ERROR============================================')
     #   logger.info(str(e))
      #  logger.info('=================================END_ERROR============================================')
if __name__ == '__main__':
    main()
#folder = '/media/khaimai/F8C6516EC6512DDE/khai_folder/projects/trend_py/trendy_ploy1.0/temp_data/server_emul2'
#corpus = dict()
#domain = dict()
#cat = dict()
#for i in range(552):
#    fpath = folder + '/%d.dat'%i
#    (t_corpus,t_domain_dic,t_cat_dic) = util.read_object(fpath)
#    util.migrate_dic2_to_dic1(corpus,t_corpus)
#    util.migrate_dic2_to_dic1(domain,t_domain_dic)
#    util.migrate_dic2_to_dic1(cat,t_cat_dic)
#util.save_object(folder+'/total.dat',(corpus,domain,cat))    
    
