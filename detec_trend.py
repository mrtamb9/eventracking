 #!/usr/bin/python
 # -*- coding: utf-8 -*-
import sys,os,event_trend_dao
import util
import logging
import resources,trend_util,tree
import numpy as np
import event_dao
#import trend_visualize
logger = logging.getLogger('detec_trend')
logger.setLevel(logging.INFO)
util.set_log_file(logger,resources.TREND_LOG)
CONTEXT_RATIO = 0.7
ENTITY_RATIO = 0.25
CLOSENESS_RATIO = 0.05
CONNECT_THRESHOLD = 0.45
TREND_LAST_SIZE = 'trend_last_size'

def update_trend_center(trend_center,trend_size,event_centers,event_sizes):
    """
    return new_center
    """
    center_weights = [(trend_center,1.0*trend_size)]
    for i in range(len(event_centers)):
        center_weights.append((event_centers[i],event_sizes[i]*resources.UPDATE_RATE))
    new_center = util.update_center_with_center_weight_pair(center_weights)
    return new_center

def update_center_with_weight(center1,w1,center2,w2):
    res = dict()
    accumulate_dict(res,center1,w1)
    accumulate_dict(res,center2,w2)
    normalize_norm2(res,w1+w2)
    return res

def update_center_with_center_weight_pair(center_weights):
    res = dict()
    leng = 0.0
    for pair in center_weights:
        (center,weight) = pair
        accumulate_dict(res,center,weight)
        leng += weight
    normalize_norm2(res,leng)
    return res

def normalize_norm2(vecto,length):
    for key in vecto:
        vecto[key] = np.sqrt(vecto[key]/float(length))

def accumulate_dict(res,center,weight):
    for key in center:
        if key not in res:
            res[key] = weight*center[key]*center[key]
        else:
            res[key] += weight*center[key]*center[key]
            
def connect_events(t_centers,t_sizes,e_center,e_size,logger=None):
    """
    this function is to determine whether this event is a member of a trend or not
    return id of trend
    """
    if (len(t_centers) == 0):# when there are no trends, event --> trend
        return (None,None)
    scores = dict()
    for trend_id in t_centers:
        center = t_centers[trend_id]
        scores[trend_id] = util.get_similarity(center,e_center)
    max_key = util.get_max_key_dic(scores)
    if (logger):
        #logger.info('max closeness (trend = %d): %f'%(max_key,scores[max_key]))
        if (len(scores) > 3):
            tops = util.get_top_pair_from_dic(scores,3)
            logger.info('top 3 max: %s'%str(tops))
    if(scores[max_key] > resources.EVENT_TREND_ASSEMBLY):
        return (max_key,scores[max_key])
    return (None,None)

def compute_last_size_score(x):# x is the last_size
    return 1.0/(1+np.exp(-0.0237*x + 2.434))

def get_gap_date_score(gap_date):
    return float(resources.DAY_TREND_THRESHOLD - gap_date + 1)/resources.DAY_TREND_THRESHOLD

def predict_connection(context,entity,closeness):
    final_score = context * CONTEXT_RATIO + entity * ENTITY_RATIO + CLOSENESS_RATIO*closeness
    return final_score

def get_score_of_event_to_trend(t_context,t_entity,trend_last_date,trend_last_size,e_context,e_entity,event_date):
    context_score = util.get_similarity(t_context,e_context)
    entity_score = util.get_similarity(t_entity,e_entity)
    gap_date = util.days_between(event_date-trend_last_date)
    gap_date_score = get_gap_date_score(gap_date)
    last_size_score = compute_last_size_score(trend_last_size)
    closeness = gap_date_score*last_size_score
    final_score = predict_connection(context_score,entity_score,closeness)
    return final_score

def get_best_trend(trends,e_context,e_entity,e_date,e_id,e_event_size):
    detail = dict()
    temp_score = -1
    max_key = -1
    for trend in trends:
        trend_id = trend[resources.TREND_ID]
        t_context = trend[resources.TREND_CENTER]
        t_entity = trend[resources.TREND_ENTITY_CENTER]
        trend_last_date = trend[resources.TREND_LAST_DATE]
        trend_last_size = trend[TREND_LAST_SIZE]
        #calculate all scores
        context_score = util.get_similarity(t_context,e_context)
        entity_score = util.get_similarity(t_entity,e_entity)
        gap_date = util.days_between(trend_last_date,e_date)
        gap_date_score = get_gap_date_score(gap_date)
        last_size_score = compute_last_size_score(trend_last_size)
        closeness = gap_date_score*last_size_score
        final_score = predict_connection(context_score,entity_score,closeness)
        if (final_score > temp_score):
            temp_score = final_score
            max_key = trend_id
            #save connection infor
            #print 'trend_catid  %d'%trend[resources.TREND_CATID]
            detail[event_trend_dao.CATID] = trend[resources.TREND_CATID]
            detail[event_trend_dao.CONTEXT_SIM] = context_score
            detail[event_trend_dao.ENTIRY_SIM] = entity_score
            detail[event_trend_dao.EVENT_ID] = e_id
            detail[event_trend_dao.EVENT_SIZE] = e_event_size
            detail[event_trend_dao.FINAL_SCORE] = final_score
            detail[event_trend_dao.GAP_DATE] = gap_date
            detail[event_trend_dao.LAST_SIZE] = trend_last_size
            detail[event_trend_dao.TREND_ID] = trend_id
            detail[event_trend_dao.CLOSENESS] = closeness
    #store all scores in detail vecto to return 
    return (temp_score,max_key,detail)

def connect_trend_with_score(score,connect_threshold = CONNECT_THRESHOLD):
    if (score >= connect_threshold):
        return True
    return False

def update_trend_in_real_time(trends,events,update_dict,new_trend_events,event_date,trend_table= resources.TREND_TABLE,event_table=resources.EVENT_TABLE,visualize = True):
    logger.info('start connecting process')
    #update trend
    event_trend_pair =[] #infor about trend_id for each event_id
    event_context_centers = get_event_field_dict(events,resources.EVENT_CENTER)
    event_entity_centers = get_event_field_dict(events,resources.EVENT_ENTITY_CENTER)
    event_sizes = get_event_field_dict(events,resources.EVENT_SIZE)
    for trend in trends:
        trend_id = trend[resources.TREND_ID]
        if (len(update_dict[trend_id]) > 0):
            logger.info('update trend: %d '%trend_id)
            new_event_centers =[]
            new_event_sizes = []
            new_event_entity = []
            event_ids = update_dict[trend_id]
            for i in range(len(event_ids)):
                eid = event_ids[i]
                event_trend_pair.append((eid,trend_id))
                new_event_centers.append(event_context_centers[eid])
                new_event_sizes.append(event_sizes[eid])
                new_event_entity.append(event_entity_centers[eid])
                #event_trend_records.append((eid,key,max_scores[key][i]))
            #update context trend_center
            t_center_context = trend[resources.TREND_CENTER]
            t_size = trend[resources.TREND_SIZE]
            new_trend_context_center = update_trend_center(t_center_context,t_size,new_event_centers,new_event_sizes)
            trend_util.save_trend_center_context(new_trend_context_center,trend_id,event_date)
            #update entity trend_center
            t_center_entity = trend[resources.TREND_ENTITY_CENTER]
            new_trend_entity_center = update_trend_center(t_center_entity,t_size,new_event_entity,new_event_sizes)
            trend_util.save_trend_center_entity(new_trend_entity_center,trend_id,event_date)
            #update last_size trend
            trend_last_size = sum(new_event_sizes)
            trend_util.save_trend_last_size(trend_id,event_date,trend_last_size)
            #update trend in database
            update_trend_with_events(trend_id,event_ids,trend_last_size,event_date,trend_table)

    for event in new_trend_events:
        pair = process_new_trend_from_event(event)
        event_trend_pair.append(pair)
    if (visualize):
        logger.info('visualize all trends updated or inserted on day: %s'%event_date)
        #trend_visualize.visualize_trend_on_day(event_date)
    event_dao.update_trend_id_for_events(event_trend_pair)

def update_trend_with_new_events(trends,events,update_dict,event_date,trend_table = resources.TREND_TABLE):
    """
    update_dict map trend_id --> [e1,e2, ...e_k]: events belonging to trend_id
    """
    event_trend_pair = []
    event_context_centers = get_event_field_dict(events,resources.EVENT_CENTER)
    event_entity_centers = get_event_field_dict(events,resources.EVENT_ENTITY_CENTER)
    event_sizes = get_event_field_dict(events,resources.EVENT_SIZE)
    for trend in trends:
        trend_id = trend[resources.TREND_ID]
        if (len(update_dict[trend_id]) > 0):
            logger.info('update trend: %d '%trend_id)
            new_event_centers =[]
            new_event_sizes = []
            new_event_entity = []
            event_ids = update_dict[trend_id]
            for i in range(len(event_ids)):
                eid = event_ids[i]
                event_trend_pair.append((eid,trend_id))
                new_event_centers.append(event_context_centers[eid])
                new_event_sizes.append(event_sizes[eid])
                new_event_entity.append(event_entity_centers[eid])
                #event_trend_records.append((eid,key,max_scores[key][i]))
            #update context trend_center
            t_center_context = trend[resources.TREND_CENTER]
            t_size = trend[resources.TREND_SIZE]
            new_trend_context_center = update_trend_center(t_center_context,t_size,new_event_centers,new_event_sizes)
            trend_util.save_trend_center_context(new_trend_context_center,trend_id,event_date)
            #update entity trend_center
            t_center_entity = trend[resources.TREND_ENTITY_CENTER]
            new_trend_entity_center = update_trend_center(t_center_entity,t_size,new_event_entity,new_event_sizes)
            trend_util.save_trend_center_entity(new_trend_entity_center,trend_id,event_date)
            #update last_size trend
            trend_last_size = sum(new_event_sizes)
            trend_util.save_trend_last_size(trend_id,event_date,trend_last_size)
            #update trend in database
            update_trend_with_events(trend_id,event_ids,trend_last_size,event_date,trend_table)
    event_dao.update_trend_id_for_events(event_trend_pair)
    
def update_trend_table_from_events(new_trend_events,event_date):
    event_trend_pair = []
    for event in new_trend_events:
        pair = process_new_trend_from_event(event)
        event_trend_pair.append(pair)
    event_dao.update_trend_id_for_events(event_trend_pair)

def connect_events_to_trends_on_date2(event_date,visualize=False,trend_table= resources.TREND_TABLE,event_table=resources.EVENT_TABLE):
    events = get_events_for_connecting(event_date,event_table)
    trends = get_trends_for_connecting(event_date,resources.DAY_TREND_THRESHOLD,trend_table)
    update_dict = dict()
    connect_infor = []
    new_trend_events = []
    for trend in trends:
        trend_id = trend[resources.TREND_ID]
        update_dict[trend_id] = []
    logger.info('start connecting process')
    for event in events:
        event_id = event[resources.EVENT_ID]
        e_context = event[resources.EVENT_CENTER]
        e_entity = event[resources.EVENT_ENTITY_CENTER]
        e_date = str(event[resources.EVENT_DATE])
        e_event_size = event[resources.EVENT_SIZE]
        (score,trend_id,detail) = get_best_trend(trends,e_context,e_entity,e_date,event_id,e_event_size)
        logger.info('event_id: %d, trend_id: %d, final_score = %f'%(event_id,trend_id,score))
        if connect_trend_with_score(score):
            update_dict[trend_id].append(event_id)
            connect_infor.append(detail)
        else:#new trend
            new_trend_events.append(event)
    logger.info('update trends having events being connected')
    update_trend_with_new_events(trends,events,update_dict,event_date,trend_table)
    logger.info('update new trends <--- new events')
    update_trend_table_from_events(new_trend_events,event_date)
    if (visualize):
        logger.info('visualize all trends updated or inserted on day: %s'%event_date)
        #trend_visualize.visualize_trend_on_day(event_date)

def connect_events_to_trends_on_date(event_date,visualize=False,trend_table= resources.TREND_TABLE,event_table=resources.EVENT_TABLE):
    events = get_events_for_connecting(event_date,event_table) # read all events on date to connect to available trends 
    trends = get_trends_for_connecting(event_date,resources.DAY_TREND_THRESHOLD,trend_table)# read candidate trends for connecting 
    update_dict = dict()
    connect_infor = []
    new_trend_events = []
    for trend in trends:
        trend_id = trend[resources.TREND_ID]
        update_dict[trend_id] = []
    logger.info('start connecting process')
    for event in events:
        event_id = event[resources.EVENT_ID]
        e_context = event[resources.EVENT_CENTER]
        e_entity = event[resources.EVENT_ENTITY_CENTER]
        e_date = str(event[resources.EVENT_DATE])
        e_event_size = event[resources.EVENT_SIZE]
        (score,trend_id,detail) = get_best_trend(trends,e_context,e_entity,e_date,event_id,e_event_size)# get the mót possible trend to connect to event
        logger.info('event_id: %d, trend_id: %d, final_score = %f'%(event_id,trend_id,score))
        if connect_trend_with_score(score):
            update_dict[trend_id].append(event_id)
            connect_infor.append(detail)
        else:#new trend
            new_trend_events.append(event)
    #save connection infor
    logger.info('save connection information: %d (event,trend)'%len(connect_infor))
    event_trend_dao.insert_records(connect_infor)
    #update trend
    event_trend_pair =[] #infor about trend_id for each event_id
    event_context_centers = get_event_field_dict(events,resources.EVENT_CENTER)
    event_entity_centers = get_event_field_dict(events,resources.EVENT_ENTITY_CENTER)
    event_sizes = get_event_field_dict(events,resources.EVENT_SIZE)
    for trend in trends:
        trend_id = trend[resources.TREND_ID]
        if (len(update_dict[trend_id]) > 0):
            logger.info('update trend: %d '%trend_id)
            new_event_centers =[]
            new_event_sizes = []
            new_event_entity = []
            event_ids = update_dict[trend_id]
            for i in range(len(event_ids)):
                eid = event_ids[i]
                event_trend_pair.append((eid,trend_id))
                new_event_centers.append(event_context_centers[eid])
                new_event_sizes.append(event_sizes[eid])
                new_event_entity.append(event_entity_centers[eid])
                #event_trend_records.append((eid,key,max_scores[key][i]))
            #update context trend_center
            t_center_context = trend[resources.TREND_CENTER]
            t_size = trend[resources.TREND_SIZE]
            new_trend_context_center = update_trend_center(t_center_context,t_size,new_event_centers,new_event_sizes)
            trend_util.save_trend_center_context(new_trend_context_center,trend_id,event_date)
            #update entity trend_center
            t_center_entity = trend[resources.TREND_ENTITY_CENTER]
            new_trend_entity_center = update_trend_center(t_center_entity,t_size,new_event_entity,new_event_sizes)
            trend_util.save_trend_center_entity(new_trend_entity_center,trend_id,event_date)
            #update last_size trend
            trend_last_size = sum(new_event_sizes)
            trend_util.save_trend_last_size(trend_id,event_date,trend_last_size)
            #update trend in database
            update_trend_with_events(trend_id,event_ids,trend_last_size,event_date,trend_table)

    for event in new_trend_events:
        pair = process_new_trend_from_event(event)
        event_trend_pair.append(pair)
    if (visualize):
        logger.info('visualize all trends updated or inserted on day: %s'%event_date)
        #trend_visualize.visualize_trend_on_day(event_date)
    event_dao.update_trend_id_for_events(event_trend_pair)

def process_new_trend_from_event(event,trend_table=resources.TREND_TABLE):
    trend = dict()
    trend[resources.TREND_START_DATE] = event[resources.EVENT_DATE]
    trend[resources.TREND_LAST_DATE] = event[resources.EVENT_DATE]
    trend[resources.TREND_CATID] = event[resources.EVENT_CATID]
    trend[resources.TREND_EVENT_IDS] ='%d,'%event[resources.EVENT_ID]
    trend[resources.TREND_SIZE] = event[resources.EVENT_SIZE]
    trend[resources.TREND_EVENT_NUM] = 1
    trend_id = event_dao.insert_new_trend(trend,trend_table)
    trend_folder = trend_util.get_trend_folder(trend_id)
    util.create_folder(trend_folder)
    #save trend_center and last_size
    context_folder = trend_util.get_trend_context_center_folder(trend_id)
    util.create_folder(context_folder)
    entity_folder = trend_util.get_trend_entity_center_folder(trend_id)
    util.create_folder(entity_folder)
    trend_util.save_trend_center_context(event[resources.EVENT_CENTER],trend_id,trend[resources.TREND_LAST_DATE])
    trend_util.save_trend_center_entity(event[resources.EVENT_ENTITY_CENTER],trend_id,trend[resources.TREND_LAST_DATE])
    trend_util.save_trend_last_size(trend_id,trend[resources.TREND_LAST_DATE],trend[resources.TREND_SIZE])
    logger.info('new trend with id: %d from event: %d'%(trend_id,event[resources.EVENT_ID]))
    return (event[resources.EVENT_ID],trend_id)
    
def update_trend_with_events(trend_id,event_ids,trend_last_size,last_date,trend_table=resources.TREND_TABLE):
    """
    As a new event belongs to a trend_id, update the event_list,center,last update
    """
    #fetch information of trend with id trend_id
    trend = event_dao.fetch_trend_by_id(trend_id)
    #update event list
    trend_event_list = trend[resources.TREND_EVENT_IDS] 
    for event_id in event_ids:
        trend_event_list += '%d,'%event_id
    trend[resources.TREND_EVENT_IDS] = trend_event_list
    #update last day in trend
    trend[resources.TREND_LAST_DATE] = last_date
    trend[resources.TREND_SIZE] += trend_last_size
    trend[resources.TREND_EVENT_NUM] += len(event_ids)
    if (resources.INSERT_EVENT_DB):
        event_dao.update_trend(trend,trend_table)

def get_event_field_dict(events,field):
    res = dict()
    for event in events:
        event_id = event[resources.EVENT_ID]
        res[event_id] = event[field]
    return res

def get_trends_for_connecting(date,day_trend_threshold = resources.DAY_TREND_THRESHOLD,trend_table = resources.TREND_TABLE):
    trends = event_dao.fetch_all_trends(date,resources.DAY_TREND_THRESHOLD,trend_table)
    for trend in trends:
        trend_id = trend[resources.TREND_ID]
        trend_last_date = trend[resources.TREND_LAST_DATE]
        trend[resources.TREND_CENTER] = trend_util.get_trend_center_context(trend_id,trend_last_date)
        trend[resources.TREND_ENTITY_CENTER] = trend_util.get_trend_center_entity(trend_id,trend_last_date)
        trend[TREND_LAST_SIZE] = trend_util.get_trend_last_size(trend_id,trend_last_date)
    return trends
def get_events_for_connecting(date,event_table=resources.EVENT_TABLE):
    events = event_dao.fetch_all_events(date,event_table)
    (centers,sizes) = trend_util.get_all_event_centers_context(date)
    (en_centers,en_sizes) = trend_util.get_all_event_centers_entity(date)
    for event in events:
        event_id = event[resources.EVENT_ID]
        event[resources.EVENT_CENTER] = centers[event_id]
        event[resources.EVENT_ENTITY_CENTER] = en_centers[event_id]
    return events
    
def filter_trend_with_multiple_events_update(new_trend,update_dict,max_scores):
    for key in update_dict:
        if (len(update_dict[key]) > 1):
            scores = max_scores[key]
            events = update_dict[key]
            logger.info("two events are about to aggragated into trend: %d"%key)
            logger.info(scores)
            logger.info(events)
            
            event_max_index = util.get_argmax_list_index(scores)
            for i in range(len(events)):
                if (i != event_max_index):
                    new_trend.append(events[i])
                else:
                    update_dict[key] =[events[event_max_index]]

def get_trees(day):
    tree_path = trend_util.get_tree_path(day)
    if not os.path.exists(tree_path):
        return None
    nodes = tree.get_nodes_from_file(tree_path)
    return nodes

def compute_trend(day,visualize):
    #util.set_log_file(logger,resources.LOG_FOLDER+'/'+day+'.log')
    logger.info('fetch all events on day: %s'%day)
    connect_events_to_trends_on_date2(day,visualize)

def remove_records_start_by_date(date):
    trends = event_dao.get_trend_update_after_date(date)
    del_list= []
    for trend in trends:
        start_date = str(trend[resources.TREND_START_DATE])
        comp = util.compare_dates(start_date,date)
        if (comp == util.EQUAL or comp == util.GREATER):
            del_list.append(trend[resources.TREND_ID])
        else:
            #TO DO recover pre-state
            event_ids = util.str_to_int_array(trend[resources.TREND_EVENT_IDS])
            
def main():
    if (len(sys.argv) != 4):
        print 'usage: python detec_trend.py date1/next date2/next visualize'
        sys.exit(1)
    start_day = sys.argv[1]
    visualize = False
    if (sys.argv[3] == 'True'):
        visualize = True
    elif (sys.argv[3] == 'False'):
        visualize = False
    else:
        print 'option visualize is only True/False'
        sys.exit(1)
    if (start_day == 'next'):
        max_date = event_dao.get_max_last_date(resources.TREND_TABLE)
        max_date = util.get_ahead_day(max_date,1)
        print '================================================detecting trend on %s============================='%max_date
        compute_trend(max_date,visualize)
    else:
        last_date = sys.argv[2]
        temp_date = start_day
        while(True):
            if (temp_date == last_date):
                compute_trend(temp_date,visualize) 
                break
            else:
                compute_trend(temp_date,False)
            temp_date = util.get_ahead_day(temp_date,1)
    print 'finished task ...'
if (__name__ == '__main__'):
    main()
#compute_trend('2015-06-03')
#trend_visualize.visualize_trend_on_day('2015-10-27')
#connect_events_to_trends_on_date2('2016-02-01',False)
    