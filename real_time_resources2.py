import resources
K_SHINGLE = 3
MIN_BATCH_SIZE = 10
LOG_PATH = resources.LOG_FOLDER + '/new_real2.log'
READ_DATA_LOG_PATH = resources.LOG_FOLDER + '/real_data.log'
REAL_TIME_UTIL_LOG_PATH = resources.LOG_FOLDER+'/real_time_util.log'
RELATED_CLUSTER_THRESHOLD = 0.20# threshold for extracting related clusters to new articles in real_time event
CONNECT_RELATED_THRESHOLD = 0.65
THRESHOLD_LEVELS = [0.75,0.7,0.6,0.55,0.5]
HIERARCHICAL_SEPARATE_THRESHOLD = 0.45
HIERARCHICAL_THRESHOLD = 0.38
SAVE_DB_THRESHOLD = 8# if size of cluster >= this threshold --> it is an event, we need to save 
HIERARCHICAL_REMOVE_THRESHOLD = 4
SAVE_FOLDER = resources.DATA_FOLDER+'/temp_data/real3'
IMG_FOLDER = resources.IMG_DATA_FOLDER+'/temp_data/real3/imgs'
TEXT_FOLDER =  SAVE_FOLDER+'/texts'
COHERENCE_PATH = SAVE_FOLDER+'/coherences.txt'
EVENT_ORDER_PATH = SAVE_FOLDER+'/event_order.dat'
HTML_REPORT_PATH = IMG_FOLDER+'/report.html'
HTML_REPORT_PATH2 = IMG_FOLDER + '/report2.html'
SUB_TREND_FOLDER = SAVE_FOLDER + '/temp_sub_trend'
SAVE_TO_DB = True# set this = True to insert event to database
RECALCULATE_TREND_ID = 6
BATCH_LOG_FOLDER = resources.LOG_FOLDER + '/batch_log'
print 'batch_log folder: %s'%resources.LOG_FOLDER