import resources,util
ONLINE_FOLDER = resources.DATA_FOLDER + '/online'
CURRENT_FOLDER = ONLINE_FOLDER + '/current'
TEXT_FOLDER = ONLINE_FOLDER + '/text'
IMG_FOLDER = ONLINE_FOLDER + '/imgs'
STAT_FOLDER= ONLINE_FOLDER + '/stat'
CATID_FOLDER = ONLINE_FOLDER + '/catid'
DAY_DATA_FOLDER = ONLINE_FOLDER + '/day_data'
CENTER_FOLDER = ONLINE_FOLDER+'/centers'
CACHE_FOLDER = ONLINE_FOLDER + '/cache'
MERGE_THRESHOLD = 0.7
REMOVE_BEFORE_MERGE = 4
REMOVE_EVENT_THRESHOLD = 5 #event having number of articles less than this thresh is removed
OUTPUT_THRESHOLDS = [0.7,0.6,0.5,0.45,0.4]
WINDOW_SIZE = 10 # in real-time event detection, we only get previous trend within a window size
EVENT_DISPLAY_NUM = 2
LOG_PATH = resources.DATA_FOLDER + '/logs/online.log'
ATTACHED_TREND='trend'
HOUR_SERIES = 'hours'
util.create_folder(CACHE_FOLDER)