# /a/b/c/d/e
HOST = '127.0.0.1'
PORT='5000'
DATA_FOLDER= 'tamnt/trend_py/data'#os.path.dirname(os.path.abspath(__file__))+'/'#get folder containing resources.py
#print DATA_FOLDER
IMG_DATA_FOLDER = 'tamnt/trend_py/data/img_folder'
CAT_TREE_FILE = DATA_FOLDER+'/deploy_resources/cat_tree.txt'
TEMP_CENTER_FOLDER=DATA_FOLDER+'/temp_data/centers'
CATID_FOLDER = DATA_FOLDER+'/temp_data/catids'
DAY_DATA_FOLDER = DATA_FOLDER+'/temp_data/day_data'
#database information 
DB_CONFIG_PATH = 'deploy_resources/acc.txt'
# resources for det
DOMAIN_PATH = 'deploy_resources/selected_domain.csv'
SMALL_DOMAIN_PATH = 'deploy_resources/small_domain.csv'
BLACK_DOMAIN_PATH = 'deploy_resources/black_domains.txt'
DICTIONARY_FILE_NAME = 'dictionary.txt'
DICTIONARY_FILE_TEMP_NAME = 'temp_dictionary.txt'
LOG_FOLDER = DATA_FOLDER+'/logs'
GENERAL_LOG = DATA_FOLDER+'/logs/general.log'
TREND_LOG = DATA_FOLDER+ '/logs/trend.log'
CORPUS_SIZE_FILE_NAME = 'corpus_size.txt'
STOP_WORD_PATH='deploy_resources/stoplist.txt'
DAYS_PATH = DATA_FOLDER+'/temp_data/days'
STAT_FOLDER = DATA_FOLDER+'/temp_data/stat'
DOC_STAT_FILE_NAME='doc_stat.csv'
TEMPARORY_DATA = DATA_FOLDER+'/temp_data/data'
TEXT_FOLDER = DATA_FOLDER+'/temp_data/text'
IMG_FOLDER = IMG_DATA_FOLDER+'/imgs'
TREND_FOLDER=IMG_DATA_FOLDER+'/trend5'
INSTANT_UPDATE = True#update new cluster immediately
DAY_THRESHOLDS=[0.7,0.6,0.5,0.45,0.4]# the hierarchical thresh used in hierarchical clustering
SEPARATE_THRESHOLDS=[0.9,0.8,0.7,0.6,0.5]
REMOVE_EVENT_THRESHOLD = 5# events with number of articles less than this is removed
REMOVE_EVENT_SEPARATE = 3#events with number of carticles less than this is removed, in separate 
REMOVE_AT_INSERT_THRESHOLD = 0.35#remove when calculate the distance of each article to center 
HIERARCHICAL_THRESHOLD = 0.4 #threshold at the merging step
HIERARCHICAL_THRESHOLD_SEPARATE = 0.45
CHANGE_THRESHOLD  = 5# at the same threshold, if the number of clusters less than this, --> change threhold
COHERENCE_THRESHOLD = 0.50
# resources for dictionary
DF_THRESHOLD = 7# words with df less than this would be removed
DF_THRESHOLD_STREAM = 4# dictionary in each stream, day is removed
STOPWORD_RATIO = 0.3# words appearing more than this ratio in documents would be removed
DAY_TREND_THRESHOLD = 100# only get trend within 7 days from past to connect event
UPDATE_RATE = 2.0# the rate indicates the learning rate, updating based on new data, the evolution of a topic
EVENT_TREND_ASSEMBLY = 0.50# aggregate event to trend
VISUAL_THRESHOLD_TREND = 1#trned with number of event greater than this threshold is valid
EVENT_CENTER_LENGTH_THRESHOLD = 50 # event with too short top words in center should be removed
IDENTICAL_DOC_THRESHOLD = 0.9
# event table fields
INSERT_EVENT_DB = False
USING_CONNECT_DETECT_EVENT = True
EVENT_CURRENT_TABLE = 'real_event_test2'
EVENT_TABLE='event_test'
EVENT_ID = 'id'
EVENT_DATE =	'date'
EVENT_ARTICCLE_IDS = 'article_ids'
EVENT_CENTER = 'center'
EVENT_SIZE = 'size'
EVENT_TOPWORD_IMG='img'
EVENT_COHERENCE = 'coherence'
EVENT_CATID = 'catid'
EVENT_TREND_ID = 'trend_id'
EVENT_TAG = 'tag'
EVENT_ENTITY_CENTER = 'en_center'
EVENT_IMG2 = 'img2'
# trend table fields
TREND_TABLE='trend_test'
TREND_ID = 'id'
TREND_START_DATE = 'start_date'
TREND_LAST_DATE = 'last_date'
TREND_EVENT_IDS = 'event_ids'
TREND_CENTER = 'center'
TREND_SIZE='size'
TREND_EVENT_NUM='event_num'
TREND_IMG = 'img'
TREND_CATID = 'catid'
TREND_TAG = 'tag'
TREND_ENTITY_CENTER = 'en_center'
# message table
FEEDBACK_MESSAGE = 'trend_message'
FEEDBACK_ID = 'id'
FEEDBACK_DATE = 'date'
FEEDBACK_TABLE='trend_massage'
FEEDBACK_EVENT_ID = 'event_id'
FEEDBACK_TREND_ID = 'trend_id'
#resources for visualize word cloud
TOP_WIDTH=400#top 10 images
TOP_HEIGHT=400#top 10 images
TOP_TREND_WIDTH = 400#top 10 for trend
TOP_TREND_HEIGHT = 400
TOP_BACKGROUND_COLOR ='white'
TOP_RELATIVE_SCALE = '0.5'
TOP_IMG_EXTENSION='png'

#visualization
TOP_WORD_EVENT = 40#top words for event
TOP_WORD_TREND = 60#top words for trend
