import sys,hashtag
sys.path.insert(0,'..')
import resource,util
LOG_FILE = resources.LOG_FOLDER+'/tag.log'
logger = logging.getLogger("hash-tag")
logger.setLevel(logging.INFO)
util.set_log_file(logger,LOG_FILE)
def gen_hashtag_for(event_id,table = resources.EVENT_CURRENT_TABLE):
    event = event_dao.get_event_by_id(event_id,table)
    arts_str = event[resources.EVENT_ARTICCLE_IDS]
    aids = util.get_full_article_ids(arts_str)
    result = hashtag.gen_hashtag_from_articles(aids)
    if (len(result) > 3):
        return result
    logger.info('event %d does not has enough hashtag from articles'%event_id)
    date = event[resources.EVENT_DATE]
    top_text_path = trend_util.get_text_top_path(event_id,date)
    result = gen_hashtag_from_top_file(top_text_path)
    return result

def fill_hashtag_for_events(event_ids):
    hashtag.ge
def main():
    
    