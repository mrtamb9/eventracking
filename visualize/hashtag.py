# -*- coding: utf-8 -*-
import sys,codecs,os
CONTAIN_FOLDER  = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0,CONTAIN_FOLDER+'/..')
MAX_WORD_LIMIT = 16
MAX_TAG_LIMIT = 10
import resources,event_dao,dao,util,trend_util,logging
import HTMLParser
import unicodedata
html_resolver = HTMLParser.HTMLParser()

LOG_FILE = resources.LOG_FOLDER+'/tag.log'
logger = logging.getLogger("hash-tag")
logger.setLevel(logging.INFO)
util.set_log_file(logger,LOG_FILE)
def gen_hashtag_from_articles(aids,max_tag_limit=MAX_TAG_LIMIT):
    articles = event_dao.get_article_urls(aids)
    return gen_hashtag_from_article_records(articles,max_tag_limit)

def gen_hashtag_from_article_records(articles,max_tag_limit = MAX_TAG_LIMIT):
    hash_tag_dict = dict()
    for art in articles:
        tags = art[dao.TAGS]
        if (tags):
            tags = normalize_tag_str(tags)
            words = get_tokenize_tag(tags)
            for word in words:
                if word not in hash_tag_dict:
                    hash_tag_dict[word] = 1
                else:
                    hash_tag_dict[word] += 1
    sort_words = util.sort_dic_des(hash_tag_dict)
    result =''
    word_num = max_tag_limit
    leng = len(sort_words)
    if (leng < 3):
        return ''
    if (leng < max_tag_limit ):
        word_num = leng
    for i in range(word_num-1):
        (v,k) = sort_words[i]
        result += '%s, '%k
    (v,k) = sort_words[word_num-1]
    result += '%s'%k
    return result

def get_tokenize_tag(tag):
    tg = tag.replace('&nbsp','')
    words = tg.split(',')
    res = []
    for word in words:
        temp = word.strip()
        if (len(temp) > 1):
            res.append(temp)
    return res
def normalize_tag_str(s):
    decode_html = html_resolver.unescape(s)
    decode_html = html_resolver.unescape(decode_html)
    norm_str = unicodedata.normalize('NFC', decode_html)
    return norm_str
def gen_hashtag_from_top_file(top_path,max_size=MAX_WORD_LIMIT):
    result = ''
    f = codecs.open(top_path,'r',encoding='utf8')
    lines = f.readlines()
    f.close()
    word_num = max_size
    if (len(lines) < word_num):
        word_num = len(lines)
    for i in range(word_num-1):
        temp_str = lines[i].strip()
        temps = temp_str.split(':')
        result += '%s,'%temps[0]
    temp_str = lines[word_num - 1].strip()
    temps = temp_str.split(':')
    result += '%s'%temps[0]
    return result

def gen_hashtag_for(event_id,table = resources.EVENT_TABLE):
    event = event_dao.get_event_by_id(event_id,table)
    arts_str = event[resources.EVENT_ARTICCLE_IDS]
    aids = util.get_full_article_ids(arts_str)
    result = gen_hashtag_from_articles(aids)
    if (len(result) > 3):
        result = result.replace('\'','\\\'')
        return result
    logger.info('event %d does not has enough hashtag from articles'%event_id)
    date = str(event[resources.EVENT_DATE])
    top_text_path = trend_util.get_text_top_path(event_id,date)
    result = gen_hashtag_from_top_file(top_text_path)
    result = result.replace('\'','\\\'')
    return result

def fill_hashtag_to_events_on_date(date,table=resources.EVENT_TABLE):
    logger.info('update hashtags for events on %s'%date)
    event_ids = event_dao.get_all_event_id_on_day(date,table)
    total_count = len(event_ids)
    dic_tag = dict()
    for i in range(total_count):
        event_id = event_ids[i]
        print 'calculate hashtags for event: %d'%event_id
        dic_tag[event_id] = gen_hashtag_for(event_id,table)
    logger.info('update hashtags for %d events'%total_count)
    event_dao.update_hash_tag_for_events(dic_tag,table)
def fill_hashtag_to_events_on_range(date1,date2,table = resources.EVENT_TABLE):
    logger.info('inserting hashtags to table event')
    date = date1
    while(True):
        fill_hashtag_to_events_on_date(date,table)
        if (date == date2):
            break
        date = util.get_ahead_day(date,1)
    logger.info('finish inserting hashtags to events from %s to %s'%(date1,date2))

def get_hashtag_for_event(aids,top_path,max_size=MAX_TAG_LIMIT):
    result = gen_hashtag_from_articles(aids,max_size)
    if (len(result) > 3):
        result = result.replace('\'','\\\'')
        return result
    result = gen_hashtag_from_top_file(top_path,max_size)
    result = result.replace('\'','\\\'')
    return result

def main():
    if (len(sys.argv) != 3):
        print 'usage: python hashtag.py date1/next date2/next'
        sys.exit(1)
    date1 = sys.argv[1]
    date2 = sys.argv[2]
    if (date1 == 'next'):
        date = event_dao.get_max_date(resources.EVENT_TABLE)
        fill_hashtag_to_events_on_range(date,date)
    else:
        fill_hashtag_to_events_on_range(date1,date2)
if (__name__ == '__main__'):
    main()