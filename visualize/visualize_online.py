import sys,pymysql
sys.path.insert(0,'..')
import real_time_resources2,resources,dao,util
def get_events():
    query = 'select * from %s order by %s desc'%(real_time_resources2.TABLE_NAME,resources.EVENT_SIZE)
    conn = dao.get_connection()
    cur = conn.cursor(pymysql.cursors.DictCursor)
    cur.execute(query)
    rows = cur.fetchall()
    dao.free_connection(conn,cur)
    return rows
def save_event_file(save_path = real_time_resources2.HTML_REPORT_PATH2):
    events = get_events()
    cluster_ids = []
    aid_urls = []
    article_nums = []
    coherences = []
    for event in events:
        art_str = event[resources.EVENT_ARTICCLE_IDS]
        aids = util.get_full_article_ids(art_str)
        cluster_ids.append(event[resources.EVENT_ID])
        aid_urls.append(util.list_to_str(aids))
        article_nums.append(event[resources.EVENT_SIZE])
        coherences.append(event[resources.EVENT_COHERENCE])
    
    gen_html_report(coherences,cluster_ids,aid_urls,article_nums,save_path)

def gen_html_report(coherences,cluster_ids,aid_urls,article_nums,save_path):
    event_imgs = []
    for i in range(len(cluster_ids)):
        key = cluster_ids[i]
        img_path = real_time_resources2.IMG_FOLDER+ '/top_%d.png'%key
        event_imgs.append(img_path)
    util.gen_html_event2(coherences,cluster_ids,event_imgs, save_path, article_nums, aid_urls)

def main():
    save_event_file()
    print 'finished'
if __name__ == '__main__':
    main()
    