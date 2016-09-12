import sys
import event_dao,resources,util,trend_util
import cluster_doc2,top_word_img
import matplotlib.pyplot as plt
import matplotlib.image as mpimg
import matplotlib.patches as patches

def visualize_trend(trend_id,trend_folder=None,trend_table = resources.TREND_TABLE,event_table = resources.EVENT_TABLE):
    print 'trend_id = %d'%trend_id
    print 'start visualize trend ....'
    trend = event_dao.fetch_trend_by_id(trend_id,trend_table)
    #event_list_str = trend[resources.TREND_EVENT_IDS]
    #event_list = util.str_to_int_array(event_list_str,',')
    last_date = str(trend[resources.TREND_LAST_DATE])
    trend_center = trend_util.get_trend_center_context(trend_id,last_date)
    last_date = str(trend[resources.TREND_LAST_DATE])
    dictionary_path = trend_util.get_dictionary_path(last_date)
    vocab_map = util.get_dictionary_map(dictionary_path)
    if (not trend_folder):
        trend_folder = trend_util.get_trend_folder(trend_id)
    util.create_folder(trend_folder)
    trend_text_path = trend_folder+'/top.txt'
    cluster_doc2.visualize_centre(vocab_map,trend_text_path,trend_center,resources.TOP_WORD_TREND)
    top_word_img.generate_img(trend_text_path,trend_folder+'/top.'+resources.TOP_IMG_EXTENSION,\
    width=resources.TOP_TREND_WIDTH,height=resources.TOP_TREND_HEIGHT)
    #visualize_trend_day_by_day(trend_id,event_list)

def visualize_trend_on_day(day):
    print 'starting visualizing trends on %s'%day
    trend_ids = event_dao.get_trend_ids_on_day(day)
    for trend_id in trend_ids:
        print 'visualize trend: %d'%trend_id
        visualize_trend(trend_id)
    print 'finish visualizing processes'

def visualize_trend_day_by_day(trend_id,event_list):
    sizes = []
    dates = []
    img_paths = []
    size_labels = []
    for i in range(len(event_list)):
        print '============================handle event: %d'%event_list[i]
        event_id = event_list[i]
        event = event_dao.get_event_by_id(event_id)
        date = str(event[resources.EVENT_DATE])
        img_path = trend_util.get_img_path(event_id,date)
        img_paths.append(img_path)
        size = event[resources.EVENT_SIZE]
        sizes.append(size)
        dates.append('%s\n%d'%(date,event_id))
        size_labels.append(str(size))
    save_path = trend_util.get_trend_img_path(trend_id)
    visualzie_series(img_paths,dates,sizes,size_labels,save_path)

def visualzie_series(img_paths,img_titles,scores,score_titles,save_path):
    max_score = max(scores)
    event_num = len(img_paths)
    width = 15*event_num
    if (event_num > 17):
        width = 15*17
    height = 20
    fig = plt.figure(figsize=(width,height))
    print 'size of image: %d*%d'%(width,height)
    plt.axis("off")
    for i in range(len(img_paths)):
        img_path = img_paths[i]
        score = scores[i]
        a=fig.add_subplot(2,event_num,i+1)
        img = mpimg.imread(img_path)
        plt.imshow(img)
        plt.axis("off")
        a.set_title(img_titles[i],fontsize=40)
        
        a = fig.add_subplot(2,event_num,event_num+i+1)
        a.add_patch(
            patches.Rectangle(
            (0.4, 0.0),   # (x,y)
                     # width
            0.2,    float(score)/float(max_score),       # height
            )
        )
        a.set_title(score_titles[i],fontsize=40)
        plt.axis("off")
    size = fig.get_size_inches()*fig.dpi
    print 'size === '
    print size
    plt.savefig(save_path)
    plt.close()

def visualize_all():
    trend_list = event_dao.get_all_trend_id()
    for trend_id in trend_list:
        visualize_trend(trend_id)
        
def main():
    if (len(sys.argv) != 2):
        print 'usage: python trend_visualize.py trend_id/all/day'
        sys.exit(1)
    trend_id_str = sys.argv[1]
    if (trend_id_str=='all'):
        visualize_all()
    else:
        if (trend_id_str.isdigit()):
            trend_id = int(trend_id_str)
            visualize_trend(trend_id)
        else:#a specific day
            visualize_trend_on_day(trend_id_str)
if __name__ == '__main__':
    main()
       
    