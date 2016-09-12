import os,sys
import util,resources
from wordcloud import WordCloud
#import trend_visualize,trend_util

def generate_top_cloud(top_folder,save_folder):
    util.create_folder(save_folder)
    fnames = os.listdir(top_folder)
    top_pre ='top'
    for name in fnames:
        if (top_pre in name):
            fpath = top_folder + '/'+name
            save_path = save_folder+'/'+name+'.'+resources.TOP_IMG_EXTENSION
            print 'img at: %s'%name
            generate_img(fpath,save_path)
def generate_img(top_path,save_path,width=resources.TOP_WIDTH,height = resources.TOP_HEIGHT):
    fres = util.read_top_word_file(top_path)
    #for pair in fres:
    #    (word,score) = pair
    #   print '%s: score:%f'%(word,score)
    #print '-----------------------'
    wc = WordCloud(width=width,height=height,\
    background_color=resources.TOP_BACKGROUND_COLOR).fit_words(fres)
    wc.to_file(save_path)

def generate_report_img(ids,dates,article_nums,save_path):
    img_paths = []
    img_titles = []
    scores = []
    score_titles = []
    for i in range(len(ids)):
        aid = ids[i]
        date = dates[i]
        img_paths.append(trend_util.get_img_path(aid,date))
        img_titles.append(str(aid))
        scores.append(article_nums[i])
        score_titles.append(str(article_nums[i]))
    #trend_visualize.visualzie_series(img_paths,img_titles,scores,score_titles,save_path)

def main():
    if (len(sys.argv) != 3):
        print 'usage: python top_word_img.py top_folder save_folder'
        sys.exit(1)
    top_folder = sys.argv[1]
    save_folder = sys.argv[2]
    generate_top_cloud(top_folder,save_folder)

if __name__=='__main__':
    main()