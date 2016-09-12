import util,resources,online_resources,real_time_resources2
def main():
    print "start installing trend_service"
    util.create_folder(resources.LOG_FOLDER)
    util.create_folder(resources.IMG_FOLDER)
    util.create_folder(resources.TEXT_FOLDER)
    util.create_folder(resources.TREND_FOLDER)
    util.create_folder(resources.TEMPARORY_DATA)
    util.create_folder(resources.STAT_FOLDER)
    util.create_folder(resources.DAYS_PATH)
    util.create_folder(resources.TEMP_CENTER_FOLDER)
    util.create_folder(resources.CATID_FOLDER)
    util.create_folder(resources.DAY_DATA_FOLDER)
    
    util.create_folder(online_resources.CATID_FOLDER)
    util.create_folder(online_resources.CURRENT_FOLDER)
    util.create_folder(online_resources.IMG_FOLDER)
    util.create_folder(online_resources.ONLINE_FOLDER)
    util.create_folder(online_resources.STAT_FOLDER)
    util.create_folder(online_resources.TEXT_FOLDER)
    util.create_folder(online_resources.DAY_DATA_FOLDER)
    
    #util.create_folder(sub_resources.SUB_EVENT_FOLDER)
    
#    util.create_folder(bi_resources.BIGRAM_FOLDER)
#    util.create_folder(bi_resources.DICTIONARY_FOLDER)
#    util.create_folder(bi_resources.DATA_FOLDER)
#    util.create_folder(bi_resources.REAL_DATA_FOLDER)
    
    util.create_folder(real_time_resources2.SAVE_FOLDER)
    util.create_folder(real_time_resources2.IMG_FOLDER)
    util.create_folder(real_time_resources2.TEXT_FOLDER)
    util.create_folder(real_time_resources2.SUB_TREND_FOLDER)
    util.create_folder(real_time_resources2.BATCH_LOG_FOLDER)

if (__name__ == '__main__'):
    main()
