from xml.etree import ElementTree
import os
import sys
sys.path.insert(0, '../')
import resources
# class news_table_config(object):
#     __slots__ = ["name_table", "article_id", ]
#     def get_news_table_config():

# ENTITY_DATA_CONFIG = None
ROOT_CONFIG = os.path.dirname(os.path.abspath(__file__))+'/resources'
ENTITY_DATA_CONFIG_FILE = ROOT_CONFIG + '/entity_data.xml'


class _EntityDataConfig(object):
    ROOT_PATH_FIELD = "root"
    DICTIONARY_TO_DAY_FIELD = "dictionary_to_day"
    DICTIONARY_IN_DAY_FIELD = "dictionary_in_day"
    CORPUS_SIZE_TO_DAY_FIELD = "porpus_size_to_day"
    CORPUS_SIZE_IN_DAY_FIELD = "porpus_size_in_day"
    ENTITY_FOLDER_FIELD = "entity_folder"
    AIDS_FOLDER_FIELD = "aids_folder"
    FILE_FORMAT_FIELD = "format_file_name_data"
    instance = None
    __slots__ = ['root_folder', 'dictionary_to_day', 'dictionary_in_day', 'corpus_size_to_day', 'corpus_size_in_day',
                 'entity_folder', 'aids_folder', 'format_file_name_data', 'check_done']

    def __init__(self):
        self.check_done = False

        tree = ElementTree.parse(ENTITY_DATA_CONFIG_FILE)
        root = tree.getroot()

        element = root.find(_EntityDataConfig.ROOT_PATH_FIELD)
        if element is not None:
            self.root_folder = element.text.strip()
        
        self.root_folder = resources.DATA_FOLDER + self.root_folder

        element = root.find(_EntityDataConfig.DICTIONARY_TO_DAY_FIELD)
        if element is not None:
            self.dictionary_to_day = element.text.strip()

        element = root.find(_EntityDataConfig.DICTIONARY_IN_DAY_FIELD)
        if element is not None:
            self.dictionary_in_day = element.text.strip()

        element = root.find(_EntityDataConfig.CORPUS_SIZE_TO_DAY_FIELD)
        if element is not None:
            self.corpus_size_to_day = element.text.strip()

        element = root.find(_EntityDataConfig.CORPUS_SIZE_IN_DAY_FIELD)
        if element is not None:
            print "AAA"
            self.corpus_size_in_day = element.text.strip()

        element = root.find(_EntityDataConfig.ENTITY_FOLDER_FIELD)
        if element is not None:
            self.entity_folder = element.text.strip()

        element = root.find(_EntityDataConfig.AIDS_FOLDER_FIELD)
        if element is not None:
            self.aids_folder = element.text.strip()

        element = root.find(_EntityDataConfig.FILE_FORMAT_FIELD)
        if element is not None:
            self.format_file_name_data = element.text.strip()

        self.check_done = True
        print "SSS"


ENTITY_DATA_CONFIG = _EntityDataConfig()


"""================================================================================================================="""


USER_DB_CONFIG_FILE = ROOT_CONFIG + '/user_db.xml'


class _UserDbDataConfig(object):
    HOST_NAME_FIELD = "host_name"
    DB_NAME_FIELD = "db_name"
    USER_NAME_FIELD = "user_name"
    PASSWORD_FIELD = "password"
    instance = None
    __slots__ = ['host_name', 'db_name', 'user_name', 'password', 'check_done']

    def __init__(self):
        self.check_done = False

        tree = ElementTree.parse(ENTITY_DATA_CONFIG_FILE)
        root = tree.getroot()

        element = root.find(_UserDbDataConfig.HOST_NAME_FIELD)
        if element is not None:
            self.host_name = element.text.strip()

        element = root.find(_UserDbDataConfig.DB_NAME_FIELD)
        if element is not None:
            self.db_name = element.text.strip()

        element = root.find(_UserDbDataConfig.USER_NAME_FIELD)
        if element is not None:
            self.user_name = element.text.strip()

        element = root.find(_UserDbDataConfig.PASSWORD_FIELD)
        if element is not None:
            self.password = element.text.strip()

        self.check_done = True
        print "SSS"


USER_DB_CONFIG = _UserDbDataConfig()


if __name__ == '__main__':
    s = None    # type: str
    # ed = _EntityDataConfig.get_instance()
    # print ed
    # ed = _EntityDataConfig.get_instance()
    # print ed
    # ed = _EntityDataConfig.get_instance()
    # print ed
