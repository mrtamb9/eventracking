import os,sys,read_config
sys.path.insert(0,'..')
import util
def main():
    util.create_folder(read_config.ENTITY_DATA_CONFIG.root_folder + read_config.ENTITY_DATA_CONFIG.aids_folder)
    util.create_folder(read_config.ENTITY_DATA_CONFIG.root_folder + read_config.ENTITY_DATA_CONFIG.corpus_size_in_day)
    util.create_folder(read_config.ENTITY_DATA_CONFIG.root_folder + read_config.ENTITY_DATA_CONFIG.corpus_size_to_day)
    util.create_folder(read_config.ENTITY_DATA_CONFIG.root_folder + read_config.ENTITY_DATA_CONFIG.dictionary_in_day)
    util.create_folder(read_config.ENTITY_DATA_CONFIG.root_folder + read_config.ENTITY_DATA_CONFIG.dictionary_to_day)
    util.create_folder(read_config.ENTITY_DATA_CONFIG.root_folder + read_config.ENTITY_DATA_CONFIG.entity_folder)
if __name__ == '__main__':
    main()
    