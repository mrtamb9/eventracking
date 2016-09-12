# -*- coding: utf-8 -*-
"""
Created on Sat Nov 14 09:09:26 2015

@author: bolx
"""
import read_config


def get_data_path(day):
    ED = read_config.ENTITY_DATA_CONFIG
    return ED.root_folder + ED.entity_folder + ED.format_file_name_data.format(day)


def get_dictionary_path(day):
    ED = read_config.ENTITY_DATA_CONFIG
    return ED.root_folder + ED.dictionary_to_day + ED.format_file_name_data.format(day)


def get_corpus_size_path(day):
    ED = read_config.ENTITY_DATA_CONFIG
    return ED.root_folder + ED.corpus_size_to_day + ED.format_file_name_data.format(day)


def get_dictionary_temp_path(day):
    ED = read_config.ENTITY_DATA_CONFIG
    return ED.root_folder + ED.dictionary_to_day + ED.format_file_name_data.format(day)


def get_corpus_size_temp_path(day):
    ED = read_config.ENTITY_DATA_CONFIG
    return ED.root_folder + ED.corpus_size_in_day + ED.format_file_name_data.format(day)


def get_ids_path(day):
    ED = read_config.ENTITY_DATA_CONFIG
    return ED.root_folder + ED.aids_folder + ED.format_file_name_data.format(day)


def get_ids_folder():
    ED = read_config.ENTITY_DATA_CONFIG
    return ED.root_folder + ED.aids_folder
    # return E_IDS_FOLDER


def get_dictionary_folder(day):
    ED = read_config.ENTITY_DATA_CONFIG
    return ED.root_folder + ED.entity_folder + ED.format_file_name_data.format(day)
    # return E_DICTION_FOLDER + day


# def get_real_ids():
#     ED = read_config.ENTITY_DATA_CONFIG
#     return ED.root_folder + ED.entity_folder + ED.format_file_name_data.format(day)
#     return E_REAL_IDS  # E_DATA_FOLDER_ALL + 'real/ids'


# def get_real_data():
#     ED = read_config.ENTITY_DATA_CONFIG
#     return ED.root_folder + ED.entity_folder + ED.format_file_name_data.format(day)
#     return E_REAL_DATA  # + 'real/data'


# def get_real_dictionary():
#     ED = read_config.ENTITY_DATA_CONFIG
#     return ED.root_folder + ED.entity_folder + ED.format_file_name_data.format(day)
#     return E_REAL_DICTION  # + 'real/dictionary'
