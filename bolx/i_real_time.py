# -*- coding: utf-8 -*-
"""
Created on Wed Dec 23 13:37:08 2015

@author: bolx
"""
import sys

import i_get_image
import run_find_img

sys.path.insert(0, '../')
import dao


def insert_replace_imgs(news_id, url_image):
    conn = dao.get_connection()
    cur = dao.get_cursor(conn)
    for i in news_id:
        run_find_img.insert_replace_img(cur, i, url_image[i])
    conn.commit()
    dao.free_connection(conn, cur)


def process(ids, minibatch=20):
    ids = run_find_img.get_ids_not_exist(ids)
    if len(ids) < 1:
        return
    all_ids = list(ids)
    while len(all_ids) != 0:
        temp_list = []
        for i in range(minibatch):
            if len(all_ids) > 0:
                tid = all_ids.pop()
                temp_list.append(tid)
            else:
                break
        if len(temp_list) > 0:
            print temp_list
            imgs = i_get_image.get_img_with_ids(temp_list)
            insert_replace_imgs(temp_list, imgs)


if __name__ == '__main__':
    process(
        [62762570, 62762574, 62762579, 62762604, 62762611, 62762613, 62762618, 62762621, 62762627, 62762633, 62762635,
         62762647, 62762652, 62762654, 62762661, 62762663, 62762672, 62762673, 62762676, 62762677, 62762682, 62762690,
         62762702, 62762710, 62762712, 62762714, 62762736, 62762739, 62762740, 62762744, 62762745, 62762748, 62762765,
         62762768, 62762770, 62762774, 62762777, 62762778, 62762784, 62762790, 62762796, 62762800, 62762812, 62762828,
         62762829, 62762835, 62762840, 62762843, 62762858, 62762860, 62762862, 62762870, 75115604])
