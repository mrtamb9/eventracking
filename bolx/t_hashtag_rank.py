# -*- coding: utf-8 -*-
"""
Created on Fri Dec 11 10:00:39 2015

@author: bolx
"""
import datetime
import math
import operator
import sys

import bolx_dao_all
import i_config
import i_util

sys.path.insert(0, '../')
import util, event_dao

NUM = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
VN_STR = [u'a', u'b', u'c', u'd', u'e', u'f', u'g', u'h', u'i', u'j', u'k', u'l', u'm', u'n', u'o', u'p', u'q', u'r',
          u's', u't', u'u', u'v', u'w', u'x', u'y', u'z', u'á', u'à', u'ả', u'ã', u'ạ', u'ă', u'ắ', u'ặ', u'ằ', u'ẳ',
          u'ẵ', u'â', u'ấ', u'ầ', u'ẩ', u'ẫ', u'ậ', u'đ', u'é', u'è', u'ẻ', u'ẽ', u'ẹ', u'ê', u'ế', u'ề', u'ể', u'ễ',
          u'ệ', u'í', u'ì', u'ỉ', u'ĩ', u'ị', u'ó', u'ò', u'ỏ', u'õ', u'ọ', u'ô', u'ố', u'ồ', u'ổ', u'ỗ', u'ộ', u'ơ',
          u'ớ', u'ờ', u'ở', u'ỡ', u'ợ', u'ú', u'ù', u'ủ', u'ũ', u'ụ', u'ư', u'ứ', u'ừ', u'ử', u'ữ', u'ự', u'ý', u'ỳ',
          u'ỷ', u'ỹ', u'ỵ']
MAP_NORMAL_STR = {u'a': 'a', u'b': 'b', u'c': 'c', u'd': 'd', u'e': 'e', u'f': 'f', u'g': 'g', u'h': 'h', u'i': 'i',
                  u'j': 'j', u'k': 'k', u'l': 'l', u'm': 'm', u'n': 'n', u'o': 'o', u'p': 'p', u'q': 'q', u'r': 'r',
                  u's': 's', u't': 't', u'u': 'u', u'v': 'v', u'w': 'w', u'x': 'x', u'y': 'y', u'z': 'z', u'á': 'a',
                  u'à': 'a', u'ả': 'a', u'ã': 'a', u'ạ': 'a', u'ă': 'a', u'ắ': 'a', u'ặ': 'a', u'ằ': 'a', u'ẳ': 'a',
                  u'ẵ': 'a', u'â': 'a', u'ấ': 'a', u'ầ': 'a', u'ẩ': 'a', u'ẫ': 'a', u'ậ': 'a', u'đ': 'd', u'é': 'e',
                  u'è': 'e', u'ẻ': 'e', u'ẽ': 'e', u'ẹ': 'e', u'ê': 'e', u'ế': 'e', u'ề': 'e', u'ể': 'e', u'ễ': 'e',
                  u'ệ': 'e', u'í': 'i', u'ì': 'i', u'ỉ': 'i', u'ĩ': 'i', u'ị': 'i', u'ó': 'o', u'ò': 'o', u'ỏ': 'o',
                  u'õ': 'o', u'ọ': 'o', u'ô': 'o', u'ố': 'o', u'ồ': 'o', u'ổ': 'o', u'ỗ': 'o', u'ộ': 'o', u'ơ': 'o',
                  u'ớ': 'o', u'ờ': 'o', u'ở': 'o', u'ỡ': 'o', u'ợ': 'o', u'ú': 'u', u'ù': 'u', u'ủ': 'u', u'ũ': 'u',
                  u'ụ': 'u', u'ư': 'u', u'ứ': 'u', u'ừ': 'u', u'ử': 'u', u'ữ': 'u', u'ự': 'u', u'ý': 'y', u'ỳ': 'y',
                  u'ỷ': 'y', u'ỹ': 'y', u'ỵ': 'y'}


def normal(s):
    ss = s.lower()
    result = ''
    for i in ss:
        if i in VN_STR:
            result += MAP_NORMAL_STR[i]
        elif i == ' ' or i in NUM:
            result += i
        else:
            result += ''
    return " ".join(result.split())


def f1(i):
    return float(1) / (i + 2)


def f2(i):
    return float(1) / (i + 2)


def f_size(size):
    return 1 + math.log(size)


def get_most_hashtag(tags, date_strs, sizes, n_tag=3):
    scores_normal = dict()
    map_tag = dict()
    scores = dict()
    dates = [datetime.datetime.strptime(d, i_util.DATE_FORMAT).date() for d in date_strs]
    dates_n = sorted(set(dates), reverse=True)
    date_m = dict()
    for i in range(len(dates_n)):
        date_m[dates_n[i]] = i
    delta_dates = [date_m[d] for d in dates]
    for i in range(len(tags)):
        tag = tags[i]
        dd = f1(delta_dates[i]) * f_size(sizes[i])
        for j in range(len(tag)):
            if tag[j] in scores:
                scores[tag[j]] += dd * f2(j)
            else:
                scores[tag[j]] = dd * f2(j)
    for t in scores:
        nor = normal(t)
        if nor in scores_normal:
            scores_normal[nor] += scores[t]
            map_tag[nor].add(t)
        else:
            scores_normal[nor] = scores[t]
            map_tag[nor] = set([t])
    tag_nor_sort = sorted(scores_normal.items(), key=operator.itemgetter(1), reverse=True)
    tag_nor_sort = [ss[0] for ss in tag_nor_sort]
    max_tags = []
    i = 0
    for tag_max in tag_nor_sort:
        if i >= n_tag:
            break
        if tag_max != '':
            c = True
            for t in max_tags:
                if t in tag_max or tag_max in t:
                    #      print '==== ', t, '=', tag_max
                    c = False
                    break
            if c:
                max_tags.append(tag_max)
                i += 1
    result = []
    for tag in max_tags:
        tag_max = ''
        v_max = 0
        for t in map_tag[tag]:
            if scores[t] > v_max:
                v_max = scores[t]
                tag_max = t
        if tag_max != '':
            result.append(tag_max)
    return result


""" Get tag theo danh sach cac envent id """


def get_tag_by_events(event_ids, nday=3):
    (tags, dates, sizes) = bolx_dao_all.get_tag_date_events_between_days(event_ids, nday)
    return get_most_hashtag(tags, dates, sizes)


""" Dien cac tag cho cac trend cu """


def fill_tag_all_trend(nday):
    trend_date_events = bolx_dao_all.get_event_ids_date_end_trends()
    for trend_id in trend_date_events:
        event_ids = trend_date_events[trend_id][0]
        (tags, dates, sizes) = bolx_dao_all.get_tag_date_events_between_days(event_ids, nday)
        ta = get_most_hashtag(tags, dates, sizes)
        bolx_dao_all.fill_tags_trend(trend_id, ta)


def fill_tag_all_trend_by_last_day(last_day, nday=3):
    # trend_date_events = bolx_dao_all.get_event_ids_trends_by_date_end()#get_event_ids_date_end_trends()
    trend_date_events = bolx_dao_all.get_event_ids_trends_by_date_end(last_day)
    for trend_id in trend_date_events:
        event_ids = trend_date_events[trend_id][0]
        (tags, dates, sizes) = bolx_dao_all.get_tag_date_events_between_days(event_ids, nday)
        ta = get_most_hashtag(tags, dates, sizes)
        bolx_dao_all.fill_tags_trend(trend_id, ta)


def process(day):
    fill_tag_all_trend_by_last_day(day)


def main():
    if len(sys.argv) < 2:
        print 'usage: python t_hashtag_rank.py date/real/max'
        return
    date = sys.argv[1]
    if date == 'next':
        max_date = event_dao.get_max_date(i_config.EVENT_TABLE)
        print 'process max day: %s' % max_date
        process(max_date)
    # elif date == 'next':
    #     current_day = event_dao.get_max_date(resources.EVENT_TABLE)
    #     #  next_day = util.get_ahead_day(current_day,1)
    #     print 'process next day: %s' % current_day
    #     process(current_day)
    else:
        if len(sys.argv) == 2:
            print 'process date: %s' % date
            process(date)
        else:
            date_2 = sys.argv[2]
            print 'process date: %s %s' % (date, date_2)
            start = date
            while start <= date_2:
                process(start)
                start = util.get_ahead_day(start, 1)


if __name__ == '__main__':
    # ids = [9842179,57664088,13168694,12315648,12584487,13119556,13191301,13919269,14590464,15984139,39298591,17066436,
    # 17580933,39331614,18560514,18600453,20119895,20193337,20456456,20718656,21008928,21456939,21606467,22616690,
    # 22919897,23414799,24248329,24253023,24293382,24690708,25247759,26212116,26460202,28533341,29167488,29678168,
    # 29718529,30292742,30521346,31352833,31417965,31776785,35646502,38660609,38671881,39243287,40370190,41617417,
    # 42500008,42579485,43809823,45172244,46732806,47596900,48318848,49597898,51011208,51358313,52992555,53588519,
    # 56814264,56969654,57045008,57208850]
    # print get_tag_by_events(ids, nday = 3)
    main()
    # fill_tag_all_trend(3)
