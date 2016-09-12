# -*- coding: utf-8 -*-
"""
Created on Thu Oct 29 11:50:49 2015

@author: bolx
"""
import StringIO
import gzip
import io
import logging
import math
import operator
import sys
import time
import urllib2 as urllib
import zlib
from PIL import Image
from lxml import etree
import resources

import i_config
import i_util
import read_config

# import n_util
#
# from bolx import config

sys.path.insert(0, '../')
import dao

logger = logging.getLogger("crawl image")
logger.setLevel(logging.INFO)
LEN_IMG_URL = 255
"""
return (state, url_image):  (1, url): thanh cong,    
                            (0, None): Khong get duoc anh, 
                            (2, None): khong lay duoc html
"""


def check_img(uri):
    # if uri[-4:].lower() == '.gif':
    #    return False
    size = getsizes(uri)
    if size[0] < 400 or size[1] < 200:
        return False
    return True


def getsizes(uri):
    try:
        fd = urllib.urlopen(uri, timeout=5)
        image_file = io.BytesIO(fd.read())
        image = Image.open(image_file)
        size = image.size
        image_file.close()
        fd.close()
        return size
    except:
        logger.info('Unexpected error %s: url: %s' % (sys.exc_info()[0], uri))
        return 0, 0


def normal_img_url(url, domain):
    if url[0] == '/' or url[0] == u'/':
        if url[1] == '/' or url[1] == u'/':
            url = 'http:' + url
        else:
            url = 'http://' + domain + url
    if url[1][:5] == 'https':
        url = (url[0], 'http' + url[1][5:])
    return url


def get_img_element(elements, domain):
    for ele in elements:
        if 'src' in ele.attrib and len(ele.attrib['src']) > 1:
            img = normal_img_url(ele.attrib['src'], domain)
            if check_img(img):
                return 1, img
    return 0, None


def get_img_xpath(url, xpaths, domain):
    response = get_html(url)
    # print response
    # print xpaths
    if response is None:
        logger.error('Can\'t request: %s' % url)
        return 2, None
    for xpath in xpaths:
        if len(xpath) < 1:
            continue
        try:
            if xpath[0] == 'm':
                elements = get_element_myxpath(response, xpath[1:])
            else:
                tree = etree.HTML(response)
                elements = tree.findall(xpath)
        except:
            logger.error('Can\'t parse: URL: %s with XPATH: %s' % (url, xpath))
            return 0, None
        img = get_img_element(elements, domain)
        if img[1] is not None:
            return img

    return 0, None


def get_element_myxpath(response, myxpath):
    tree = etree.HTML(response)
    mpath = parse_xpath(myxpath)
    element = set([tree])
    for tag in mpath:
        child = set()
        for ele in element:
            s = ele.findall('.//%s' % tag[0])
            for e in s:
                if tag[1] == '':
                    child.add(e)
                    continue
                if tag[1] not in e.attrib:
                    continue
                if tag[2] not in e.attrib[tag[1]]:
                    continue
                child.add(e)
        element = child
    return element


TIME_OUT = i_config.TIME_OUT_GET


def get_html(url):
    opener = urllib.build_opener()
    opener.addheaders = [('User-agent', 'Mozilla/5.0'), ('Accept-Encoding', 'gzip,deflate')]
    max_iter = 5
    count = 0
    while True:
        try:
            response = opener.open(url, timeout=10)
            return decode(response)
        except:
            logger.info('Unexpected error %s: url: %s' % (sys.exc_info()[0], url))
            if count >= max_iter:
                return None
            time.sleep(1)
            count += 1


def parse_xpath(xpath):
    s = xpath.split('.//')
    result = []
    for ss in s:
        if ss == '':
            continue
        st = 0
        # ed = 0
        while st < len(ss) and not ss[st].isalpha():
            st += 1
        ed = st
        while ed < len(ss) and ss[ed].isalpha():
            ed += 1
        tag = ss[st:ed]

        st = ed
        while st < len(ss) and not ss[st].isalpha():
            st += 1
        ed = st + 1
        while ed < len(ss) and ss[ed].isalpha():
            ed += 1
        attrib = ss[st:ed]

        st = ed
        while st < len(ss) and not ss[st].isalpha():
            st += 1
        ed = st + 1
        while ed < len(ss) and ss[ed] != '"':
            ed += 1
        value = ss[st:ed]
        result.append((tag, attrib, value))
    return result


def read_all_xpath():
    f = open(i_config.FILE_DOMAIN_XPATH, 'r')
    xpaths = dict()
    for line in f:
        s = line.strip().split('\t')
        if len(s) < 2:
            break
        xpaths[s[0]] = s[1:]
    f.close()
    return xpaths


all_xpath = read_all_xpath()


def get_img_all(ids, domains, urls):
    logger.info('Total: %s doc' % (len(ids)))
    imgs = dict()
    for i in ids:
        if i not in domains:
            continue
        if i not in urls:
            continue
        if domains[i] not in all_xpath:
            logger.info('don\'t have xpath: domain %s' % (domains[i]))
            imgs[i] = (3, None)
            continue
        img = get_img_xpath(urls[i], all_xpath[domains[i]], domains[i])
        #   logger.info('doc: %s, url: %s, image: %s'%(i, urls[i], img[1]))
        if img[1] is None:
            imgs[i] = img
            continue
        if len(img[1]) < 2:
            imgs[i] = (0, None)
        imgs[i] = img
    return imgs


def set_log_file(logger, file_path):
    handler = logging.FileHandler(file_path)
    handler.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    ch.setFormatter(formatter)
    logger.addHandler(ch)


set_log_file(logger, resources.LOG_FOLDER + '/crawl_image.log')


def get_img_with_ids(ids, store_error=True):
    conn = dao.get_connection()
    cur = dao.get_cursor(conn)
    query = 'SELECT {1}, {2}, {3} from {4} WHERE id IN ({0})'.format(i_util.ids_to_str(ids), dao.ID, dao.DOMAIN,
                                                                     dao.URL, dao.NEWS_TABLE)
    cur.execute(query)
    rows = cur.fetchall()
    dao.free_connection(conn, cur)
    ids = []
    domains = dict()
    urls = dict()
    for row in rows:
        ids.append(row[0])
        domains[row[0]] = row[1]
        urls[row[0]] = row[2]
    imgs = get_img_all(ids, domains, urls)
    return imgs


def decode(page):
    encoding = page.info().get("Content-Encoding")
    if encoding in ('gzip', 'x-gzip', 'deflate'):
        content = page.read()
        if encoding == 'deflate':
            data = StringIO.StringIO(zlib.decompress(content))
        else:
            data = gzip.GzipFile('', 'rb', 9, StringIO.StringIO(content))
        page = data.read()
    else:
        page = page.read()
    return page


""" ============ Image Title ============== """


def get_thumbnail(image, size=(128, 128), stretch_to_fit=False, greyscale=False):
    """ get a smaller version of the image - makes comparison much faster/easier"""
    if not stretch_to_fit:
        image.thumbnail(size, Image.ANTIALIAS)
    else:
        image = image.resize(size)  # for faster computation
    if greyscale:
        image = image.convert("L")  # Convert it to grayscale.
    return image


def similar_histograms(h1, h2):
    if len(h1) != len(h2):
        return 0.0
    rms = reduce(operator.add, list(map(lambda a, b: a * b, h1, h2)))
    return rms


def get_histogram(uri):
    fd = urllib.urlopen(uri)
    image_file = io.BytesIO(fd.read())
    image = Image.open(image_file)
    image = get_thumbnail(image, greyscale=True)
    h1 = image.histogram()
    m1 = math.sqrt(reduce(operator.add, list(map(lambda a: a * a, h1))))
    h1 = [float(t) / m1 for t in h1]
    return h1


def get_title_img(ids):
    conn = dao.get_connection()
    cur = dao.get_cursor(conn)
    query = 'SELECT %s FROM %s WHERE %s in (%s)' % (
        i_config.IMG_URL, i_config.IMG_TABLE, i_config.IMG_ID, i_util.ids_to_str(ids))
    cur.execute(query)
    rows = cur.fetchall()
    rows = [row[0] for row in rows if row[0] is not None]
    sims = [0] * len(rows)
    histograms = dict()
    for i in range(len(rows)):
        if rows[i] is None:
            continue
        if len(rows[i]) > LEN_IMG_URL:
            continue
        try:
            fd = urllib.urlopen(rows[i], timeout=5)
            image_file = io.BytesIO(fd.read())
            image = Image.open(image_file)
            image = get_thumbnail(image, greyscale=True)
            h1 = image.histogram()
            m1 = math.sqrt(reduce(operator.add, list(map(lambda a: a * a, h1))))
            h1 = [float(t) / m1 for t in h1]
            histograms[i] = h1
        except:
            print sys.exc_info()[0]
            continue
    if len(histograms) < 1:
        return None
    histogram_keys = histograms.keys()
    for i in range(len(histograms)):
        #       print i
        row = histograms[histogram_keys[i]]
        for j in range(len(histograms)):
            other = histograms[histogram_keys[j]]
            if i == j:
                continue
            if similar_histograms(row, other) > 0.8:
                sims[i] += 1
    im = 0
    m = 0
    for i in range(len(sims)):
        if m < sims[i]:
            m = sims[i]
            im = i
    return rows[im]


# -----------------------------------------------------------------
def check_img2(uri):
    size = getsizes(uri)
    if size[0] == 0 and size[1] == 0:
        return True
    if size[0] < 400 or size[1] < 200:
        return False
    return True


def get_ids_img_null():
    conn = dao.get_connection()
    cur = dao.get_cursor(conn)
    query = u'SELECT %s, %s FROM %s WHERE %s is not null' % (
        i_config.IMG_ID, i_config.IMG_URL, i_config.IMG_TABLE, i_config.IMG_URL)
    cur.execute(query)
    rows = cur.fetchall()
    dao.free_connection(conn, cur)
    n = len(rows)
    print n
    it = 0
    for row in rows:
        if not check_img2(row[1]):
            print it, '/', n, '\t', row
            conn = dao.get_connection()
            cur = dao.get_cursor(conn)
            query = 'update NewsDb.news_image SET url_image = null WHERE news_id = %s' % (row[0])
            cur.execute(query)
            conn.commit()
            dao.free_connection(conn, cur)
        it += 1


if __name__ == '__main__':
    get_ids_img_null()
# uri_1 = 'http://media.thethao247.vn/files/000111/042015/15/u21hagl-vs-u19hanquoc-1.jpg'
#    uri_2 = 'http://images.vov.vn/uploaded/trongphu/2015_11_29/VOV9_MMNN.jpg?width=490'
#    print similar_histograms(get_histogram(uri_1), get_histogram(uri_2))
# #   pass
# #   url = 'http://news.zing.vn/Xe-Ford-phong-86-kmh--khong-du-can-cu-xu-phat-post598940.html'
#  #  url = 'http://motthegioi.vn/thoi-su/vu-xa-sung-o-california-nguyen-nhan-ngay-cang-giong-khung-bo-263251.html'    
#  #  xpath = './/div[@class="detail_content fl mgt15"].//img'
#   # print get_html(url)
#    
# #    opener = urllib.build_opener()
# #    opener.addheaders = [
# #    ('User-agent', 'Mozilla/5.0'),
# #    ('Accept-Encoding', 'gzip,deflate')]
# #    usock = opener.open(url)
# #   # url = usock.geturl()
# #    data = decode(usock)
# #    usock.close() 9674936, 58295041, 58294536, 58295654
# #   print data
#    
#    print get_img_with_ids([61163384])
