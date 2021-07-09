from contextlib import contextmanager

import os
import re
import pickle
import tempfile
import requests
import random
import time
import json
import dateutil.parser

# global settings
# -----------------------------------------------------------------------------


class Config(object):
    # main paper information repo file
    db_arxiv_dir = './data/arxiv/'
    db_other_dir = './data/other/'
    todo_paper_link = './data/runtime/todo.txt'
    # intermediate processing folders
    runtime_dir = './data/runtime/'
    pdf_dir = os.path.join('data', 'arxiv_pdf')
    txt_dir = os.path.join('data', 'arxiv_txt')
    thumbs_dir = os.path.join('static', 'arxiv_thumbs')
    # intermediate pickles
    tfidf_path = './data/runtime/tfidf.p'
    meta_path = './data/runtime/tfidf_meta.p'
    sim_path = './data/runtime/sim_dict.p'
    user_sim_path = './data/runtime/user_sim.p'
    # sql database file
    # an enriched db.p with various preprocessing info
    db_serve_path = './data/runtime/db2.p'
    database_path = './data/runtime/as.db'
    serve_cache_path = './data/runtime/serve_cache.p'

    # do we beg the active users randomly for money? 0 = no.
    beg_for_hosting_money = 1
    banned_path = 'banned.txt'  # for twitter users who are banned
    tmp_dir = 'tmp'

    arxiv_cat = 'stat.ML, cs.AI, cs.AR, cs.CC, cs.CE, cs.CG, cs.CL, cs.CR, cs.CV, cs.CY, '\
        'cs.DB, cs.DC, cs.DL, cs.DM, cs.DS, cs.ET, cs.FL, cs.GL, cs.GR, cs.GT, cs.HC, cs.IR, '\
        'cs.IT, cs.LG, cs.LO, cs.MA, cs.MM, cs.MS, cs.NA, cs.NE, cs.NI, cs.OH, cs.OS, cs.PF, '\
        'cs.PL, cs.RO, cs.SC, cs.SD, cs.SE, cs.SI, cs.SY'
    default_published_time = '2000-01-01'
# Context managers for atomic writes courtesy of
# http://stackoverflow.com/questions/2333872/atomic-writing-to-file-with-python


@contextmanager
def _tempfile(*args, **kws):
    """ Context for temporary file.

    Will find a free temporary filename upon entering
    and will try to delete the file on leaving

    Parameters
    ----------
    suffix : string
        optional file suffix
    """

    fd, name = tempfile.mkstemp(*args, **kws)
    os.close(fd)
    try:
        yield name
    finally:
        try:
            os.remove(name)
        except OSError as e:
            if e.errno == 2:
                pass
            else:
                raise e


@contextmanager
def open_atomic(filepath, *args, **kwargs):
    """ Open temporary file object that atomically moves to destination upon
    exiting.

    Allows reading and writing to and from the same filename.

    Parameters
    ----------
    filepath : string
        the file path to be opened
    fsync : bool
        whether to force write the file to disk
    kwargs : mixed
        Any valid keyword arguments for :code:`open`
    """
    fsync = kwargs.pop('fsync', False)

    with _tempfile(dir=os.path.dirname(filepath)) as tmppath:
        with open(tmppath, *args, **kwargs) as f:
            yield f
            if fsync:
                f.flush()
                os.fsync(f.fileno())
        os.rename(tmppath, filepath)


def safe_pickle_dump(obj, fname):
    with open_atomic(fname, 'wb') as f:
        pickle.dump(obj, f, -1)


# arxiv utils
# -----------------------------------------------------------------------------

def strip_version(idstr):
    """ identity function if arxiv id has no version, otherwise strips it. """
    parts = idstr.split('v')
    return parts[0]

# "1511.08198v1" is an example of a valid arxiv id that we accept


def isvalidid(pid):
    return re.match('^\d+\.\d+(v\d+)?$', pid)


ua_list = [
    "Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2228.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2227.1 Safari/537.36",
    "Mozilla/5.0 (X11; OpenBSD i386) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/36.0.1985.125 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/36.0.1944.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 5.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/35.0.3319.102 Safari/537.36",
    "Mozilla/5.0 (Windows NT 5.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/35.0.2309.372 Safari/537.36",
    "Mozilla/5.0 (Windows NT 5.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/35.0.2117.157 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/35.0.1916.47 Safari/537.36",
    "Mozilla/5.0 (Windows NT 5.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/34.0.1866.237 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/34.0.1847.137 Safari/4E423F",
    "Mozilla/5.0 (Windows NT 6.1; WOW64; rv:40.0) Gecko/20100101 Firefox/40.1",
    "Mozilla/5.0 (Windows NT 6.3; rv:36.0) Gecko/20100101 Firefox/36.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10; rv:33.0) Gecko/20100101 Firefox/33.0",
    "Mozilla/5.0 (X11; Linux i586; rv:31.0) Gecko/20100101 Firefox/31.0",
    "Mozilla/5.0 (Windows NT 6.1; WOW64; rv:31.0) Gecko/20130401 Firefox/31.0",
    "Mozilla/5.0 (Windows NT 5.1; rv:31.0) Gecko/20100101 Firefox/31.0",
    "Opera/9.80 (X11; Linux i686; Ubuntu/14.10) Presto/2.12.388 Version/12.16",
    "Opera/9.80 (Windows NT 6.0) Presto/2.12.388 Version/12.14",
    "Mozilla/5.0 (Windows NT 6.0; rv:2.0) Gecko/20100101 Firefox/4.0 Opera 12.14",
    "Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.0) Opera 12.14",
    "Opera/9.80 (Windows NT 5.1; U; zh-sg) Presto/2.9.181 Version/12.00"
]


count = 50

def get_ip_pool():

    url1 = ''
    url2 = ''
    ip_pool = []
    for url in [url1, url2]:
        ip_pool += requests.get(url).text.strip().split('\r\n')
    return ip_pool

#ip_pool = get_ip_pool()
#failed = {}


def requests_get(url):
    global ip_pool
    global failed
    retry_count = 0
    while retry_count <= 3:
        try:
            if len(failed) < len(ip_pool):
                proxy = ip_pool[len(failed)]
                failed[proxy] = 0
            else:
                failed_sorted = sorted(failed, key=lambda x: failed[x])
                if failed[failed_sorted[0]] > 2:
                    ip_pool = get_ip_pool()
                    failed = {}
                    continue
                proxy = random.choice(failed_sorted[:count])
            response = requests.get(url, proxies={'http': proxy, 'https': proxy}, headers={
                                    'User-Agent': random.choice(ua_list)}, timeout=10)
            return response
        except Exception as e:
            #print('Fail to download page %s: %s, retrying....' % (url, e))
            time.sleep(1)
            retry_count += 1
            failed[proxy] += 1
    return ''


def requests_get_test(url):
    while True:
        try:
            proxy = random.choice(get_ip_pool())
            response = requests.get(url, proxies={'http': proxy, 'https': proxy}, headers={
                                    'User-Agent': random.choice(ua_list)}, timeout=10)
            return response
        except Exception as e:
            print('Fail to download page %s: %s, retrying....' % (url, e))
            time.sleep(1)

def load_db():
    # read arxiv papers
    print('Reading files from arxiv db...')
    db_arxiv = pickle.load(open(Config.db_arxiv_dir+'db_arxiv.p', 'rb'))
    print('%d papers loaded from arxiv'%len(db_arxiv))

    # read other papers
    print('Reading files from other db...')
    db_other = pickle.load(open(Config.db_other_dir+'db_other.p', 'rb'))
    print('%d papers loaded from other'%len(db_other))

    # combine the two db into one
    db_other.update(db_arxiv)
    print('%d papers found in total'%len(db_other))
    return db_other

def parse_time(t):
    try:
        timestruct = dateutil.parser.parse(t)
    except:
        try:
            timestruct = dateutil.parser.parse(t.split('-')[1])
        except:
            try:
                timestruct = dateutil.parser.parse(t.split('/')[-1])
            except:
                timestruct = dateutil.parser.parse(Config.default_published_time)
    return timestruct

def flatten(l):
    if isinstance(l, list):
        res = []
        for each in l:
            res += flatten(each)
        return res
    else:
        return [l]