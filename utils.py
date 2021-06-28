from contextlib import contextmanager

import os
import re
import pickle
import tempfile
import requests
import random
import time
import requests_random_user_agent

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
    tfidf_path = './data/run_time/tfidf.p'
    meta_path = './data/run_time/tfidf_meta.p'
    sim_path = './data/run_time/sim_dict.p'
    user_sim_path = './data/run_time/user_sim.p'
    # sql database file
    db_serve_path = './data/run_time/db2.p' # an enriched db.p with various preprocessing info
    database_path = './data/run_time/as.db'
    serve_cache_path = './data/run_time/serve_cache.p'
    
    beg_for_hosting_money = 1 # do we beg the active users randomly for money? 0 = no.
    banned_path = 'banned.txt' # for twitter users who are banned
    tmp_dir = 'tmp'
    

    arxiv_cat = 'stat.ML, cs.AI, cs.AR, cs.CC, cs.CE, cs.CG, cs.CL, cs.CR, cs.CV, cs.CY, '\
    'cs.DB, cs.DC, cs.DL, cs.DM, cs.DS, cs.ET, cs.FL, cs.GL, cs.GR, cs.GT, cs.HC, cs.IR, '\
    'cs.IT, cs.LG, cs.LO, cs.MA, cs.MM, cs.MS, cs.NA, cs.NE, cs.NI, cs.OH, cs.OS, cs.PF, '\
    'cs.PL, cs.RO, cs.SC, cs.SD, cs.SE, cs.SI, cs.SY'

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

def requests_get(url):
    while True:
        try:
            proxy = None
            response = requests.get(url, proxies={'http': proxy}, timeout=5)
            return response
        except:
            print('Fail to download page %s, retrying....'%url)
            time.sleep(1)