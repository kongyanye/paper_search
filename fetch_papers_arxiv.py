"""
Queries arxiv API and downloads papers (the query is a parameter).
The script is intended to enrich an existing database pickle (by default db.p),
so this file will be loaded first, and then new results will be added to it.
"""

import os
import time
import pickle
import random
import argparse
import feedparser
import threading

from utils import Config, safe_pickle_dump, requests_get


def encode_feedparser_dict(d):
    """ 
    helper function to get rid of feedparser bs with a deep copy. 
    I hate when libs wrap simple things in their own classes.
    """
    if isinstance(d, feedparser.FeedParserDict) or isinstance(d, dict):
        j = {}
        for k in d.keys():
            j[k] = encode_feedparser_dict(d[k])
        return j
    elif isinstance(d, list):
        l = []
        for k in d:
            l.append(encode_feedparser_dict(k))
        return l
    else:
        return d


def parse_arxiv_url(url):
    """ 
    examples is http://arxiv.org/abs/1512.08756v2
    we want to extract the raw id and the version
    """
    ix = url.rfind('/')
    idversion = url[ix+1:]  # extract just the id (and the version)
    parts = idversion.split('v')
    assert len(parts) == 2, 'error parsing url ' + url
    return parts[0], int(parts[1])


def download_cats(cats):
    assert isinstance(cats, list)
    for cat in cats:
        
        # misc hardcoded variables
        base_url = 'http://export.arxiv.org/api/query?'  # base api query url
        print('### Searching arXiv for %s...' % (cat, ))

        if not os.path.exists(Config.db_arxiv_dir):
            print('Creating directory %s'%Config.db_arxiv_dir)
            os.makedirs(Config.db_arxiv_dir)

        filepath = Config.db_arxiv_dir + cat[4:] + '.p'

        # lets load the existing database to memory
        try:
            db = pickle.load(open(filepath, 'rb'))
        except Exception as e:
            print('error loading existing database:')
            print(e)
            print('starting from an empty database')
            db = {}

        # -----------------------------------------------------------------------------
        # main loop where we fetch the new results
        print('database has %d entries at start' % (len(db), ))
        num_added_total = 0
        exit = False # whether to exit downloading this category
        for i in range(args.start_index, args.max_index, args.results_per_iteration):
            retry_count = 0
            while True:
                try:
                    print("Results %i - %i" % (i, i+args.results_per_iteration))
                    query = 'search_query=%s&sortBy=lastUpdatedDate&start=%i&max_results=%i' % (cat, i, args.results_per_iteration)
                    response = requests_get(base_url+query).content
                    parse = feedparser.parse(response)
                    if len(parse.entries) > 0:
                        break
                    else:
                        print(response)
                        retry_count += 1
                        print('Retry %d times...' % retry_count)
                except Exception as e:
                    print(e)
                time.sleep(args.wait_time)

            num_added = 0
            num_skipped = 0
            for e in parse.entries:

                j = encode_feedparser_dict(e)
                
                # break if we find a paper that is updated after args.date_after
                if j['updated'] < args.date_after:
                    print('Seems the paper is old enough. Save and exit.')
                    exit = True
                    break

                # extract just the raw arxiv id and version for this paper
                rawid, version = parse_arxiv_url(j['id'])
                j['_rawid'] = rawid
                j['_version'] = version

                # add to our database if we didn't have it before, or if this is a new version
                if (rawid not in db or j['_version'] > db[rawid]['_version']) and 'title' in j:
                    db[rawid] = j
                    print('Updated %s added %s' %
                        (j['updated'].encode('utf-8'), j['title'].encode('utf-8')))
                    num_added += 1
                    num_added_total += 1
                else:
                    num_skipped += 1


                            
            # print some information
            print('Added %d papers, skipped %d.' % (num_added, num_skipped))

            if len(parse.entries) == 0:
                print(
                    'Received no results from arxiv. Rate limiting? Exiting. Restart later maybe.')
                print(response)
                break

            if num_added == 0 and args.break_on_no_added == 1:
                print('No new papers were added. Assuming no new papers exist. Exiting.')
                break

            # save the database before we quit, if we found anything new
            if num_added_total > 0:
                print('Saving database with %d papers to %s' %
                    (len(db), filepath))
                safe_pickle_dump(db, filepath)

            print('Sleeping for %i seconds' % (args.wait_time, ))
            time.sleep(args.wait_time + random.uniform(0, 3))
            
            if exit:
                print('Finished downloading category %s'%cat)
                break

if __name__ == "__main__":

    # parse input arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('--search-query', type=str, default='all',
                        help='e.g.: "stat.ML+cs.AI". Query used for arxiv API. See http://arxiv.org/help/api/user-manual#detailed_examples')
    parser.add_argument('--start-index', type=int, default=0,
                        help='0 = most recent API result')
    parser.add_argument('--max-index', type=int, default=50000,
                        help='upper bound on paper index we will fetch, 50,000 is the count limit for arxiv')
    parser.add_argument('--results-per-iteration', type=int,
                        default=100, help='passed to arxiv API')
    parser.add_argument('--wait-time', type=float, default=1.0,
                        help='lets be gentle to arxiv API (in number of seconds)')
    parser.add_argument('--break-on-no-added', type=int, default=1,
                        help='break out early if all returned query papers are already in db? 1=yes, 0=no')
    parser.add_argument('--date-after', type=str, default='2019-01-01',
                        help='only save papers updated after this date')
    args = parser.parse_args()

    if args.search_query == 'all':
        queries = Config.arxiv_cat.split(', ')
    else:
        queries = args.search_query.split('+')
    cat_list = ['cat:'+each for each in queries]

    download_cats(cat_list)

    '''
    n_threads = 10
    cat_list = ['cat:'+each for each in queries]
    stride = len(cat_list)//n_threads+1
    thread_pool = []
    for start in range(0, len(cat_list), stride):
        end = min(start+stride, len(cat_list))
        t = threading.Thread(target=download_cats, args=(cat_list[start:end],))
        thread_pool.append(t)
        t.start()
    for t in thread_pool:
        t.join()
    '''