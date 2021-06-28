import os
import time
import pickle
import random
import threading
import requests
import requests_random_user_agent

from utils import Config

timeout_secs = 10  # after this many seconds we give up on a paper
if not os.path.exists(Config.pdf_dir):
    os.makedirs(Config.pdf_dir)

db = pickle.load(open(Config.db_path, 'rb'))

# for pid,j in db.items():


def download_one(pid_list):
    # get list of all pdfs we already have
    have = set(os.listdir(Config.pdf_dir))
    numok = 0
    numtot = 0
    proxy = None
    for pid in pid_list:
        j = db[pid]

        pdfs = [x['href']
                for x in j['links'] if x['type'] == 'application/pdf']
        assert len(pdfs) == 1
        pdf_url = pdfs[0] + '.pdf'
        basename = pdf_url.split('/')[-1]
        fname = os.path.join(Config.pdf_dir, basename)

        # try retrieve the pdf
        numtot += 1
        try:
            if not basename in have:
                print('fetching %s into %s' % (pdf_url, fname))
                req = requests.get(
                    pdf_url, timeout=timeout_secs, proxies=proxy)
                with open(fname, 'wb') as fp:
                    fp.write(req.content)
                time.sleep(0.05 + random.uniform(0, 0.1))
            else:
                print('%s exists, skipping' % (fname, ))
            numok += 1
        except Exception as e:
            print('error downloading: ', pdf_url)
            print(e)
            # change proxy
            proxy_info = None
            proxy = {'http': proxy_info, 'https': proxy_info}

        print('%d/%d of %d downloaded ok.' % (numok, numtot, len(db)))


#print('final number of papers downloaded okay: %d/%d' % (numok, len(db)))
if __name__ == '__main__':
    n_threads = 50
    pid_list = list(db.keys())
    stride = len(db)//n_threads
    thread_pool = []
    for start in range(0, len(db), stride):
        end = min(start+stride, len(db))
        t = threading.Thread(target=download_one, args=(pid_list[start:end],))
        thread_pool.append(t)
        t.start()
    for t in thread_pool:
        t.join()
