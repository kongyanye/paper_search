"""
Scrape papers (including abstract, author, date) from dblp and other sites.
The script is intended to enrich an existing database pickle (by default db.p),
so this file will be loaded first, and then new results will be added to it.
"""

import os
import time
import pickle
import random
import argparse
import feedparser
import requests
import threading
import pandas as pd
from bs4 import BeautifulSoup

import sys

from utils import Config, safe_pickle_dump, requests_get


def load_ccf_list():
    output_dir = Config.runtime_dir
    if not os.path.exists(output_dir+'ccf.csv'):
        print('%s/ccf.csv not found. Initialize the file...' % (output_dir))
        if not os.path.exists(output_dir+'ccf.txt'):
            if not os.path.exists('./data/中国计算机学会推荐国际学术会议和期刊目录-2019.pdf'):
                print('Fail to find file ./data/中国计算机学会推荐国际学术会议和期刊目录-2019.pdf. Please download it first (https://www.ccf.org.cn/ccf/contentcore/resource/download?ID=99185).')
                sys.exit(1)
            os.system(
                'pdf2txt ./data/中国计算机学会推荐国际学术会议和期刊目录-2019.pdf %sccf.txt' % output_dir)
        f = open(output_dir+'ccf.txt', 'r').readlines()
        f = [each.strip() for each in f]

        ccf_link = open(output_dir+'ccf_link.txt', 'w')
        # save all links in ccf_link.txt
        for each in f:
            if 'http://' in each or 'https://' in each:
                ccf_link.write(each+'\n')
        ccf_link.close()

        # save all info in ccf.csv
        level = None
        i = -1
        res = []
        while i < len(f)-1:
            i += 1
            line = f[i]
            if not level:
                if 'A 类' in line:
                    level = 'A'
                continue
            else:
                if 'A 类' in line:
                    level = 'A'
                    continue
                elif 'B 类' in line:
                    level = 'B'
                    continue
                elif 'C 类' in line:
                    level = 'C'
                    continue

            if line.startswith('http'):
                link = f[i]
                full_name = []
                for j in range(i-4, 0, -1):
                    if f[j]:
                        full_name.append(f[j])
                    else:
                        ti = j
                        break
                full_name = ' '.join(full_name)
                if f[ti-1].startswith('http') or f[ti-1].isdigit():
                    short_name = ''
                else:
                    short_name = f[ti-1]
                res.append([level, short_name, full_name, link])
        res = pd.DataFrame(
            res, columns=['level', 'short_name', 'full_name', 'link'])
        res.to_csv(output_dir+'ccf.csv', index=False)

    ccf = pd.read_csv(output_dir+'ccf.csv')
    return ccf


def get_paper_links_dblp(link, lock=None):
    assert 'dblp' in link
    #print('Parsing site %s for paper links...'%link)

    if 'journals' in link:
        link_type = 'j'
    else:
        link_type = 'c'

    def parse_one_page(page_link):
        res = []
        page_con = requests_get(page_link)
        page_soup = BeautifulSoup(page_con.text, 'html.parser')
        if link_type == 'j':
            class_type = 'entry article'
        else:
            class_type = 'entry inproceedings'

        for paper in page_soup.find_all('li', {'class': class_type}):
            paper_link = paper.find('nav').find(
                'div', {'class': 'head'}).find('a')
            if paper_link: # some entries do not have href
                res.append(paper_link['href'])
        return res

    res = []
    page_index = requests_get(link)
    page_index_soup = BeautifulSoup(page_index.text, 'html.parser')

    if link_type == 'c':
        for conf in page_index_soup.find_all('ul', {'class': 'publ-list'}):
            for sub_conf in conf.children:
                sub_conf_link = sub_conf.find('nav').find(
                    'div', {'class': 'head'}).find('a')
                if sub_conf_link:
                    res += parse_one_page(sub_conf_link['href'])
    else:
        # the first <ul> after info section is what we want
        info_sec = page_index_soup.find('div', {'id': 'info-section'})
        while True:
            s = info_sec.find_next_sibling()
            if s.name == 'ul':
                break
            else:
                info_sec = s
        for vol in s.find_all('a'):
            vol_link = vol['href']
            res += parse_one_page(vol_link)

    print('%d paper links fetched for %s' % (len(res), link))
    if lock:
        lock.acquire()
    with open(Config.todo_paper_link, 'a') as f:
        for each in res:
            f.write(each+'\n')
    if lock:
        lock.release()


def parse_site(link, lock=None):
    if 'dblp' in link:
        get_paper_links_dblp(link, lock)
    else:
        return


def parse_sites(link, lock=None):
    if isinstance(link, list):
        for l in link:
            parse_site(l, lock)
    else:
        parse_site(link, lock)


if __name__ == '__main__':
    if os.path.exists(Config.todo_paper_link):
        print('File %s already exists. Please remove it first.' %
              Config.todo_paper_link)
        sys.exit(1)

    ccf = load_ccf_list()
    interested_links = ccf.loc[ccf.level.isin(
        ['A', 'B']), 'link'].values.tolist()
    print('%d sites to parse in total' % len(interested_links))

    n_threads = 10
    output_path = Config.todo_paper_link
    thread_pool = []
    stride = len(interested_links)//n_threads + 1
    lock = threading.Lock()
    for start in range(0, len(interested_links), stride):
        end = min(start+stride, len(interested_links))
        t = threading.Thread(target=parse_sites, args=(
            interested_links[start:end], lock))
        t.start()
        thread_pool.append(t)

    for t in thread_pool:
        t.join()
