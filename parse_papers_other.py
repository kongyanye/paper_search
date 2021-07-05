import pickle
from bs4 import BeautifulSoup
import re
import json
import threading
import time
from datetime import datetime
import numpy as np
from urllib.parse import unquote
import sys

from utils import Config, safe_pickle_dump, requests_get

sys.setrecursionlimit(10000)

failed = []
skipped = []

def wrap_color(text, color):
    if color == 'red':
        return f'\033[1;031m{text}\033[0m'
    elif color == 'green':
        return f'\033[1;032m{text}\033[0m'
    elif color == 'orange':
        return f'\033[1;033m{text}\033[0m'
    elif color == 'purple':
        return f'\033[1;035m{text}\033[0m'
    elif color == 'yellow':
        return f'\033[1;093m{text}\033[0m'
    else:
        return ' [UNKNOWN COLOR %s] '%color + text

def parse_acm(page_soup):
    title = page_soup.find('h1').text
    published = page_soup.find(
        'span', {'class': 'epub-section__date'}).text.strip()
    tmp = page_soup.find('div', {'class': 'abstractSection abstractInFull'})
    if tmp:
        summary = tmp.text.strip()
    else:
        summary = ''
    author = []
    for a in page_soup.find('ul', {'ariaa-label': 'authors'}).find_all('a', {'class': 'author-name'}):
        author.append(a.attrs['title'])
    affiliation = []
    for a in page_soup.find_all('span', {'class': 'info--text auth-institution'}):
        affiliation.append(a.text.strip())
    conf_full_name = page_soup.find(
        'span', {'class': 'epub-section__title'}).text.strip()

    r = {}
    r['title'] = title
    r['published'] = published
    if summary:
        r['summary'] = summary
    r['author'] = author
    r['affiliation'] = affiliation
    r['conf_full_name'] = conf_full_name
    return r


def parse_ieee(page_con):
    tmp = re.search('xplGlobal.document.metadata=(.*});', page_con.text)
    if not tmp:
        return {}
    meta = json.loads(tmp.groups()[0])
    title = meta['displayDocTitle']
    if 'journalDisplayDateOfPublication' in meta:
        published = meta['journalDisplayDateOfPublication']
    else:
        published = meta['conferenceDate']
    
    summary = ''
    if 'abstract' in meta:
        summary = meta['abstract']
    if 'authors' not in meta:
        return {}
    else:
        author = [each['name'] for each in meta['authors']]
    affiliation = [each['affiliation'] for each in meta['authors'] if 'affiliation' in each ]
    conf_full_name = meta['displayPublicationTitle']

    r = {}
    r['title'] = title
    r['published'] = published
    if summary: r['summary'] = summary
    r['author'] = author
    r['affiliation'] = affiliation
    r['conf_full_name'] = conf_full_name
    return r


def parse_springer(page_soup):
    title = page_soup.find('h1').text
    published = ''
    if page_soup.find('time'):
        published = page_soup.find('time').text.strip()
    summary = page_soup.find('h2').find_next_sibling().text
    author = []
    for a in page_soup.find_all('span', {'class': 'authors__name'}):
        author.append(' '.join(a.text.split()))
    affiliation = []
    for a in page_soup.find_all('span', {'class': 'affiliation__name'}):
        affiliation.append(a.text)
    tmp = page_soup.find('span', {'class': 'BookTitle'})
    if tmp:
        conf_full_name = tmp.find('a').text
    else:
        conf_full_name = page_soup.find('meta',{'name':'citation_journal_title'}).attrs['content']

    r = {}
    r['title'] = title
    if published: r['published'] = published
    r['summary'] = summary
    r['author'] = author
    r['affiliation'] = affiliation
    r['conf_full_name'] = conf_full_name
    return r

def parse_elsevier(page_con): # elsevier is sciencedirect
    page_soup = BeautifulSoup(page_con.text, 'html.parser')
    title = page_soup.find('span', {'class':'title-text'}).text

    tmp = re.search('({"abstracts":.*}})', page_con.text)
    if not tmp:
        return {}
    data = json.loads(tmp.groups()[0])
    author = []
    affiliation = []
    for each in data['authors']['content'][0]['$$']:
        if each['#name'] == 'author':
            given_name = each['$$'][0]['_']
            surname = each['$$'][1]['_']
            author.append(given_name + ' ' + surname)
        elif each['#name'] == 'affiliation':
            for t in each['$$']:
                if t['#name'] == 'textfn':
                    affiliation.append(t['_'])

    summary = page_soup.find('div',{'id':'abstracts'}).find('p').text
    published = page_soup.find('meta', {'name':'citation_publication_date'}).attrs['content']
    conf_full_name = page_soup.find('a',{'class':'publication-title-link'}).text

    r = {}
    r['title'] = title
    r['published'] = published
    r['summary'] = summary
    r['author'] = author
    r['affiliation'] = affiliation
    r['conf_full_name'] = conf_full_name
    return r
    
def parse_aaai(page_soup):
    title = page_soup.find('div', {'id': 'title'})
    if not title: # if we can't find the title, that means the paper is tooooooo old, skip it
        return {}
    title = title.text
    published = page_soup.find('blockquote').text.strip().split()[-1]
    summary = page_soup.find('div', {'id': 'abstract'}).find('div').text
    author = page_soup.find('div', {'id': 'author'}).find('em').text.split(',')
    author = [each.strip() for each in author]

    # the author info is in another page
    current_url = page_soup.find('a', {'class': 'current'}).attrs['href']
    author_url = current_url.replace('paper', 'rt').replace('view', 'bio')
    author_page = requests_get(author_url)
    affiliation = []
    if author_page:
        author_soup = BeautifulSoup(author_page.text, 'html.parser')
        
        for each in author_soup.find_all('div', {'id': 'author'}):
            tmp = each.find('p').text.split('\t')
            if len(tmp) >= 3:
                affiliation.append(each.find('p').text.split('\t')[2])
    
    conf_full_name = ' '.join(page_soup.find('h2').text.split())

    r = {}
    r['title'] = title
    r['published'] = published
    r['summary'] = summary
    r['author'] = author
    if affiliation: r['affiliation'] = affiliation
    r['conf_full_name'] = conf_full_name
    return r


def parse_ijcai(page_soup):
    # there are several versions of IJCAI pages
    try: # e.g., https://www.ijcai.org/Abstract/15/008
        ps = page_soup.find_all('p')
        title, author = ps[0].text.split('\n')[:2]
        title = title.split('/')[0].strip()
        author = [each.strip() for each in author.split(',')]
        try:
            published = int(ps[0].find('a').attrs['href'].split('/')[2])
        except:
            published = int(page_soup.find('div', {'class': 'content'}).find_all('p')[-1].find('a').attrs['href'].split('/')[2])
        if published >= 69:
            published += 1900
        else:
            published += 2000
        summary = ps[1].text
        conf_full_name = page_soup.find(
            'div', {'class': 'content'}).find('title').text
    except: # e.g., https://www.ijcai.org/proceedings/2017/78
        conf_full_name = page_soup.find('h1').text.strip().replace('\n', ' ')
        title = page_soup.find('h1',{'class':'page-title'}).text
        author = page_soup.find('h2').text.split(', ')
        summary = page_soup.find('div',{'class':'col-md-12'}).text.strip()
        published = page_soup.find('meta',{'name':'citation_publication_date'}).attrs['content']

    r = {}
    r['title'] = title
    r['published'] = str(published)
    r['summary'] = summary
    r['author'] = author
    r['conf_full_name'] = conf_full_name
    return r


def parse_nips(page_soup):
    title = ''
    published = ''
    d = page_soup.find('div', {'class': 'col'}).children
    while True:
        e = next(d)
        if not e:
            continue
        name = e.name
        if not title and name == 'h4':
            title = e.text
            continue
        if not published and name == 'p':
            published = e.find('a')['href'].split('/')[-1]
            conf_full_name = e.find('a').text
            continue
        if name == 'h4' and e.text == 'Authors':
            t = next(d)
            t = next(d)
            author = [each.strip() for each in t.find('i').text.split(',')]
            continue
        if name == 'h4' and e.text == 'Abstract':
            t = next(d)
            t = next(d)
            summary = t.find('p')
            if summary:
                summary = summary.text
            break

    r = {}
    r['title'] = title
    r['published'] = published
    if summary: r['summary'] = summary
    r['author'] = author
    r['conf_full_name'] = conf_full_name
    return r


def parse_pmlr(page_soup):
    title = page_soup.find('h1').text
    *tmp, conf_full_name, published = page_soup.find(
        'div', {'id': 'info'}).text.split('\xa0')
    conf_full_name = conf_full_name.split(' ')[0]
    published = published.split('.')[0]
    summary = page_soup.find('div', {'id': 'abstract'}).text.strip()
    author = page_soup.find('span', {'class': 'authors'}).text.split(',\xa0')

    r = {}
    r['title'] = title
    r['published'] = published
    r['summary'] = summary
    r['author'] = author
    r['conf_full_name'] = conf_full_name
    return r


def parse_usenix(page_soup):
    # multiple versions for usenix.org
    title = page_soup.find('h2').text.replace('\n', ' ')

    try:
        d = page_soup.find('body').find('p').children
        author = ''
        affiliation = []
        summary = ''
        while True:
            e = next(d)
            if not e or e == '\n' or e.name == 'br':
                continue
            if not author:
                author = e.strip().split(' and ')
                continue
            if not summary and '@' not in e and e.name != 'h4':
                affiliation.append(e.strip())
                continue
            if e.name == 'h4' and e.text == 'Abstract':
                summary = next(d).strip().replace('\n', ' ')
                break

        _, conf_full_name, published = page_soup.find('title').text.split(', ')

    except: # https://www.usenix.org/legacy/events/usenix06/tech/albrecht.html
        e = [each for each in page_soup.find_all('font')[1].text.split('\n\n') if each]
        author, summary = e[1].split('Abstract')
        author = author.strip().split(', ')
        summary = summary.replace('\n', ' ')
        conf_full_name = page_soup.find_all('font')[0]
        published = page_soup.find_all('table')[-1].text.strip().split(': ')[-1]
        
    r = {}
    r['title'] = title
    r['published'] = published
    r['summary'] = summary
    r['author'] = author
    r['conf_full_name'] = conf_full_name
    return r


def parse_cvpr(page_soup):
    title = page_soup.find('div', {'id': 'papertitle'}).text.strip()
    author, published = page_soup.find(
        'div', {'id': 'authors'}).text.strip().split('; ')
    author = author.split(', ')
    conf_full_name, published, _ = published.split(', ')
    summary = page_soup.find('div', {'id': 'abstract'}).text.strip()

    r = {}
    r['title'] = title
    r['published'] = published
    r['summary'] = summary
    r['author'] = author
    r['conf_full_name'] = conf_full_name
    return r


def parse_jmlr(page_soup):
    title = page_soup.find('h2').text
    author, published = page_soup.find(
        'div', {'id': 'content'}).find('p').text.split('; ')
    author = author.split(', ')
    published = published.split(', ')[1].replace('.', '')
    summary = re.search('Abstract(.*)\[abs\]', page_soup.find(
        'div', {'id': 'content'}).text.replace('\n', '')).groups()[0]

    r = {}
    r['title'] = title
    r['published'] = published
    r['summary'] = summary
    r['author'] = author
    r['conf_full_name'] = 'jmlr'
    return r


def parse_one(link):
    # grep -oP '(?<=)http(s)?://.*?(?=/)' todo.txt | sort | uniq -c | sort -s -k 1,1
    if link.endswith('.pdf'):
        #print(f"Skipping {wrap_color(link, 'orange')}, ends with pdf")
        skipped.append(link)
        return {}
    page_con = requests_get(link)
    if not page_con: 
        print(f"Failed {wrap_color(link, 'red')}, no page content fetched")
        failed.append(link)
        return {} # the page may be failed to fetch
    url = page_con.url
    page_soup = BeautifulSoup(page_con.text, 'html.parser')
    if 'acm.org' in url:
        r = parse_acm(page_soup)
    elif 'ieee.org' in url:
        r = parse_ieee(page_con)
    elif 'springer.com' in url:
        r = parse_springer(page_soup)
    elif 'elsevier.com' in url: # the link redirects to sciencedirect.com
        link = page_soup.find('input',{'name':'redirectURL'}).attrs['value']
        link = unquote(link)
        page_con = requests_get(link)
        if not page_con: 
            print(f"Failed {wrap_color(link, 'red')}, no page content fetched")
            failed.append(link)
            return {} # the page may be failed to fetch
        url = page_con.url
        r = parse_elsevier(page_con)
    elif 'aaai.org' in url:
        # aaai.org pages need to explicitly use https and switch to a non-frame version of the page
        if not page_soup.contents:
            link = link.replace('http', 'https').replace('view', 'viewPaper')
            page_con = requests_get(link)
            if not page_con: # the page may be failed to fetch
                print(f"Failed {wrap_color(link, 'red')}, no aaai page content fetched")
                return {} 
            url = page_con.url
            page_soup = BeautifulSoup(page_con.text, 'html.parser')
        r = parse_aaai(page_soup)
    elif 'ijcai.org' in url:
        r = parse_ijcai(page_soup)
    elif 'neurips.cc' in url:
        r = parse_nips(page_soup)
    elif 'mlr.press' in url:
        r = parse_pmlr(page_soup)
    elif 'usenix.org' in url:
        r = parse_usenix(page_soup)
    elif 'thecvf.com' in url:
        r = parse_cvpr(page_soup)
    elif 'jmlr.org' in url:
        r = parse_jmlr(page_soup)
    else:
        print(f"Skipping {wrap_color(link, 'orange')} -> {wrap_color(url, 'orange')}, unknown site category")
        skipped.append(link)
        return {}
    if not r:
        print(f"Failed {wrap_color(link, 'red')}, empty parse result")
        failed.append(link)
        return {}
    else:
        r['url'] = url
        return r


def parse(links, db, filepath, lock):
    fetched = 0
    last = datetime.now().timestamp()
    wait_time = np.random.randint(60,nthreads*180)
    if not isinstance(links, list):
        links = [links]
    for link in links:
        if link in db:
            continue
        try:
            r = parse_one(link)
            if r:
                print(f"succcess with {wrap_color(link, 'purple')}")
                with lock:
                    db[link] = r
                fetched += 1
        except Exception as e:
            print(f"ERROR parsing {wrap_color(link, 'red')}, {e}")
            failed.append(link)
        if fetched >= 20 and datetime.now().timestamp()-last >= wait_time:
            with lock:
                print(wrap_color(f'Saving database with {len(db)} papers to {filepath}', 'green'))
                safe_pickle_dump(db, filepath)
            fetched = 0
            last = datetime.now().timestamp()
        time.sleep(0.1)
    with lock:
        safe_pickle_dump(db, filepath)

if __name__ == '__main__':
    # read in todo list
    with open(Config.todo_paper_link, 'r') as f:
        todo = f.readlines()
    todo = [each.strip() for each in todo]
    print('%d papers in todo list' % len(todo))

    # shuffle for random start
    np.random.shuffle(todo)

    # read in local database to save
    filepath = Config.db_other_dir + 'db' + '.p'
    # lets load the existing database to memory
    try:
        db = pickle.load(open(filepath, 'rb'))
        print('found %d entries in databse at start'%len(db))
    except Exception as e:
        print('error loading existing database:')
        print(e)
        print('starting from an empty database')
        db = {}

    # main
    nthreads = 50
    stride = len(todo)//nthreads + 1
    thread_pool = []
    lock = threading.Lock()
    for start in range(0, len(todo), stride):
        end = min(start+stride, len(todo))
        t = threading.Thread(target=parse, args=(
            todo[start:end], db, filepath, lock))
        thread_pool.append(t)
        t.start()

    for t in thread_pool:
        t.join()

    with open(Config.db_other_dir + 'failed_todo.txt', 'w') as f:
        for each in failed:
            f.write(each + '\n')

    with open(Config.db_other_dir + 'skipped_todo.txt', 'w') as f:
        for each in skipped:
            f.write(each + '\n')