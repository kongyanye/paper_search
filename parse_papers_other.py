import pickle
from bs4 import BeautifulSoup

from utils import Config, safe_pickle_dump, requests_get

def parse_acm(page_soup):
    title = page_soup.find('h1').text
    published = page_soup.find('span', {'class':'epub-section__date'}).text.strip()
    summary = page_soup.find('div', {'class':'abstractSection abstractInFull'}).text.strip()
    author = []
    for a in page_soup.find('ul',{'ariaa-label':'authors'}).find_all('a',{'class':'author-name'}):
        author.append(a.attrs['title'])
    author_info = []
    for ai in page_soup.find_all('span',{'class':'info--text auth-institution'}):
        author_info.append(ai.text.strip())
    category = page_soup.find('span', {'class':'epub-section__title'}).text.strip()
    
    r = {}
    r['title'] = title
    r['published'] = published
    r['summary'] = summary
    r['author'] = author
    r['author_info'] = author_info
    r['category'] = category
    return r
    
def parse_ieee(page_soup):
    title = page_soup.find('h1').find('span').text.strip()

def parse_one(link):
    page_con = requests_get(link)
    url = page_con.url
    page_soup = BeautifulSoup(page_con.text, 'html.parser')
    if 'acm.org' in url:
        r = parse_acm(page_soup)
        r['url'] = url
    elif 'ieee.org' in url:

    return r

if __name__ == '__main__':
    with open(Config.todo_paper_link, 'r') as f:
        todo = f.readlines()
    todo = [each.strip() for each in todo]
    print('%d papers in todo list')

