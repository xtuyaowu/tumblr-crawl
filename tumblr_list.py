# -*- coding: utf-8 -*-
import requests
from settings import HEADERS, DbNum1, DbNum2
import random
from lxml import etree
import re
from store import Redis
from requests import RequestException

rds = Redis(DbNum1)
rds_blog = Redis(DbNum2)
url = 'https://www.tumblr.com/'
post_url = 'https://www.tumblr.com/search/fashion/post_page/2'


def get_key():
    headers = {'User-Agent': random.choice(HEADERS)}
    res = requests.get(url, headers=headers)
    if res.status_code == 200:
        sel = etree.HTML(res.text)
        key = sel.xpath('//meta[@name="tumblr-form-key"]/@content')
        if key:
            rds.add_set('tumblr_form_key', key[0])
        else:
            print('Not Found Key')
            return None
    else:
        print('get_key: {}'.format(res.status_code))
        return None


def get_post_data(word: str, page_num: 'int > 0'):
        post_data = {
            'q': '{}'.format(word),
            'sort': 'top',
            'post_view': 'masonry',
            'blogs_before': '{}'.format((page_num-1)*8),
            'num_blogs_shown': '{}'.format((page_num-1)*8),
            'num_posts_shown': '20',
            'before': '26',
            'blog_page': '{}'.format(page_num),
            'safe_mode': 'true',
            'post_page': '2',
            'filter_nsfw': 'true',
            'filter_post_type': '',
            'next_ad_offset': '0',
            'ad_placement_id': '0',
            'more_blogs': 'true',
        }
        return post_data


def get_post_headers():
    key = rds.get_members('tumblr_form_key', 1)
    if key:
        headers = {'User-Agent': random.choice(HEADERS),
                   'X-tumblr-form-key': key[0].decode('utf-8'),
                   'X-Requested-With': 'XMLHttpRequest'}
        return headers
    else:
        print('No Key')
        return None


def page_post(data):
    global flag
    headers = get_post_headers()
    if headers:
        word = data['q']
        page_num = data['blog_page']
        key = headers['X-tumblr-form-key']
        resp = requests.post(post_url, headers=headers, data=data)
        if resp.status_code == 200:
            result = resp.json()["response"]["blogs_html"]
            blog_titles = re.findall("\\\/blog\\\/(.+?)&quot", result)
            if blog_titles:
                for title in blog_titles:
                    if not rds_blog.exists_key(title):
                        rds_blog.set_hash(title, {'word': word})
                        rds.add_set('blog', title)
                    else:
                        new_word = rds_blog.get_hash_field(title, 'word').decode('utf-8')
                        if word not in new_word:
                            new_word = '/'.join([word, new_word])
                            rds_blog.set_hash_field(title, 'word', new_word)
                rds.increase_hash_field('word', word, 1)
                print(word, page_num, blog_titles)
            else:
                flag = False
                print('{} has finished crawling'.format(word))
                rds.add_set('finish_word', word)
        elif resp.status_code == 403:
            print(403)
            rds.remove_member('tumblr_form_key', key)
            get_key()
        else:
            print('page_post: {}'.format(resp.status_code))
    else:
        print('No Headers')


if __name__ == '__main__':
    for i in range(3):
        get_key()
    for wd in rds.get_hash_all_keys('word'):
        wd = wd.decode('utf-8')
        if not rds.is_member('finish_word', wd):
            flag = True
            while flag:
                num = rds.get_hash_field('word', wd)
                num = int(num.decode('utf-8'))
                try:
                    page_post(get_post_data(wd, num))
                except RequestException as exc:
                    print('Raise exception: {!r}'.format(exc))
    print('All words have finished crawling')
