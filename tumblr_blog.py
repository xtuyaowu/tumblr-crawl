# -*- coding: utf-8 -*-
import requests
from settings import HEADERS, DbNum1, DbNum2
import random
from lxml import etree
import datetime
import sys
from requests import ConnectionError, RequestException
from get_proxy_ip import get_vps_ip
from store import Store, Redis
import json
import time
from collections import OrderedDict

rds = Redis(DbNum1)
rds_blog = Redis(DbNum2)
tumblr_store = Store()


def get_proxy_ip_from_redis():
    proxy_ip = rds.get_key_value('vps')
    if proxy_ip:
        proxy_ip = proxy_ip.decode('utf-8')
        proxy_ip = {'http': 'http://{}'.format(proxy_ip)}
    else:
        proxy_ip = None
    return proxy_ip


def change_vps():
    proxy_ip = get_vps_ip()
    if proxy_ip:
        rds.set_key_value('vps', proxy_ip)


def has_blogroll(page_url, blog_name):
    headers = {'User-Agent': random.choice(HEADERS)}
    try:
        resp = requests.get(page_url, headers=headers)
        if resp.status_code == 200:
            rds_blog.set_hash_field(blog_name, 'blogroll', 1)
        else:
            rds_blog.set_hash_field(blog_name, 'blogroll', 0)
    except ConnectionError:
        rds_blog.set_hash_field(blog_name, 'blogroll', 0)


def get_now_last12_month():
    today = str(datetime.date.today())
    year, month = today.split('-')[:2]
    _now_month = '{}{}'.format(year, month)   # 当月
    # 前12个月
    if int(month) == 12:
        month_last12 = 1
        year_last12 = year
    else:
        month_last12 = int(month) + 1
        year_last12 = int(year) - 1
    _last12_month = '{}{:0>2}'.format(year_last12, month_last12)
    # 前13个月
    _last13_month = '{}{:0>2}'.format(int(year)-1, month)
    return _now_month, _last12_month, _last13_month


def get_archive_page(blog_name, page_url):
    ip = get_proxy_ip_from_redis()
    headers = {'User-Agent': random.choice(HEADERS)}
    try:
        resp = requests.get(page_url, headers=headers, proxies=ip, timeout=30)
        if resp.status_code == 200:
            resp.encoding = 'utf-8'
            parse_html(blog_name, resp.text)
        elif resp.status_code == 404:
            rds.remove_member('blog', blog_name)
            rds.remove_member('crawl_blog', blog_name)
            rds_blog.delete_key(blog_name)
            rds_blog.add_set('error_blog', blog_name)
        else:
            print(resp.status_code)
    except ConnectionError:
        rds.remove_member('blog', blog_name)
        rds.remove_member('crawl_blog', blog_name)
        rds_blog.delete_key(blog_name)
        rds_blog.add_set('error_blog', blog_name)
    except RequestException:
        print('Change Vps')
        change_vps()


def set_update_blog(blog_name):
    first_blog = rds_blog.get_hash_field(blog_name, 'first_blog')
    if first_blog:
        first_blog = first_blog.decode('utf-8')
        rds_blog.set_hash_field(blog_name, 'update_blog', first_blog)


def parse_html(blog_name, html):
    sel = etree.HTML(html)
    sections = sel.xpath('//section[contains(@id, "posts")]')
    for section in sections:
        month = section.xpath('./@id')[0].split('_')[-1]
        print(month)
        last_update_month = rds_blog.get_hash_field(blog_name, 'update_month')
        if last_update_month:
            if int(month) < int(last_update_month):
                rds_blog.set_hash_field(blog_name, 'update_month', now_month)
                set_update_blog(blog_name)
                print('{}: has finished updating'.format(blog_name))
                break
        last_blog = rds_blog.get_hash_field(blog_name, 'update_blog')
        if last_blog:
            last_blog = last_blog.decode('utf-8')
        if month == now_month:   # 判断有无当月博客，有则记录当月最新一条博客
            first_blog = section.xpath('./div/@id')[0].strip()
            rds_blog.set_hash_field(blog_name, 'first_blog', first_blog)
        if int(month) < int(last12_month):   # 判断近12个月的是否采集完
            rds_blog.set_hash_field(blog_name, 'update_month', now_month)
            rds_blog.delete_hash_field(blog_name, 'before_time')
            set_update_blog(blog_name)
            print('{}: Recently 12 months has finished crawling'.format(blog_name))
            break
        else:
            post_num = 0
            notes_num = 0
            is_original = 0
            items = section.xpath('./div')
            for item in items:
                blog_id = item.xpath('./@id')[0].strip()
                if blog_id == last_blog:
                    rds_blog.set_hash_field(blog_name, 'update_month', now_month)
                    set_update_blog(blog_name)
                    print('{}: has finished updating'.format(blog_name))
                    break
                post_num += 1
                notes = item.xpath('.//span[@class="post_notes"]/@data-notes')
                if notes:
                    notes_num += int(notes[0])
                div_class = item.xpath('./@class')
                if div_class:
                    div_class = div_class[0]
                    if 'is_original' in div_class:
                        is_original += 1
            if rds_blog.exists_hash_field(blog_name, month):
                last_result = rds_blog.get_hash_field(blog_name, month).decode('utf-8')
                last_post_num, last_notes_num, last_is_original = last_result.split('/')
                post_num += int(last_post_num)
                notes_num += int(last_notes_num)
                is_original += int(last_is_original)
            result = '{}/{}/{}'.format(post_num, notes_num, is_original)
            rds_blog.set_hash_field(blog_name, month, result)
    else:
        next_page = sel.xpath('//a[@id="next_page_link"]/@href')
        if next_page:
            next_page_url = 'http://{}.tumblr.com{}'.format(blog_name, next_page[0])
            rds_blog.set_hash_field(blog_name, 'before_time', next_page_url)
        else:
            rds_blog.set_hash_field(blog_name, 'update_month', now_month)
            rds_blog.delete_hash_field(blog_name, 'before_time')
            set_update_blog(blog_name)
            print('{}: Recently 12 months has finished crawling'.format(blog_name))
    if rds_blog.exists_hash_field(blog_name, 'update_month'):
        rds_blog.delete_hash_field(blog_name, last13_month)
        tumblr_insert_update(blog_name)
        rds.remove_member('blog', blog_name)
        rds.remove_member('crawl_blog', blog_name)
        rds.add_set('finish_blog', blog_name)


def tumblr_insert_update(blog):
    d = OrderedDict()
    word = rds_blog.get_hash_field(blog, 'word').decode('utf-8')
    blogroll = rds_blog.get_hash_field(blog, 'blogroll').decode('utf-8')
    keys = rds_blog.get_hash_all_keys(blog)
    month_list = [key.decode('utf-8') for key in keys if key.isdigit()]
    month_list.sort(reverse=True)
    for month in month_list:
        d[month] = rds_blog.get_hash_field(blog, month).decode('utf-8')
    raw_data = json.dumps(d)
    crawl_time = time.strftime('%Y-%m-%d', time.localtime(time.time()))
    create_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
    if rds_blog.exists_hash_field(blog, 'insert'):
        tumblr_store.update(word, raw_data, blogroll, crawl_time, create_time, blog)
    else:
        tumblr_store.insert(blog, word, raw_data, blogroll, crawl_time, create_time)
        rds_blog.set_hash_field(blog, 'insert', 1)


if __name__ == '__main__':
    if not rds.exists_key('vps'):
        ip = get_vps_ip()
        if ip:
            rds.set_key_value('vps', ip)
        else:
            print('Redis has no vps key')
            sys.exit()
    now_month, last12_month, last13_month = get_now_last12_month()
    try:
        while rds.exists_key('blog'):
            blog_title = rds.get_members('blog', 1)[0].decode('utf-8')
            rds.add_set('crawl_blog', blog_title)
            archive_page = 'http://{}.tumblr.com/archive'.format(blog_title)
            blogroll_page = 'http://{}.tumblr.com/Blogroll'.format(blog_title)
            print('{}'.format(blogroll_page))
            has_blogroll(blogroll_page, blog_title)
            while rds.is_member('blog', blog_title):
                before_time = rds_blog.get_hash_field(blog_title, 'before_time')
                if before_time:
                    url = before_time.decode('utf-8')
                else:
                    url = archive_page
                get_archive_page(blog_title, url)
    except KeyboardInterrupt:
        print('KeyboardInterrupt')

