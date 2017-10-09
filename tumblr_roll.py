# -*- coding: utf-8 -*-
import requests
from settings import HEADERS, DbNum1, DbNum2
import random
from lxml import etree
from store import Redis
import re


rds = Redis(DbNum1)
rds_blog = Redis(DbNum2)


def parse_page(html):
    sel = etree.HTML(html)
    roll_list = sel.xpath('//div[@class="roll"]/div/a/@href')
    for roll in roll_list:
        print(roll)
        blog_title = re.findall(r'//(.+).tumblr', roll)
        if blog_title:
            blog_title = blog_title[0]
        print(blog_title)


if __name__ == '__main__':
    url = 'http://japan-overload.tumblr.com/Blogroll'
    headers = {'User-Agent': random.choice(HEADERS)}
    resp = requests.get(url, headers=headers)
    parse_page(resp.text)
