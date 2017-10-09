# -*- coding: utf-8 -*-
from store import Redis
from settings import DbNum1

rds = Redis(DbNum1)

word_list = ['pink', 'beautifulhalo', 'zaful', 'sammydress', 'shein', 'nastydress', 'clothes', 'skirts', 'kfashion',
             'kstyle', 'jfashion', 'pink fashion', 'fashion']


def words_to_redis(words: list):
    words_list = words[:]
    for word in words_list:
        if not rds.exists_hash_field('word', word):
            rds.set_hash('word', {word: 1})


if __name__ == '__main__':
    words_to_redis(word_list)
