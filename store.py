# -*- coding: utf-8 -*-
from settings import MYSQL_CONFIG_LOCAL, MYSQL_CONFIG_SERVER
import sys
import traceback
import pymysql
import redis


class Redis:
    def __init__(self, db):
        try:
            #self.rds = redis.StrictRedis(host='localhost', port=6379, db=db)
            self.rds = redis.StrictRedis(host='47.91.104.136', port=6361, db=db)
        except:
            traceback.print_exc()

    def count_keys(self):   # 查询当前库里有多少key
        rds = self.rds
        return rds.dbsize()

    # key操作
    def get_random_key(self):  # 从当前数据库随机返回一个key， 没有返回None， 不删除key
        rds = self.rds
        return rds.randomkey()

    def exists_key(self, key):
        rds = self.rds
        return rds.exists(key)

    def delete_key(self, key):
        rds = self.rds
        rds.delete(key)

    def get_all_keys(self):
        rds = self.rds
        return rds.keys()

    def move_keys(self, key, num):   # 如果移入的库中已有此key，则无法移入
        rds = self.rds
        rds.move(key, num)

    # String操作
    def set_key_value(self, key, value):
        rds = self.rds
        rds.set(key, value)

    def get_key_value(self, key):   # 没有对应key返回None
        rds = self.rds
        return rds.get(key)

    # Hash操作
    def set_hash(self, key, mapping):   # mapping为字典, 已存在key会覆盖mapping
        rds = self.rds
        rds.hmset(key, mapping)

    def set_hash_field(self, key, field, value):  # 将哈希表key中的字段field的值设为 value
        rds = self.rds
        rds.hset(key, field, value)

    def delete_hash_field(self, key, field):   # 删除hash表中某个字段，无论字段是否存在
        rds = self.rds
        rds.hdel(key, field)

    def exists_hash_field(self, key, field):   # 检查hash表中某个字段存在
        rds = self.rds
        return rds.hexists(key, field)

    def get_hash_field(self, key, field):   # 获取hash表中指定字段的值, 没有返回None
        rds = self.rds
        return rds.hget(key, field)

    def get_hash_all_field(self, key):   # 获取hash表中指定key所有字段和值,以字典形式，没有key返回空字典
        rds = self.rds
        return rds.hgetall(key)

    def get_hash_all_keys(self, key):   # 获取hash表中指定key所有字段
        rds = self.rds
        return rds.hkeys(key)

    def increase_hash_field(self, key, field, increment):   # 为hash表key某个字段的整数型值增加increment
        rds = self.rds
        rds.hincrby(key, field, increment)

    # List操作
    def push_into_lst(self, key, value):  # url从左至右入列
        rds = self.rds
        rds.rpush(key, value)

    def pop_lst_item(self, key):  # 从左至右取出列表第一个元素(元组形式)，并设置超时，超时返回None
        rds = self.rds
        return rds.blpop(key, timeout=5)

    # Set操作
    def add_set(self, key, value):
        rds = self.rds
        rds.sadd(key, value)

    def is_member(self, key, value):
        rds = self.rds
        return rds.sismember(key, value)

    def pop_member(self, key):  # 随机移除一个值并返回该值,没有返回None
        rds = self.rds
        return rds.spop(key)

    def get_members(self, key, num):  # 随机取出num个值（非移除），列表形式返回这些值，没有返回空列表
        rds = self.rds
        return rds.srandmember(key, num)

    def remove_member(self, key, value):   # 移除集合中指定元素
        rds = self.rds
        rds.srem(key, value)

    def get_all_members(self, key):   # 返回集合中全部元素,不删除
        rds = self.rds
        return rds.smembers(key)

    def remove_into(self, key1, key2, value):   # 把集合key1中value元素移入集合key2中
        rds = self.rds
        rds.smove(key1, key2, value)

    def count_members(self, key):   # 计算集合中成员数量
        rds = self.rds
        return rds.scard(key)


class Store:
    def __init__(self):
        try:
            #self.conn = pymysql.connect(**MYSQL_CONFIG_LOCAL)
            self.conn = pymysql.connect(**MYSQL_CONFIG_SERVER)
        except:
            traceback.print_exc()
            sys.exit()

    def insert(self, title, search_word, raw_data, blogroll, crawl_time, create_time):
        sql = "insert into scb_crawler_tumblr(sct_title, sct_search_word, sct_raw_data, sct_blogroll, sct_crawl_time," \
              "sct_create_time)values(%s,%s,%s,%s,%s,%s)"
        cur = self.conn.cursor()
        cur.execute(sql, (title, search_word, raw_data, blogroll, crawl_time, create_time))
        self.conn.commit()

    def update(self, search_word, raw_data, blogroll, crawl_time, create_time, title):
        sql = "update scb_crawler_tumblr set sct_search_word=%s, sct_raw_data=%s, sct_blogroll=%s, sct_crawl_time=%s, " \
              "sct_create_time=%s where sct_title=%s"
        cur = self.conn.cursor()
        cur.execute(sql, (search_word, raw_data, blogroll, crawl_time, create_time, title))
        self.conn.commit()

    def update_without_time(self, raw_data, title):
        sql = "update scb_crawler_tumblr set sct_raw_data=%s where sct_title=%s"
        cur = self.conn.cursor()
        cur.execute(sql, (raw_data, title))
        self.conn.commit()

    def close(self):
        cursor = self.conn.cursor()
        cursor.close()
        self.conn.close()


if __name__ == '__main__':
    pass
