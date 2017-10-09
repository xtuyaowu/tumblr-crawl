# -*- coding: utf-8 -*-
from settings import HEADERS, DbNum1, DbNum2
from store import Redis


rds = Redis(DbNum1)
rds_blog = Redis(DbNum2)

if __name__ == '__main__':
    ll = rds_blog.get_all_keys()
    print(ll)
    for l in ll:
        if not rds.is_member('finish_blog', l):
            print(l)


