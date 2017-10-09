# -*- coding: utf-8 -*-
import requests
from store import Redis
from requests import Timeout
from settings import DbNum1

rds = Redis(DbNum1)
ProxyUrl = 'http://dps.kuaidaili.com/api/getdps/?orderid=980552788062433&num=1000&format=json&sep=1'
VpsUrl = 'http://47.91.140.136:20000/get_vps_proxy?vps_name=bgbigdata1'


def get_vps_ip():
    try:
        res = requests.get(VpsUrl, timeout=10)
        if res.status_code == 200:
            return res.text
        return None
    except Timeout:
        print('Timeout')
        return None


def get_ip_list():
    res = requests.get(ProxyUrl)
    res_code = res.json()['code']
    while res_code == -51:
        res = requests.get(ProxyUrl)
        res_code = res.json()['code']
    return res.json()['data']['proxy_list']


if __name__ == '__main__':
    ip = get_vps_ip()
    print(ip)





