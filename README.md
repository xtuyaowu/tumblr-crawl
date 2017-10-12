# tumblr-crawl
## 简介
在tumblr上根据关键字搜索出相关博主，采集这些博主的近12个月的总post数，总notes，总original数。
## 文件说明
* get_proxy_ip.py: 获取vps
* settings.py: 相关设置信息
* store.py: mysql、redis相关函数
* tumblr_list.py: 依据关键词采集相关博主
* tumblr_blog.py: 采集博主近12个月总post数，总notes，总original数
* tumblr_words_to_redis.py: 搜索关键词入redis队列
## 依赖库
    pip install -r requirements.txt
## 运行程序
    python tumblr_words_to_redis.py
    python tumblr_list.py
    python tumblr_blog.py
## 备注
本项目在windows10下开发完成，使用python3.6、mysql5.7、redis3.0
