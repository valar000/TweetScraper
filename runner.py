import os
import sys
from scrapy.cmdline import execute
from scrapy.crawler import CrawlerProcess
from TweetScraper.spiders import TweetCrawler
from time import sleep
import datetime
from multiprocessing import Process, freeze_support, set_start_method
import random
import signal
from scrapy.utils.project import get_project_settings
from scrapy.cmdline import execute
import json
import time
import subprocess
SETTINGS = get_project_settings()



#emojis = ['😠', '✋', '😳', '💖', '😎', '😒', '😍', '😣', '😫', '😖', '☺', '♥', '👊', '🔫', '😊', '✌', '💟', '😈', '😕', '💔', '💙', '😘', '💯', '😢', '😭', '😔', '😡', '💕', '😑', '😬', '😜', '😩', '💪', '💁', '🙅', '😪', '😋', '🙈', '😞', '😅', '👏', '👍', '🙊', '🎶', '😐', '😉', '😤', '😂', '👌', '❤', '😏', '😓', '🙏', '👀', '😷', '😁', '💜', '💀', '🙌', '😌', '🎧', '✨', '😴', '😄']


def timeGen(step=4,start = datetime.datetime(2013, 9, 1),end = datetime.datetime(2016, 1, 1)):
    step = datetime.timedelta(days=step)
    i=start
    while i < end:
        out = []
        tmp=i
        while  tmp<=i+step:
            out.append(tmp.strftime('%Y-%m-%d'))
            tmp+=datetime.timedelta(days=1)
        yield ' '.join(out)
        i+=step

SQUID_BIN_PATH = '/usr/sbin/squid'  # mac os '/usr/local/sbin/squid'
SQUID_CONF_PATH = '/etc/squid/squid.conf'  # mac os '/usr/local/etc/squid.conf'
SQUID_TEMPLATE_PATH = '/etc/squid/squid.conf.backup'  # mac os /usr/local/etc/squid.conf.backup
default_conf_detail = "cache_peer {} parent {} 0 no-query weighted-round-robin weight=1 " \
                        "connect-fail-limit=1 allow-miss max-conn=5 name=proxy-{}"
other_confs = ['request_header_access Via deny all', 'request_header_access X-Forwarded-For deny all',
                'request_header_access From deny all', 'never_direct allow all']
def update_proxy():
    import  requests
    try:
        url='http://localhost:8899/api/v1/proxies/?limit=500&anonymous=true&https=true&countries=US'
        r=requests.get(url)
        with open(SQUID_TEMPLATE_PATH, 'rt') as fr, open(SQUID_CONF_PATH, 'wt') as fw:
            original_conf = fr.read()
            fw.write(original_conf)
            for index,i in enumerate(r.json()['proxies']):
                ip = str(i['ip'])
                port=str(i['port'])
                print(default_conf_detail.format(ip, port, index),file=fw)
            for i in other_confs:
                print(i,file=fw)
    except Exception as e: print(e)
    finally:
        subprocess.Popen([SQUID_BIN_PATH, '-k', 'reconfigure'])

    
def run(limit):
    execute(["scrapy", "crawl", "TweetScraper",
            "-a", "limit={}".format(limit),
            "-a", "lang={}".format('en')])
    return 0

def waitePool(pool,num):
    start=time.perf_counter()
    while len(pool)>=num:
        end=time.perf_counter()
        sleep(1)
        #update proxy 1hour
        if end-start > 60*60*3:
            start=end
            update_proxy()
        delItem=[]
        for i in pool:
            if not i.is_alive():delItem.append(i)
        if len(delItem)>0:
            for i in delItem:
                pool.remove(i)

if  __name__ == "__main__":
    subprocess.Popen(["man","-N","-d1"])
    os.chdir(os.path.dirname(os.path.realpath(__file__)))
    import pymongo
    try:
        sleep(120)   
        connection = pymongo.MongoClient(SETTINGS['PIPELINE_MONGO_URI '])
        db = connection[SETTINGS['PIPELINE_MONGO_DATABASE']]
        collect=db[SETTINGS['PIPELINE_MONGO_COLLECTION']]
        index=[i  for i in collect.list_indexes() if 'ID' in i['name'] ]
        if index==[]:collect.create_index(SETTINGS['MONGO_INDEXES'])
        connection.close()
    except:pass
    num=1
    try:num=int(sys.argv[-1])
    except:pass

    update_proxy()
    sleep(2)
    pool=[]
    for i in timeGen(step=30):
        t=Process(target=run, args=(i,))
        waitePool(pool,num)
        print(i)
        t.start()
        pool.append(t)
        

    # run(list(timeGen(5))[0])
