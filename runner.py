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
SETTINGS = get_project_settings()

os.chdir(os.path.dirname(os.path.realpath(__file__)))

#emojis = ['😠', '✋', '😳', '💖', '😎', '😒', '😍', '😣', '😫', '😖', '☺', '♥', '👊', '🔫', '😊', '✌', '💟', '😈', '😕', '💔', '💙', '😘', '💯', '😢', '😭', '😔', '😡', '💕', '😑', '😬', '😜', '😩', '💪', '💁', '🙅', '😪', '😋', '🙈', '😞', '😅', '👏', '👍', '🙊', '🎶', '😐', '😉', '😤', '😂', '👌', '❤', '😏', '😓', '🙏', '👀', '😷', '😁', '💜', '💀', '🙌', '😌', '🎧', '✨', '😴', '😄']


def timeGen(step=4,start = datetime.datetime(2012, 1, 1),end = datetime.datetime(2016, 1, 1)):
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





    
def run(limit):
    execute(["scrapy", "crawl", "TweetScraper",
            "-a", "limit={}".format(limit),
            "-a", "lang={}".format('en')])
    return 0

def waitePool(pool,num):
    while len(pool)>=num:
        sleep(1)
        delItem=[]
        for i in pool:
            if not i.is_alive():delItem.append(i)
        if len(delItem)>0:
            for i in delItem:
                pool.remove(i)

if  __name__ == "__main__":
    num=1
    try:num=int(sys.argv[-1])
    except:pass
    sleep(120)
    pool=[]
    for i in timeGen(step=30):
        t=Process(target=run, args=(i,))
        waitePool(pool,num)
        print(i)
        t.start()
        pool.append(t)
        

    # run(list(timeGen(5))[0])
