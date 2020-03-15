import os
import sys
from scrapy.cmdline import execute
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
from TweetScraper.spiders import TweetCrawler
from time import sleep
import datetime
from multiprocessing import Process, freeze_support, set_start_method
import random
import signal
SETTINGS = get_project_settings()

os.chdir(os.path.dirname(os.path.realpath(__file__)))

#emojis = ['😠', '✋', '😳', '💖', '😎', '😒', '😍', '😣', '😫', '😖', '☺', '♥', '👊', '🔫', '😊', '✌', '💟', '😈', '😕', '💔', '💙', '😘', '💯', '😢', '😭', '😔', '😡', '💕', '😑', '😬', '😜', '😩', '💪', '💁', '🙅', '😪', '😋', '🙈', '😞', '😅', '👏', '👍', '🙊', '🎶', '😐', '😉', '😤', '😂', '👌', '❤', '😏', '😓', '🙏', '👀', '😷', '😁', '💜', '💀', '🙌', '😌', '🎧', '✨', '😴', '😄']


def timeGen(step=4,start = datetime.datetime(2016, 1, 1),end = datetime.datetime(2017, 1, 1)):
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
    from scrapy.cmdline import execute
    execute(["scrapy", "crawl", "TweetScraper",
            "-a", "limit={}".format(limit),
            "-a", "lang={}".format('en')]) 
    print("INFO: "+limit)
    return 0

if  __name__ == "__main__":
    num=1
    try:num=int(sys.argv[-1])
    except:pass
    sleep(120)
    from multiprocessing import get_context,Pool
    with Pool(num) as p:
        p.map(run,timeGen())
    # run(list(timeGen())[0])
