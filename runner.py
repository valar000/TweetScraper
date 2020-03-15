import os
import sys
from scrapy.cmdline import execute
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
from TweetScraper.spiders import TweetCrawler,ConversaCrawler
from time import sleep
from itertools import zip_longest
def grouper(iterable, n, fillvalue=None):
    "Collect data into fixed-length chunks or blocks"
    # grouper('ABCDEFG', 3, 'x') --> ABC DEF Gxx"
    args = [iter(iterable)] * n
    return zip_longest(*args, fillvalue=fillvalue)
import datetime
SETTINGS = get_project_settings()

os.chdir(os.path.dirname(os.path.realpath(__file__)))

cmdTemplate = ['scrapy','crawl','TweetScraper']
#emojis = ['😠', '✋', '😳', '💖', '😎', '😒', '😍', '😣', '😫', '😖', '☺', '♥', '👊', '🔫', '😊', '✌', '💟', '😈', '😕', '💔', '💙', '😘', '💯', '😢', '😭', '😔', '😡', '💕', '😑', '😬', '😜', '😩', '💪', '💁', '🙅', '😪', '😋', '🙈', '😞', '😅', '👏', '👍', '🙊', '🎶', '😐', '😉', '😤', '😂', '👌', '❤', '😏', '😓', '🙏', '👀', '😷', '😁', '💜', '💀', '🙌', '😌', '🎧', '✨', '😴', '😄']


# emojis = [#'👌','😎','😔','😬','👏','🙌','😕','😋','😫','🙏','💜','🙊','😌','😴','😐','💁','💙','😑','💔','😞',
# '👊','😣','😤','💪','😈','😡', '💯','😪','✌','✨','😖','😓','😠','😷','🙅','♥', '🔫','✋','🎶','💟','🎧']
#timeLimit = ['until:2017-1-1','since:2017-1-1 until:2018-1-2','since:2018-1-1 until:2019-1-1']
timeLimit = ['since:2018-%s-1 until:2018-%s-1'%(i,i+1) for i in range(7,11)]

def timeGen(step_days=1,start = datetime.datetime(2016, 1, 1),end = datetime.datetime(2020, 3, 1)):

    step = datetime.timedelta(days=step_days)
    while start < end:
        yield start.strftime('%Y-%m-%d')
        start += step

def getLimit(timeGener):
    for i in grouper(timeGener,2):
        if len(i)==2: 
            timelimit ='since:%s until:%s'%i
            yield timelimit
    
def run(limit):
    process = CrawlerProcess(settings=SETTINGS)
    try:
        process.crawl(TweetCrawler.TweetScraper,limit,'en')
        sleep(1)
    except SystemExit:
        pass

    process.start()

if  __name__ == "__main__":
    num=1
    try:num=int(sys.argv[-1])
    except:pass
    sleep(60)
    from multiprocessing import Pool
    with Pool(num) as p:
        p.map(run,getLimit(timeGen()))

    # if len(sys.argv) == 2:
    #     time_index = int(sys.argv[-1])
    #     print(sys.argv[-1])
    #     run(time_index)
    # else:
    #     execute(['scrapy','crawl','ConversaScraper'])