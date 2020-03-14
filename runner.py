import os
import sys
from scrapy.cmdline import execute
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
from TweetScraper.spiders import TweetCrawler,ConversaCrawler
from time import sleep

SETTINGS = get_project_settings()

os.chdir(os.path.dirname(os.path.realpath(__file__)))

cmdTemplate = ['scrapy','crawl','TweetScraper']
#emojis = ['😠', '✋', '😳', '💖', '😎', '😒', '😍', '😣', '😫', '😖', '☺', '♥', '👊', '🔫', '😊', '✌', '💟', '😈', '😕', '💔', '💙', '😘', '💯', '😢', '😭', '😔', '😡', '💕', '😑', '😬', '😜', '😩', '💪', '💁', '🙅', '😪', '😋', '🙈', '😞', '😅', '👏', '👍', '🙊', '🎶', '😐', '😉', '😤', '😂', '👌', '❤', '😏', '😓', '🙏', '👀', '😷', '😁', '💜', '💀', '🙌', '😌', '🎧', '✨', '😴', '😄']


# emojis = [#'👌','😎','😔','😬','👏','🙌','😕','😋','😫','🙏','💜','🙊','😌','😴','😐','💁','💙','😑','💔','😞',
# '👊','😣','😤','💪','😈','😡', '💯','😪','✌','✨','😖','😓','😠','😷','🙅','♥', '🔫','✋','🎶','💟','🎧']
#timeLimit = ['until:2017-1-1','since:2017-1-1 until:2018-1-2','since:2018-1-1 until:2019-1-1']
timeLimit = ['since:2018-%s-1 until:2018-%s-1'%(i,i+1) for i in range(7,11)]

def run(time):
    process = CrawlerProcess(settings=SETTINGS)
    limit=timeLimit[0]
    try:
        process.crawl(TweetCrawler.TweetScraper,limit,'en')
        sleep(1)
    except SystemExit:
        pass

    process.start()

if  __name__ == "__main__":
    run(0)
    # if len(sys.argv) == 2:
    #     time_index = int(sys.argv[-1])
    #     print(sys.argv[-1])
    #     run(time_index)
    # else:
    #     execute(['scrapy','crawl','ConversaScraper'])