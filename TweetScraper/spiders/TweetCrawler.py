from scrapy.spiders import CrawlSpider, Rule
from scrapy.selector import Selector
from scrapy import http
from scrapy.shell import inspect_response  # for debugging
from scrapy.utils.project import get_project_settings
import re
import json
import time
import logging
import pymongo
from scrapy.crawler import CrawlerProcess
from scrapy.exceptions import CloseSpider
from functools import partial
import os
from bson.objectid import ObjectId
SETTINGS = get_project_settings()
try:
    from urllib import quote  # Python 2.X
except ImportError:
    from urllib.parse import quote  # Python 3+
import random
from datetime import datetime
import subprocess
from TweetScraper.items import Tweet, User,Conversa
from txmongo.collection import Collection
from txmongo.connection import ConnectionPool
from txmongo.database import Database
from txmongo.filter import sort as txsort
from twisted.internet.defer import inlineCallbacks
emojis =  ['😠', '✋', '😳', '💖', '😎', '😒', '😍', '😣', '😫', '😖', '☺', '♥', '👊', '🔫', '😊', '✌', '💟', '😈', '😕', '💔', '💙', '😘', '💯', '😢', '😭', '😔', '😡', '💕', '😑', '😬', '😜', '😩', '💪', '💁', '🙅', '😪', '😋', '🙈', '😞', '😅', '👏', '👍', '🙊', '🎶', '😐', '😉', '😤', '😂', '👌', '❤', '😏', '😓', '🙏', '👀', '😷', '😁', '💜', '💀', '🙌', '😌', '🎧', '✨', '😴', '😄']
SETTINGS = get_project_settings()
from functools import reduce
user_agent_list = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.113 Safari/537.36',
    'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.90 Safari/537.36',
    'Mozilla/5.0 (Windows NT 5.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.90 Safari/537.36',
    'Mozilla/5.0 (Windows NT 6.2; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.90 Safari/537.36',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/44.0.2403.157 Safari/537.36',
    'Mozilla/5.0 (Windows NT 6.3; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.113 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/57.0.2987.133 Safari/537.36',
    'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/57.0.2987.133 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.87 Safari/537.36',
    'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.87 Safari/537.36',
    'Mozilla/4.0 (compatible; MSIE 9.0; Windows NT 6.1)',
    'Mozilla/5.0 (Windows NT 6.1; WOW64; Trident/7.0; rv:11.0) like Gecko',
    'Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; WOW64; Trident/5.0)',
    'Mozilla/5.0 (Windows NT 6.1; Trident/7.0; rv:11.0) like Gecko',
    'Mozilla/5.0 (Windows NT 6.2; WOW64; Trident/7.0; rv:11.0) like Gecko',
    'Mozilla/5.0 (Windows NT 10.0; WOW64; Trident/7.0; rv:11.0) like Gecko',
    'Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.0; Trident/5.0)',
    'Mozilla/5.0 (Windows NT 6.3; WOW64; Trident/7.0; rv:11.0) like Gecko',
    'Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; Trident/5.0)',
    'Mozilla/5.0 (Windows NT 6.1; Win64; x64; Trident/7.0; rv:11.0) like Gecko',
    'Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 6.1; WOW64; Trident/6.0)',
    'Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 6.1; Trident/6.0)',
    'Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 5.1; Trident/4.0; .NET CLR 2.0.50727; .NET CLR 3.0.4506.2152; .NET CLR 3.5.30729)'
]
logger = logging.getLogger(__name__)


class TweetScraper(CrawlSpider):
    name = 'TweetScraper'
    allowed_domains = ['twitter.com']

    def __init__(self, limit,lang=''):

        self.limit=limit.split(' ')
        self.url = "https://twitter.com/i/search/timeline?l={}".format(lang)
        self.converUrl="https://twitter.com%s"
        self.url = self.url + "&q=%s&src=unkn&vertical=default&include_available_features=1&include_entities=1&reset_error_state=false&max_position=%s"


    def start_requests(self):
        for j in range(len(self.limit)-1):
            for i in emojis:
                tmpurl = self.url % (quote(i+' '+'since:%s until:%s'%(self.limit[j],self.limit[j+1])),'')
                yield http.Request(tmpurl,headers=[("User-Agent", random.choice(user_agent_list))],meta={'tmpurl':tmpurl,'emoji': i,"proxy": SETTINGS['PROXY']},callback=self.parse_tweet_page)


    def parse_tweet_page(self, response):
        # handle current page
        emoji = response.meta['emoji']
        try:data = json.loads(response.body.decode("utf-8"))
        except:yield http.Request(response.url,headers=[("User-Agent", random.choice(user_agent_list))], meta={'tmpurl':response.meta['tmpurl'],'emoji': emoji,"proxy": SETTINGS['PROXY']},callback=self.parse_tweet_page)
        for item in self.parse_tweets_block(data['items_html']):

            url = self.converUrl % item['url']
            parse_page = partial(self.parse_page,item)
            yield http.Request(url,callback=parse_page)

        # get next page
        min_position = data['min_position']
        min_position = min_position.replace("+","%2B")
        url = response.meta['tmpurl']+min_position
        yield http.Request(url,headers=[("User-Agent", random.choice(user_agent_list))], meta={'tmpurl':response.meta['tmpurl'],'emoji': emoji,"proxy": SETTINGS['PROXY']},callback=self.parse_tweet_page)

    def parse_tweets_block(self, html_page):
        page = Selector(text=html_page)

        ### for text only tweets
        items = page.xpath('//li[@data-item-type="tweet"]/div')
        for item in self.parse_tweet_item(items):
            yield item
    def parse_page(self,tweet,response):
        def parse_tweet_item(items):
            for item in items:
                
                tweet = Tweet()

                tweet['usernameTweet'] = item.xpath('.//span[@class="username u-dir u-textTruncate"]/b/text()').extract()[0]
                tweet['lang'] = item.xpath('.//@lang').get()
                if tweet['lang'] not in {'en','und'}:
                    raise NameError('not support lang')
                ID = item.xpath('.//@data-tweet-id').extract()
                if not ID:
                    raise NameError('no ID')
                tweet['ID'] = ID[0]

                ### get text content
                tweet['text'] = ' '.join(
                    item.xpath('.//div[@class="js-tweet-text-container"]/p//text()|.//div[@class="js-tweet-text-container"]/p//img/@alt').extract()).replace(' # ',
                                                                                                        '#').replace(
                    ' @ ', '@')
                tweet['emoji'] = ' '.join(
                    item.xpath('.//div[@class="js-tweet-text-container"]/p//img/@alt').extract())
                if tweet['text'] == '':
                    # If there is not text, we ignore the tweet
                    raise NameError('empty text')

                ### get meta data
                tweet['url'] = item.xpath('.//@data-permalink-path').extract()[0]

                '''nbr_retweet = item.css('span.ProfileTweet-action--retweet > span.ProfileTweet-actionCount').xpath(
                    '@data-tweet-stat-count').extract()
                if nbr_retweet:
                    tweet['nbr_retweet'] = int(nbr_retweet[0])
                else:
                    tweet['nbr_retweet'] = 0

                nbr_favorite = item.css('span.ProfileTweet-action--favorite > span.ProfileTweet-actionCount').xpath(
                    '@data-tweet-stat-count').extract()
                if nbr_favorite:
                    tweet['nbr_favorite'] = int(nbr_favorite[0])
                else:
                    tweet['nbr_favorite'] = 0

                nbr_reply = item.css('span.ProfileTweet-action--reply > span.ProfileTweet-actionCount').xpath(
                    '@data-tweet-stat-count').extract()
                if nbr_reply:
                    tweet['nbr_reply'] = int(nbr_reply[0])
                else:
                    tweet['nbr_reply'] = 0'''

                tweet['datetime'] = datetime.fromtimestamp(int(
                    item.xpath('.//small[@class="time"]/a/span/@data-time').extract()[
                        0])).strftime('%Y-%m-%d %H:%M:%S')

                ### get photo
                has_cards = item.xpath('.//@data-card-type').extract()
                if has_cards and has_cards[0] == 'photo':
                    tweet['has_image'] = True
                    tweet['images'] = item.xpath('.//*/div/@data-image-url').extract()
                elif has_cards:
                    logger.debug('Not handle "data-card-type":\n%s' % item.xpath('.').extract()[0])

                ### get animated_gif
                has_cards = item.xpath('.//@data-card2-type').extract()
                if has_cards:
                    if has_cards[0] == 'animated_gif':
                        tweet['has_video'] = True
                        tweet['videos'] = item.xpath('.//*/source/@video-src').extract()
                    elif has_cards[0] == 'player':
                        tweet['has_media'] = True
                        tweet['medias'] = item.xpath('.//*/div/@data-card-url').extract()
                    elif has_cards[0] == 'summary_large_image':
                        tweet['has_media'] = True
                        tweet['medias'] = item.xpath('.//*/div/@data-card-url').extract()
                    elif has_cards[0] == 'amplify':
                        tweet['has_media'] = True
                        tweet['medias'] = item.xpath('.//*/div/@data-card-url').extract()
                    elif has_cards[0] == 'summary':
                        tweet['has_media'] = True
                        tweet['medias'] = item.xpath('.//*/div/@data-card-url').extract()
                    elif has_cards[0] == '__entity_video':
                        pass  # TODO
                        # tweet['has_media'] = True
                        # tweet['medias'] = item.xpath('.//*/div/@data-src').extract()
                    else:  # there are many other types of card2 !!!!
                        logger.debug('Not handle "data-card2-type":\n%s' % item.xpath('.').extract()[0])

                is_reply = item.xpath('.//div[@class="ReplyingToContextBelowAuthor"]').extract()
                tweet['is_reply'] = is_reply != []
                if  tweet['is_reply']: tweet['reply_to'] = item.xpath('.//div[@class="ReplyingToContextBelowAuthor"]//@href|.//div[@class="ReplyingToContextBelowAuthor"]//@data-user-id').extract() 
                #href uid
                is_retweet = item.xpath('.//span[@class="js-retweet-text"]').extract()
                tweet['is_retweet'] = is_retweet != []

                tweet['user_id'] = item.xpath('.//@data-user-id').extract()[0]
                yield tweet

        html_page = response.body.decode("utf-8")
        page = Selector(text=html_page)

        
        con = Conversa()
        #ids = set(page.xpath('.//div[contains(@class,"js-stream-tweet") and @data-conversation-id]/@data-conversation-id').extract())
        #去掉转推等， 出现两个twitter 文的对话
        ids = set(page.xpath('.//div[@data-conversation-id]/@data-conversation-id').extract())
        if len(ids) != 1:
            return 
        else :
            con['ID'] = ids.pop()
        ancestors = page.xpath('.//div[@id="ancestors"]')
        items = ancestors.xpath('.//div[@data-conversation-id=%s]'%con['ID'])
        #这是被删掉的twitter的数量
        delItiem = len(list(ancestors.xpath('.//div[@class="stream-tombstone-container ThreadedConversation-tweet last"]')))
        #过滤小于3轮的对话
        if len(items) < 2 or delItiem > 0 :return
        try:
            con['context'] = list(parse_tweet_item(items))
            originTweet = page.xpath('.//div[@data-tweet-id=%s]'%tweet['ID'])
            originTweet = list(parse_tweet_item(originTweet))[0]
        except:
            return
        #con['tweet_id'] = tweet['_id']
        con['context'].append(originTweet)
        checkUser  = lambda con:sum([0 if i['user_id']==con['context'][0]['user_id'] else 1 for i in con['context']])
        if len(con['context'])>9 and con['context'][0]['is_reply'] and checkUser(con)>0:
            i=con['context'][0]
            parse_page = partial(self.parse_page,i)
            yield http.Request(self.converUrl%i['url'],callback=parse_page)
        yield con




    def parse_tweet_item(self, items):
        for item in items:
            try:
                tweet = Tweet()

                tweet['usernameTweet'] = item.xpath('.//span[@class="username u-dir u-textTruncate"]/b/text()').extract()[0]

                ID = item.xpath('.//@data-tweet-id').extract()
                if not ID:
                    continue
                tweet['ID'] = ID[0]

                ### get text content
                tweet['text'] = ' '.join(
                    item.xpath('.//div[@class="js-tweet-text-container"]/p//text()|.//div[@class="js-tweet-text-container"]/p//img/@alt').extract()).replace(' # ',
                                                                                                        '#').replace(
                    ' @ ', '@')
                tweet['emoji'] = ' '.join(
                    item.xpath('.//div[@class="js-tweet-text-container"]/p//img/@alt').extract())
                if tweet['text'] == '':
                    # If there is not text, we ignore the tweet
                    continue

                ### get meta data
                tweet['url'] = item.xpath('.//@data-permalink-path').extract()[0]

                nbr_retweet = item.css('span.ProfileTweet-action--retweet > span.ProfileTweet-actionCount').xpath(
                    '@data-tweet-stat-count').extract()
                if nbr_retweet:
                    tweet['nbr_retweet'] = int(nbr_retweet[0])
                else:
                    tweet['nbr_retweet'] = 0

                nbr_favorite = item.css('span.ProfileTweet-action--favorite > span.ProfileTweet-actionCount').xpath(
                    '@data-tweet-stat-count').extract()
                if nbr_favorite:
                    tweet['nbr_favorite'] = int(nbr_favorite[0])
                else:
                    tweet['nbr_favorite'] = 0

                nbr_reply = item.css('span.ProfileTweet-action--reply > span.ProfileTweet-actionCount').xpath(
                    '@data-tweet-stat-count').extract()
                if nbr_reply:
                    tweet['nbr_reply'] = int(nbr_reply[0])
                else:
                    tweet['nbr_reply'] = 0

                tweet['datetime'] = datetime.fromtimestamp(int(
                    item.xpath('.//div[@class="stream-item-header"]/small[@class="time"]/a/span/@data-time').extract()[
                        0])).strftime('%Y-%m-%d %H:%M:%S')

                ### get photo
                has_cards = item.xpath('.//@data-card-type').extract()
                if has_cards and has_cards[0] == 'photo':
                    tweet['has_image'] = True
                    tweet['images'] = item.xpath('.//*/div/@data-image-url').extract()
                elif has_cards:
                    logger.debug('Not handle "data-card-type":\n%s' % item.xpath('.').extract()[0])

                ### get animated_gif
                has_cards = item.xpath('.//@data-card2-type').extract()
                if has_cards:
                    if has_cards[0] == 'animated_gif':
                        tweet['has_video'] = True
                        tweet['videos'] = item.xpath('.//*/source/@video-src').extract()
                    elif has_cards[0] == 'player':
                        tweet['has_media'] = True
                        tweet['medias'] = item.xpath('.//*/div/@data-card-url').extract()
                    elif has_cards[0] == 'summary_large_image':
                        tweet['has_media'] = True
                        tweet['medias'] = item.xpath('.//*/div/@data-card-url').extract()
                    elif has_cards[0] == 'amplify':
                        tweet['has_media'] = True
                        tweet['medias'] = item.xpath('.//*/div/@data-card-url').extract()
                    elif has_cards[0] == 'summary':
                        tweet['has_media'] = True
                        tweet['medias'] = item.xpath('.//*/div/@data-card-url').extract()
                    elif has_cards[0] == '__entity_video':
                        pass  # TODO
                        # tweet['has_media'] = True
                        # tweet['medias'] = item.xpath('.//*/div/@data-src').extract()
                    else:  # there are many other types of card2 !!!!
                        logger.debug('Not handle "data-card2-type":\n%s' % item.xpath('.').extract()[0])

                is_reply = item.xpath('.//div[@class="ReplyingToContextBelowAuthor"]').extract()
                tweet['is_reply'] = is_reply != []
                if not tweet['is_reply']: continue
                is_retweet = item.xpath('.//span[@class="js-retweet-text"]').extract()
                tweet['is_retweet'] = is_retweet != []

                tweet['user_id'] = item.xpath('.//@data-user-id').extract()[0]
                yield tweet
            except:
                logger.debug("Error tweet:\n%s" % item.xpath('.').extract()[0])
                # raise

    def extract_one(self, selector, xpath, default=None):
        extracted = selector.xpath(xpath).extract()
        if extracted:
            return extracted[0]
        return default

