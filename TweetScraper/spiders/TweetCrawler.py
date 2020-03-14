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

from datetime import datetime
import subprocess
from TweetScraper.items import Tweet, User,Conversa
from txmongo.collection import Collection
from txmongo.connection import ConnectionPool
from txmongo.database import Database
from txmongo.filter import sort as txsort
from twisted.internet.defer import inlineCallbacks
emojis = ['😠', '✋', '😳', '💖', '😎', '😒', '😍', '😣', '😫', '😖', '☺', '♥', '👊', '🔫', '😊', '✌', '💟', '😈', '😕', '💔', '💙', '😘', '💯', '😢', '😭', '😔', '😡', '💕', '😑', '😬', '😜', '😩', '💪', '💁', '🙅', '😪', '😋', '🙈', '😞', '😅', '👏', '👍', '🙊', '🎶', '😐', '😉', '😤', '😂', '👌', '❤', '😏', '😓', '🙏', '👀', '😷', '😁', '💜', '💀', '🙌', '😌', '🎧', '✨', '😴', '😄']
SETTINGS = get_project_settings()


logger = logging.getLogger(__name__)


class TweetScraper(CrawlSpider):
    name = 'TweetScraper'
    allowed_domains = ['twitter.com']

    def __init__(self, query='', lang='', crawl_user=False, top_tweet=False):

        
        self.query = query
        self.url = "https://twitter.com/i/search/timeline?l={}".format(lang)
        self.converUrl="https://twitter.com%s"
        if not top_tweet:
            self.url = self.url + "&f=tweets"

        self.url = self.url + "&q=%s&src=unkn&vertical=default&include_available_features=1&include_entities=1&reset_error_state=false&max_position=%s"

        self.crawl_user = crawl_user

    def start_requests(self):
        connection  =  pymongo.MongoClient(SETTINGS['PIPELINE_MONGO_URI'])
        db=connection[SETTINGS['PIPELINE_MONGO_DATABASE']]
        self.tweetCollection = db[SETTINGS['MONGODB_TWEET_COLLECTION']]
        url = self.url % (quote(self.query), '')
        yield http.Request(url, callback=self.parse_tweet_page)

    #@inlineCallbacks
    # def query(self,ID):
    #     res =  self.tweetCollection.find_one({'ID': ID})
    #     return  res
    def parse_tweet_page(self, response):
        # inspect_response(response, self)
        # handle current page
        data = json.loads(response.body.decode("utf-8"))
        for item in self.parse_tweets_block(data['items_html']):
            res = self.tweetCollection.find_one({'ID': item['ID']})
            if res is None:
                url = self.converUrl % item['url']
                parse_page = partial(self.parse_page,item)
                yield http.Request(url,callback=parse_page)

        # get next page
        min_position = data['min_position']
        min_position = min_position.replace("+","%2B")
        url = self.url % (quote(self.query), min_position)
        yield http.Request(url, callback=self.parse_tweet_page)

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
        if len(items) < 3 or delItiem > 0 :return
        try:
            con['context'] = list(parse_tweet_item(items))
            originTweet = page.xpath('.//div[@data-tweet-id=%s]'%tweet['ID'])
            originTweet = list(parse_tweet_item(originTweet))[0]
        except:
            return
        #con['tweet_id'] = tweet['_id']
        con['context'].append(originTweet)
        
        return con




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

                if self.crawl_user:
                    ### get user info
                    user = User()
                    user['ID'] = tweet['user_id']
                    user['name'] = item.xpath('.//@data-name').extract()[0]
                    user['screen_name'] = item.xpath('.//@data-screen-name').extract()[0]
                    user['avatar'] = \
                        item.xpath('.//div[@class="content"]/div[@class="stream-item-header"]/a/img/@src').extract()[0]
                    yield user
            except:
                logger.error("Error tweet:\n%s" % item.xpath('.').extract()[0])
                # raise

    def extract_one(self, selector, xpath, default=None):
        extracted = selector.xpath(xpath).extract()
        if extracted:
            return extracted[0]
        return default

