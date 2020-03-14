# -*- coding: utf-8 -*-
from scrapy.exceptions import DropItem
from scrapy.utils.project import get_project_settings
import logging
import pymongo
import json
import os
import inspect
import pymongo

from TweetScraper.items import Tweet, User, Conversa
from TweetScraper.utils import mkdirs
from pymongo.errors import OperationFailure
from pymongo.results import InsertOneResult
from scrapy.crawler import Crawler
from scrapy.item import Item
from scrapy.settings import Settings
from scrapy.spiders import Spider
from twisted.internet.defer import inlineCallbacks
from txmongo.collection import Collection
from txmongo.connection import ConnectionPool
from txmongo.database import Database
from txmongo.filter import sort as txsort


SETTINGS = get_project_settings()

logger = logging.getLogger(__name__)






def get_args(func):
    """

    :param func:
    :type func: callable
    :return:
    :rtype: tuple
    """
    sig = inspect.signature(func)
    return tuple(sig.parameters.keys())


class SaveToMongoPipeline(object):
    """
    A pipeline saved into MongoDB asynchronously with txmongo
    """

    def __init__(self, uri, settings):
        """

        :param uri:
        :type uri: str
        :param settings:
        :type settings:
        """

        self.settings = settings
        self.crawler = None
        self.uri = uri

        self.mongo = None
        self.database = None
        self.collection = None


    @classmethod
    def from_settings(cls, settings):
        """

        :param settings:
        :type settings: Settings
        :return:
        :rtype: MongoPipeline
        """
        uri = settings["PIPELINE_MONGO_URI"]
        return cls(uri=uri, settings=settings)

    def _get_args_from_settings(self, func) :
        """

        :param func:
        :type func: Callable
        :return:
        :rtype: Dict[str, str]
        """
        func_args = dict()
        for arg in get_args(func):
            key = "PIPELINE_MONGO_{arg}".format(arg=arg.upper())
            if key in self.settings:
                func_args.update({arg: self.settings[key]})
        return func_args

    def _get_callable(self, callable_, **kwargs):
        """

        :param callable_:
        :param kwargs:
        :return:
        :rtype:
        """
        args = self._get_args_from_settings(func=callable_)
        args.update(kwargs)
        return callable_(**args)

    @inlineCallbacks
    def open_spider(self, spider):
        """

        :param spider:
        :type spider: Spider
        :return:
        :rtype:
        """
        self.mongo = yield self._get_callable(ConnectionPool)
        self.database = yield self._get_callable(
            Database,
            factory=self.mongo,
            database_name=self.settings.get("PIPELINE_MONGO_DATABASE"),
        )
        if all(
                (
                    self.settings.get("PIPELINE_MONGO_USERNAME"),
                    self.settings.get("PIPELINE_MONGO_PASSWORD"),
                )
        ):
            yield self._get_callable(
                self.database.authenticate,
                name=self.settings.get("PIPELINE_MONGO_USERNAME"),
            )
        try:
            yield self.database.collection_names()
        except OperationFailure as err:
            self.crawler.engine.close_spider(spider=spider, reason=str(err))
        else:
            self.collection = yield self._get_callable(
                Collection,
                database=self.database,
                name=self.settings.get("PIPELINE_MONGO_COLLECTION"),
            )
            yield self.create_indexes(spider=spider)


    @inlineCallbacks
    def close_spider(self, spider):
        """

        :param spider:
        :type spider: Spider
        :return:
        :rtype:
        """
        yield self.mongo.disconnect()



    @inlineCallbacks
    def create_indexes(self, spider):
        """

        :param spider:
        :type spider: Spider
        :return:
        :rtype:
        """
        indexes = self.settings.get("PIPELINE_MONGO_INDEXES", list())
        for field, _order, *args in indexes:
            sort_fields = txsort(_order(field))
            try:
                kwargs = args[0]
            except IndexError:
                kwargs = {}
            _ = yield self.collection.create_index(sort_fields, **kwargs)

    @inlineCallbacks
    def process_item(self, item, spider):
        item_len=len(item['context'])
        items = item['context']
        for i in range(item_len):
            if i == item_len-1:
                res = yield self.collection.insert_one(document=dict(items[item_len-i-1]))
            else:
                res = yield self.collection.insert_one(
                    document=dict({**items[item_len-1-i], 'rep_ID': items[item_len-2-i]['ID']}))


        return item


#
# class SaveToMongoPipeline(object):
#
#     ''' pipeline that save data to mongodb '''
#     def __init__(self):
#         self.connection =  yield ConnectionPool(SETTINGS['mongodb_uri'])
#         self.db = self.connection[SETTINGS['MONGODB_DB']]
#         self.tweetCollection = self.db[SETTINGS['MONGODB_TWEET_COLLECTION']]
#
#
#
#     @classmethod
#     def from_settings(cls, settings: Settings):
#         """
#
#         :param settings:
#         :type settings: Settings
#         :return:
#         :rtype: MongoPipeline
#         """
#         return cls()
#
#     @inlineCallbacks
#     def process_item(self, item: Item, spider: Spider) -> Item:
#         """
#
#         :param item:
#         :type item: Item
#         :param spider:
#         :type spider: Spider
#         :return:
#         :rtype: Item
#         """
#         _id=None
#         for index,i in enumerate( reversed(item)):
#             res = yield self.tweetCollection.find_one({'ID':i['ID']})
#             if  res is None:
#                 if index==0:
#                     _id = yield self.tweetCollection.insert_one(document=dict(i))
#                 else:
#                     _id = yield self.tweetCollection.insert_one(document=dict({**i,'rep':[{'$ref':SETTINGS['MONGODB_TWEET_COLLECTION'],'_id':_id}]}))
#             else:
#                 if _id is not None:
#                     yield self.tweetCollection.update_one({'_id':res['_id']},
#                     {'$set':
#                         {'rep':
#                             [*res['rep'],{'$ref':SETTINGS['MONGODB_TWEET_COLLECTION'],'_id':_id}]}})
#                 else: break
#
#         return item

    # def process_item(self, item, spider):
    #     if isinstance(item, Tweet):
    #         dbItem = self.tweetCollection.find_one({'ID': item['ID']})
    #         if dbItem:
    #             pass # simply skip existing items
    #             ### or you can update the tweet, if you don't want to skip:
    #             # dbItem.update(dict(item))
    #             # self.tweetCollection.save(dbItem)
    #             # logger.info("Update tweet:%s"%dbItem['url'])
    #         else:
    #             self.tweetCollection.insert_one(dict(item))
    #             logger.debug("Add tweet:%s" %item['url'])

    #     elif isinstance(item, User):
    #         dbItem = self.userCollection.find_one({'ID': item['ID']})
    #         if dbItem:
    #             pass # simply skip existing items
    #             ### or you can update the user, if you don't want to skip:
    #             # dbItem.update(dict(item))
    #             # self.userCollection.save(dbItem)
    #             # logger.info("Update user:%s"%dbItem['screen_name'])
    #         else:
    #             self.userCollection.insert_one(dict(item))
    #             logger.debug("Add user:%s" %item['screen_name'])
    #     elif isinstance(item,Conversa):
    #         dbItem = self.conversaCollection.find_one({'ID': item['ID']})
    #         if dbItem:
    #             pass
    #         else:
    #             self.conversaCollection.insert_one(dict(item))
    #             logger.debug("Add conversa:%s" %item)
    #     else:
    #         logger.info("Item type is not recognized! type = %s" %type(item))



