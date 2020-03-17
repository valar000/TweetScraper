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
            index=item_len-1-i
            res =yield self.collection.find_one('ID':items[index]['ID'])
            if res:
                if index == 0:
                    result = yield self.collection.insert_one(document=dict(items[index]))
                else:
                    result = yield self.collection.insert_one(
                        document=dict({**items[index], 'rep_ID': items[index-1]['ID']}))
            elif i==0: raise DropItem("Duplicate item")
            else: break

        return item




