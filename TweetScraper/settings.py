# -*- coding: utf-8 -*-

# !!! # Crawl responsibly by identifying yourself (and your website/e-mail) on the user-agent
USER_AGENT = 'valar000@outlook.com'
# settings for spiders
BOT_NAME = 'TweetScraper'
LOG_LEVEL = 'INFO'
DOWNLOAD_HANDLERS = {'s3': None,} # from http://stackoverflow.com/a/31233576/2297751, TODO

PROXY='http://127.0.0.1:3128'
SPIDER_MODULES = ['TweetScraper.spiders']
NEWSPIDER_MODULE = 'TweetScraper.spiders'
ITEM_PIPELINES = {
    'TweetScraper.pipelines.SaveToMongoPipeline':100, # replace `SaveToFilePipeline` with this to use MongoDB
}
RETRY_HTTP_CODES = [500, 502, 503, 504, 522, 524,400,403, 408, 429]

RETRY_TIMES=10
DOWNLOADER_MIDDLEWARES = {
    'scrapy.downloadermiddlewares.retry.RetryMiddleware': None,
    'TweetScraper.middlewares.TooManyRequestsRetryMiddleware': 543,
}
# settings for where to save data on disk
SAVE_TWEET_PATH = './Data/tweet/'
SAVE_USER_PATH = './Data/user/'
SAVE_CONVERSA_FILE = './Data/conversa'
CONCURRENT_REQUESTS_PER_DOMAIN = 60
DOWNLOAD_DELAY = 0.001
# settings for mongodb
PIPELINE_MONGO_URI = "mongodb://localhost:27017"
PIPELINE_MONGO_DATABASE ="TweetScrape"
PIPELINE_MONGO_COLLECTION=MONGODB_TWEET_COLLECTION = "tweet" # collection name to save tweets
MONGO_INDEXES='ID'
TELNETCONSOLE_ENABLED = False