# -*- coding: utf-8 -*-

# !!! # Crawl responsibly by identifying yourself (and your website/e-mail) on the user-agent
USER_AGENT = 'valar000@outlook.com'

# settings for spiders
BOT_NAME = 'TweetScraper'
LOG_LEVEL = 'INFO'
DOWNLOAD_HANDLERS = {'s3': None,} # from http://stackoverflow.com/a/31233576/2297751, TODO
PIPELINE_MONGO_URI = "mongodb://localhost:27017"
SPIDER_MODULES = ['TweetScraper.spiders']
NEWSPIDER_MODULE = 'TweetScraper.spiders'
ITEM_PIPELINES = {
    #'TweetScraper.pipelines.SaveToFilePipeline':100,
    'TweetScraper.pipelines.SaveToMongoPipeline':100, # replace `SaveToFilePipeline` with this to use MongoDB
    #'TweetScraper.pipelines.SavetoMySQLPipeline':100, # replace `SaveToFilePipeline` with this to use MySQL
}

# settings for where to save data on disk
SAVE_TWEET_PATH = './Data/tweet/'
SAVE_USER_PATH = './Data/user/'
SAVE_CONVERSA_FILE = './Data/conversa'
CONCURRENT_REQUESTS_PER_DOMAIN = 8
#DOWNLOAD_DELAY = 0.01
# settings for mongodb
MONGODB_SERVER = "127.0.0.1"
MONGODB_PORT = 27017
MONGODB_DB = "TweetScraper"        # database name to save the crawled data
PIPELINE_MONGO_DATABASE ="TweetScrape"
PIPELINE_MONGO_COLLECTION=MONGODB_TWEET_COLLECTION = "tweet" # collection name to save tweets
MONGODB_USER_COLLECTION = "user"   # collection name to save users
MONGODB_CONVERSA_COLLECTION = "conversation"
#settings for mysql
MYSQL_SERVER = "127.0.0.1"
MYSQL_DB     = "TweetScraper"
MYSQL_TABLE  = "scraper" # the table will be created automatically
MYSQL_USER   = ""        # MySQL user to use (should have INSERT access granted to the Database/Table
MYSQL_PWD    = ""        # MySQL user's password
TELNETCONSOLE_ENABLED = False