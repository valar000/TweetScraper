from scrapy.downloadermiddlewares.retry import RetryMiddleware
from scrapy.utils.response import response_status_message
from scrapy.utils.project import get_project_settings
SETTINGS = get_project_settings()
import time
import json
class TooManyRequestsRetryMiddleware(RetryMiddleware):

    def __init__(self, crawler):
        super(TooManyRequestsRetryMiddleware, self).__init__(crawler.settings)
        self.crawler = crawler
    @classmethod
    def from_crawler(cls, crawler):
        return cls(crawler)

    def process_response(self, request, response, spider):
        if 'proxy' in request.meta:
            try: json.loads(response.body.decode("utf-8"))
            except: 
                reason = response_status_message(response.status)
                return self._retry(request, reason, spider) or response
        if request.meta.get('dont_retry', False):
            return response
        elif response.status == 429:
            if request.meta.get('retry_times', 0)  > (SETTINGS['RETRY_TIMES']/2) and  'proxy' in request.meta:
                request.meta.pop('proxy')
            reason = response_status_message(response.status)
            return self._retry(request, reason, spider) or response
        elif response.status >= 500  or response.status==408:
            if 'proxy' in request.meta: request.meta.pop('proxy')
            reason = response_status_message(response.status)
            return self._retry(request, reason, spider) or response
        elif response.status in self.retry_http_codes:
            reason = response_status_message(response.status)
            return self._retry(request, reason, spider) or response
        return response 