import datetime
from typing import Dict
import boto3
import requests

from aws_lambda_powertools import Logger

logger = Logger(__name__)


class HTTPCacheHandler:
    def __init__(self, dynamodb_table: str = None, validity_timeout: int = 15, aws_region: str = 'us-east-1',
                 logger: Logger = logger):
        """
        validity_timeout : in seconds
        """
        self.logger = logger
        self.validity_timeout = validity_timeout
        session = boto3.session.Session(region_name=aws_region)
        self.table = session.resource("dynamodb").Table(dynamodb_table)

    # check result from remote cache and add to cache if changed
    def get_result(self, url: str, method: str = 'GET', headers: Dict = None, body: Dict = None) -> str:
        cache_result = self._get_item(url, method)

        # if item exists in cache
        if cache_result.get('Item'):
            return self._handle_cached_item(cache_result.get('Item'), url, method, headers, body)
        # item not in cache, query + update cache
        else:
            return self._handle_non_cached_item(url, method, headers, body)

    def _get_item(self, url: str, method: str):
        self.logger.info("Retrieving item from cache")
        return self.table.get_item(Key={
            'url': url,
            'method': method
        })

    def _put_item(self, url: str, method: str, response_text: str):
        self.logger.info("Putting item to cache")
        return self.table.put_item(Item={
            'url': url,
            'method': method,
            'response': response_text,
            'time': datetime.datetime.now().isoformat()
        })

    def _is_cache_item_invalid(self, cache_result):
        cache_time = datetime.datetime.fromisoformat(cache_result['time'])
        return datetime.datetime.now() - cache_time < datetime.timedelta(seconds=self.validity_timeout)

    def _handle_cached_item(self, cache_result, url, method, headers, body):
        self.logger.info("Checking if cache is valid")
        is_valid = self._is_cache_item_invalid(cache_result)
        self.logger.info(f"Item validity is {is_valid}")
        if is_valid:
            return cache_result.get('response')
        else:
            self.logger.info("querying real resource for item")
            return self._handle_non_cached_item(url, method, headers, body)

    def _handle_non_cached_item(self, url, method, headers, body) -> str:
        response = requests.request(method=method, url=url, headers=headers, json=body)
        self._put_item(url, method, response.text)
        return response.text
