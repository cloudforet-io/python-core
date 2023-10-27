import logging
import json
import redis
from redis import SSLConnection

from spaceone.core.error import *
from spaceone.core.cache.base_cache import BaseCache

_LOGGER = logging.getLogger(__name__)


class RedisCache(BaseCache):

    def __init__(self, alias, cache_conf):
        try:
            if cache_conf.get('ssl', False):
                del cache_conf['ssl']
                pool = redis.ConnectionPool(connection_class=SSLConnection, **cache_conf)
            else:
                pool = redis.ConnectionPool(**cache_conf)
            self.conn = self._get_connection(pool)
            self.conn.ping()
        except redis.exceptions.TimeoutError:
            raise ERROR_CACHE_TIMEOUT(config=cache_conf)
        except Exception as e:
            _LOGGER.error(f'[RedisCache.__init__] failed to create connection: {e}')
            raise ERROR_CACHE_CONFIGURATION(alias=alias)

    def _get_connection(self, pool):
        return redis.Redis(connection_pool=pool)

    def get(self, key, **kwargs):
        try:
            cache_value = self.conn.get(key)
            if cache_value:
                return json.loads(cache_value)
            else:
                return cache_value

        except Exception as e:
            raise ERROR_CACHE_DECODE(reason=e)

    def set(self, key, value, expire=None, **kwargs):
        try:
            if value is None:
                value = {}

            cache_value = json.dumps(value)
            return self.conn.set(key, cache_value, ex=expire)
        except Exception as e:
            raise ERROR_UNKNOWN(message=e)

    def increment(self, key, amount=1):
        try:
            return self.conn.incr(key, amount)
        except Exception as e:
            raise ERROR_UNKNOWN(message=e)

    def decrement(self, key, amount=1):
        try:
            return self.conn.decr(key, amount)
        except Exception as e:
            raise ERROR_UNKNOWN(message=e)

    def keys(self, pattern='*'):
        return self.conn.keys(pattern)

    def ttl(self, key):
        return self.conn.ttl(key)

    def delete(self, *keys):
        self.conn.delete(*keys)

    def delete_pattern(self, pattern):
        for key in self.keys(pattern):
            self.conn.delete(key)

    def flush(self, is_async=False):
        self.conn.flushdb(is_async)
