# -*- coding: utf-8 -*-

from cachetools import TTLCache

from spaceone.core.error import *
from spaceone.core.cache import BaseCache


class LocalCache(BaseCache):

    def __init__(self, backend, cache_conf):
        try:
            max_size = cache_conf.get('max_size', 128)
            ttl = cache_conf.get('ttl', 86400)
            self.cache = TTLCache(maxsize=max_size, ttl=ttl)
        except Exception:
            raise ERROR_CACHE_CONFIGURATION(backend=backend)

    def get(self, key):
        return self.cache.get(key)

    def set(self, key, value, expire=None):
        if expire:
            raise ERROR_CACHE_OPTION(method='cache.set', option='expire')

        self.cache[key] = value
        return True

    def delete(self, *keys):
        for key in keys:
            del self.cache[key]

    def flush(self, is_async=False):
        self.cache.popitem()
        self.cache.expire()
