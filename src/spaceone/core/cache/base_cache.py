class BaseCache(object):

    def get(self, key, **kwargs):
        """
        Args:
            key (str)
            **kwargs (dict)
        Returns:
            cache_value (any)
        """
        raise NotImplementedError('cache.get not implemented!')

    def set(self, key, value, **kwargs):
        """
        Args:
            key (str)
            value (str)
            **kwargs (dict)
                - expire (int: seconds)

        Returns:
            True | False
        """
        raise NotImplementedError('cache.set not implemented!')

    def increment(self, key, amount):
        """
        Args:
            key (str)
            amount (int)

        Returns:
            True | False
        """
        raise NotImplementedError('cache.incr not implemented!')

    def decrement(self, key, amount):
        """
        Args:
            key (str)
            amount (int)

        Returns:
            True | False
        """
        raise NotImplementedError('cache.decr not implemented!')

    def keys(self, pattern='*'):
        """
        Args:
            pattern (str)

        Returns:
            keys (list)
        """
        raise NotImplementedError('cache.keys not implemented!')

    def ttl(self, key):
        """
        Args:
            key (str)

        Returns:
            expire_time (int: seconds)
        """
        raise NotImplementedError('cache.ttl not implemented!')

    def delete(self, *keys):
        """
        Args:
            keys (list)

        Returns:
            None
        """
        raise NotImplementedError('cache.delete not implemented!')

    def delete_pattern(self, pattern):
        """
        Args:
            pattern (str)

        Returns:
            None
        """
        raise NotImplementedError('cache.delete_pattern not implemented!')

    def flush(self, is_async=False):
        """
        Args:
            is_async (bool)

        Returns:
            None
        """
        raise NotImplementedError('cache.flush not implemented!')
