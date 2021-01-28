import logging
import inspect
from spaceone.core import config
from spaceone.core.error import *

__init__ = ['is_set', 'get', 'set', 'increment', 'decrement', 'keys', 'ttl', 'delete', 'delete_pattern',
            'flush', 'cacheable']

_CACHE_CONNECTIONS = {}
_LOGGER = logging.getLogger(__name__)


def _create_connection(backend):
    global_conf = config.get_global()
    if backend not in global_conf.get('CACHES', {}):
        raise ERROR_CACHE_CONFIGURATION(backend=backend)

    cache_conf = global_conf['CACHES'][backend].copy()
    backend = cache_conf.pop('backend', None)

    if backend is None:
        raise ERROR_CACHE_CONFIGURATION(backend=backend)

    module_name, class_name = backend.rsplit('.', 1)
    cache_module = __import__(module_name, fromlist=[class_name])
    return getattr(cache_module, class_name)(backend, cache_conf)


def connection(func):
    def wrapper(*args, backend='default', **kwargs):
        if backend not in _CACHE_CONNECTIONS:
            _CACHE_CONNECTIONS[backend] = _create_connection(backend)

        return func(_CACHE_CONNECTIONS[backend], *args, **kwargs)

    return wrapper


def _change_args_to_dict(func, args):
    args_dict = {}
    args_length = len(args)
    func_args = inspect.getfullargspec(func).args

    defaults = inspect.getfullargspec(func).defaults
    if defaults:
        first_default_index = len(func_args) - len(defaults)

    i = 0
    for arg_key in func_args:
        if i < args_length:
            args_dict[arg_key] = args[i]

        else:
            if defaults:
                args_dict[arg_key] = defaults[i - first_default_index]

        i += 1

    return args_dict


def _make_cache_key(key_format, args_dict):
    key_data = {}
    for key, value in args_dict.items():
        if isinstance(value, list) or isinstance(value, tuple):
            sorted_values = sorted(value)
            values_str = ','.join(sorted_values)
            key_data[key] = values_str
        else:
            key_data[key] = value

    try:
        return key_format.format(**key_data)

    except Exception:
        raise ERROR_CACHE_KEY_FORMAT(key=key_format)


def cacheable(key=None, value=None, expire=None, action='cache', backend='default'):
    def wrapper(func):
        def wrapped_func(*args, **kwargs):
            if is_set(backend):
                args_dict = _change_args_to_dict(func, args)
                args_dict.update(kwargs)
                cache_key = _make_cache_key(key, args_dict)
                if action in ['cache']:
                    data = get(cache_key, backend=backend)
                    if data is not None:
                        return data

            result = func(*args, **kwargs)

            if is_set(backend):
                if action in ['put', 'cache']:
                    cache_value = result
                    if value:
                        if isinstance(value, dict):
                            cache_value = result.get(value)

                        else:
                            if hasattr(result, value):
                                cache_value = getattr(result, value)
                            else:
                                raise ERROR_CACHEABLE_VALUE_TYPE()

                    set(cache_key, cache_value, expire=expire, backend=backend)

                if action in ['delete']:
                    delete(cache_key, backend=backend)

            return result

        return wrapped_func

    return wrapper


def is_set(backend='default'):
    global_conf = config.get_global()
    if global_conf.get('CACHES', {}).get(backend, {}) != {}:
        return True
    else:
        return False


@connection
def get(cache_cls, key):
    return cache_cls.get(key)


@connection
def set(cache_cls, key, value, expire=None):
    return cache_cls.set(key, value, expire=expire)


@connection
def increment(cache_cls, key, amount=1):
    return cache_cls.increment(key, amount)


@connection
def decrement(cache_cls, key, amount=1):
    return cache_cls.decrement(key, amount)


@connection
def keys(cache_cls, pattern):
    return cache_cls.keys(pattern)


@connection
def ttl(cache_cls, key):
    return cache_cls.ttl(key)


@connection
def delete(cache_cls, *keys):
    return cache_cls.delete(*keys)


@connection
def delete_pattern(cache_cls, pattern):
    return cache_cls.delete_pattern(pattern)


@connection
def flush(cache_cls, is_async=False):
    return cache_cls.flush(is_async)


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
