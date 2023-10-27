import logging
import inspect
import copy
from spaceone.core import config
from spaceone.core.error import *
from spaceone.core.cache.local_cache import LocalCache
from spaceone.core.cache.redis_cache import RedisCache

__init__ = ['is_set', 'get', 'set', 'increment', 'decrement', 'keys', 'ttl', 'delete', 'delete_pattern',
            'flush', 'cacheable']

_CACHE_CONNECTIONS = {}
_LOGGER = logging.getLogger(__name__)


def _create_connection(alias):
    global_conf = config.get_global()
    if alias not in global_conf.get('CACHES', {}):
        raise ERROR_CACHE_CONFIGURATION(alias=alias)

    cache_conf = copy.deepcopy(global_conf['CACHES'][alias])

    engine = cache_conf.get('engine')

    if engine:
        del cache_conf['engine']

    if 'backend' in cache_conf:
        del cache_conf['backend']

    if engine == 'LocalCache':
        return LocalCache(alias, cache_conf)
    elif engine == 'RedisCache':
        return RedisCache(alias, cache_conf)
    else:
        raise ERROR_CACHE_ENGINE_UNDEFINE(alias=alias)


def connect(func):
    def wrapper(*args, alias='default', **kwargs):
        if alias not in _CACHE_CONNECTIONS:
            _CACHE_CONNECTIONS[alias] = _create_connection(alias)

        return func(_CACHE_CONNECTIONS[alias], *args, **kwargs)

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


def cacheable(key=None, value=None, expire=None, action='cache', alias='default'):
    def wrapper(func):
        def wrapped_func(*args, **kwargs):
            if is_set(alias):
                args_dict = _change_args_to_dict(func, args)
                args_dict.update(kwargs)
                cache_key = _make_cache_key(key, args_dict)
                if action in ['cache']:
                    data = get(cache_key, alias=alias)
                    if data is not None:
                        return data

            result = func(*args, **kwargs)

            if is_set(alias):
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

                    set(cache_key, cache_value, expire=expire, alias=alias)

                if action in ['delete']:
                    delete(cache_key, alias=alias)

            return result

        return wrapped_func

    return wrapper


def is_set(alias='default'):
    global_conf = config.get_global()
    if global_conf.get('CACHES', {}).get(alias, {}) != {}:
        return True
    else:
        return False


@connect
def get(cache_cls, key):
    return cache_cls.get(key)


@connect
def set(cache_cls, key, value, expire=None):
    return cache_cls.set(key, value, expire=expire)


@connect
def increment(cache_cls, key, amount=1):
    return cache_cls.increment(key, amount)


@connect
def decrement(cache_cls, key, amount=1):
    return cache_cls.decrement(key, amount)


@connect
def keys(cache_cls, pattern):
    return cache_cls.keys(pattern)


@connect
def ttl(cache_cls, key):
    return cache_cls.ttl(key)


@connect
def delete(cache_cls, *keys):
    return cache_cls.delete(*keys)


@connect
def delete_pattern(cache_cls, pattern):
    return cache_cls.delete_pattern(pattern)


@connect
def flush(cache_cls, is_async=False):
    return cache_cls.flush(is_async)
