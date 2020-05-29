import functools
import inspect
import logging
import types
import copy
import traceback
from dateutil.parser import parse
from datetime import datetime

from spaceone.core import config
from spaceone.core.error import *
from spaceone.core.locator import Locator
from spaceone.core.logger import set_logger
from spaceone.core.transaction import Transaction

__all__ = ['BaseService', 'check_required', 'append_query_filter', 'change_timestamp_value', 'change_timestamp_filter',
           'append_keyword_filter', 'transaction', 'authentication_handler', 'authorization_handler', 'event_handler']

_LOGGER = logging.getLogger(__name__)


class BaseService(object):

    def __init__(self, metadata={}, transaction=None):
        self.func_name = None
        self.is_with_statement = False

        if transaction:
            self.transaction = transaction
        else:
            self.transaction = Transaction(metadata)

        set_logger(transaction=self.transaction)
        self.locator = Locator(self.transaction)
        self.handler = {
            'authentication': {'handlers': [], 'methods': []},
            'authorization': {'handlers': [], 'methods': []},
            'event': {'handlers': [], 'methods': []},
        }

    def __enter__(self):
        self.is_with_statement = True
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type:
            error = _error_handler(self, exc_val)
            raise error

    def __del__(self):
        if self.transaction.state == 'IN-PROGRESS':
            self.transaction.state = 'SUCCESS'


def check_required(required_keys):
    def wrapper(func):
        @functools.wraps(func)
        def wrapped_func(cls, params):
            for key in required_keys:
                if key not in params:
                    raise ERROR_REQUIRED_PARAMETER(key=key)

            return func(cls, params)

        return wrapped_func

    return wrapper


def append_query_filter(filter_keys):
    def wrapper(func):
        @functools.wraps(func)
        def wrapped_func(cls, params):
            query = params.get('query', {})
            query['filter'] = query.get('filter', [])

            for key in filter_keys:
                if params.get(key):
                    if isinstance(params[key], list):
                        query['filter'].append({'k': key, 'v': params[key], 'o': 'in'})
                    else:
                        query['filter'].append({'k': key, 'v': params[key], 'o': 'eq'})

            params['query'] = query

            return func(cls, params)

        return wrapped_func

    return wrapper


def append_keyword_filter(keywords=[]):
    def wrapper(func):
        @functools.wraps(func)
        def wrapped_func(cls, params):
            query = params.get('query', {})
            if 'keyword' in query:
                query['filter_or'] = query.get('filter_or', [])

                for key in keywords:
                    query['filter_or'].append({
                        'k': key,
                        'v': query['keyword'],
                        'o': 'contain'
                    })

                del query['keyword']
                params['query'] = query

            return func(cls, params)

        return wrapped_func

    return wrapper


def change_timestamp_value(timestamp_keys=[], timestamp_format='google_timestamp'):
    def wrapper(func):
        @functools.wraps(func)
        def wrapped_func(cls, params):
            change_params = {}

            for key, value in params.items():
                if key in timestamp_keys:
                    value = _convert_datetime_from_timestamp(value, key, timestamp_format)

                change_params[key] = value

            return func(cls, change_params)

        return wrapped_func

    return wrapper


def change_timestamp_filter(filter_keys=[], timestamp_format='google_timestamp'):
    def wrapper(func):
        @functools.wraps(func)
        def wrapped_func(cls, params):
            query = params.get('query', {})
            query_filter = query.get('filter')
            query_filter_or = query.get('filter_or')
            if query_filter:
                query['filter'] = _change_timestamp_condition(query_filter, filter_keys,
                                                              'filter', timestamp_format)

            if query_filter_or:
                query['filter_or'] = _change_timestamp_condition(query_filter_or, filter_keys,
                                                                 'filter_or', timestamp_format)

            params['query'] = query

            return func(cls, params)

        return wrapped_func

    return wrapper


def _change_timestamp_condition(query_filter, filter_keys, filter_type, timestamp_format):
    change_filter = []

    for condition in query_filter:
        key = condition.get('k') or condition.get('key')
        value = condition.get('v') or condition.get('value')
        operator = condition.get('o') or condition.get('operator')

        if key in filter_keys:
            value = _convert_datetime_from_timestamp(value, f'query.{filter_type}.{key}', timestamp_format)

        change_filter.append({
            'key': key,
            'value': value,
            'operator': operator
        })

    return change_filter


def _convert_datetime_from_timestamp(timestamp, key, timestamp_format):
    type_message = 'google.protobuf.Timestamp({seconds: <second>, nanos: <nano second>})'

    try:
        if timestamp_format == 'iso8601':
            type_message = 'ISO 8601(YYYY-MM-DDThh:mm:ssTZD)'
            return parse(timestamp)
        else:
            seconds = timestamp['seconds']
            nanos = timestamp.get('nanos')

            # TODO: change nanoseconds to timestamp

            return datetime.utcfromtimestamp((int(seconds)))
    except Exception as e:
        raise ERROR_INVALID_PARAMETER_TYPE(key=key, type=type_message)


def transaction(func):
    @functools.wraps(func)
    def wrapper(self, params):
        return _pipeline(func, self, params)

    return wrapper


def _error_handler(self, error):
    if not isinstance(error, ERROR_BASE):
        error = ERROR_UNKNOWN(message=error)

    # Failure Event
    if _check_handler_method(self, 'event'):
        for handler in self.handler['event']['handlers']:
            handler.notify(self.transaction, 'FAILURE', {
                'error_code': error.error_code,
                'message': error.message
            })

    self.transaction.state = 'FAILURE'
    _LOGGER.error(f'(Error) => {error.message} {error}',
                  extra={'error_code': error.error_code,
                         'error_message': error.message,
                         'traceback': traceback.format_exc()})
    self.transaction.execute_rollback()

    return error


def _success_handler(self, response):
    if _check_handler_method(self, 'event'):
        for handler in self.handler['event']['handlers']:
            handler.notify(self.transaction, 'SUCCESS', response)


def _generate_response(self, response_iterator):
    for response in response_iterator:
        _success_handler(self, response)
        yield response


def _pipeline(func, self, params):
    try:
        self.func_name = func.__name__
        _LOGGER.info('(REQEUST) =>', extra={'parameter': copy.deepcopy(params)})

        # 1. Authentication
        if _check_handler_method(self, 'authentication'):
            for handler in self.handler['authentication']['handlers']:
                params = handler.notify(self.transaction, params)

        # 2. Authorization
        if _check_handler_method(self, 'authorization'):
            for handler in self.handler['authorization']['handlers']:
                params = handler.notify(self.transaction, params)

        # 3. Start Event
        if _check_handler_method(self, 'event'):
            for handler in self.handler['event']['handlers']:
                handler.notify(self.transaction, 'START', params)

        # 4. Service Body
        self.transaction.state = 'IN-PROGRESS'
        response_or_iterator = func(self, params)

        # 5. Success Event
        if isinstance(response_or_iterator, types.GeneratorType):
            return _generate_response(self, response_or_iterator)
        else:
            _success_handler(self, response_or_iterator)
            return response_or_iterator

    except ERROR_BASE as e:
        if not self.is_with_statement:
            _error_handler(self, e)

        raise e

    except Exception as e:
        error = ERROR_UNKNOWN(message=e)

        if not self.is_with_statement:
            _error_handler(self, error)

        raise error


def authentication_handler(func=None, methods='*', exclude=[]):
    return _set_handler(func, 'authentication', methods, exclude)


def authorization_handler(func=None, methods='*', exclude=[]):
    return _set_handler(func, 'authorization', methods, exclude)


def event_handler(func=None, methods='*', exclude=[]):
    return _set_handler(func, 'event', methods, exclude)


def _set_handler(func, handler_type, methods, exclude):
    def wrapper(cls):
        @functools.wraps(cls)
        def wrapped_cls(*args, **kwargs):
            self = cls(*args, **kwargs)
            _load_handler(self, handler_type)
            return _bind_handler(self, handler_type, methods, exclude)

        return wrapped_cls

    if func:
        return wrapper(func)

    return wrapper


def _load_handler(self, handler_type):
    try:
        handlers = config.get_handlers(handler_type)
        for handler in handlers:
            module_name, class_name = handler['backend'].rsplit('.', 1)
            handler_module = __import__(module_name, fromlist=[class_name])
            handler_conf = handler.copy()
            del handler_conf['backend']

            self.handler[handler_type]['handlers'].append(
                getattr(handler_module, class_name)(handler_conf))

    except ERROR_BASE as error:
        raise error

    except Exception as e:
        raise ERROR_HANDLER(handler_type=handler_type, reason=e)


def _get_service_methods(self):
    service_methods = []
    for f_name, f_object in inspect.getmembers(self.__class__, predicate=inspect.isfunction):
        if not f_name.startswith('__'):
            service_methods.append(f_name)

    return service_methods


def _bind_handler(self, handler_type, methods, exclude):
    handler_methods = []
    if methods == '*':
        handler_methods = _get_service_methods(self)
    else:
        if isinstance(methods, list):
            service_methods = _get_service_methods(self)
            for method in methods:
                if method in service_methods:
                    handler_methods.append(method)

    if isinstance(exclude, list):
        handler_methods = list(set(handler_methods) - set(exclude))

    self.handler[handler_type]['methods'] = \
        list(set(self.handler[handler_type]['methods'] + handler_methods))

    return self


def _check_handler_method(self, handler_type):
    if self.func_name in self.handler[handler_type]['methods']:
        return True
    else:
        return False
