import functools
import inspect
import logging
import types
import copy
import traceback

from spaceone.core import config
from spaceone.core.error import *
from spaceone.core.locator import Locator
from spaceone.core.logger import set_logger
from spaceone.core.transaction import Transaction

_LOGGER = logging.getLogger(__name__)


class BaseService(object):

    def __init__(self, metadata: dict = None, transaction: Transaction = None, **kwargs):
        self.func_name = None
        self.is_with_statement = False

        if metadata is None:
            metadata = {}

        if transaction:
            self.transaction = transaction
        else:
            self.transaction = Transaction(metadata)

        if config.get_global('SET_LOGGING', True):
            set_logger(transaction=self.transaction)

        self.locator = Locator(self.transaction)
        self.handler = {
            'authentication': {'handlers': [], 'methods': []},
            'authorization': {'handlers': [], 'methods': []},
            'mutation': {'handlers': [], 'methods': []},
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
        if self.transaction.status == 'IN_PROGRESS':
            self.transaction.status = 'SUCCESS'


def transaction(func=None, append_meta=None):
    def wrapper(func):
        @functools.wraps(func)
        def wrapped_func(self, params):
            return _pipeline(func, self, params, append_meta)

        return wrapped_func

    if func:
        return wrapper(func)

    return wrapper


def _pipeline(func, self, params, append_meta):
    try:
        self.func_name = func.__name__
        _LOGGER.info('(REQEUST) =>', extra={'parameter': copy.deepcopy(params)})

        # 0. Set Extra Metadata
        if append_meta and isinstance(append_meta, dict):
            for key, value in append_meta.items():
                self.transaction.set_meta(key, value)

        # 1. Start Event: Ignore exceptions
        if _check_handler_method(self, 'event'):
            for handler in self.handler['event']['handlers']:
                try:
                    handler.notify(self.transaction, 'STARTED', params)
                except Exception as e:
                    _LOGGER.error(f'{handler.__class__.__name__} Error (STARTED): {e}')

        # 2. Authentication:
        if _check_handler_method(self, 'authentication'):
            for handler in self.handler['authentication']['handlers']:
                handler.verify(params)

        # 3. Authorization
        if _check_handler_method(self, 'authorization'):
            for handler in self.handler['authorization']['handlers']:
                handler.verify(params)

        # 4. Mutation
        if _check_handler_method(self, 'mutation'):
            for handler in self.handler['mutation']['handlers']:
                params = handler.request(params)

        # 5. Service Body
        self.transaction.status = 'IN_PROGRESS'
        response_or_iterator = func(self, params)

        # 6. Response Handlers
        if isinstance(response_or_iterator, types.GeneratorType):
            return _generate_response(self, response_or_iterator)
        else:
            response_or_iterator = _response_mutation_handler(self, response_or_iterator)
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


def _error_handler(self, error):
    if not isinstance(error, ERROR_BASE):
        error = ERROR_UNKNOWN(message=error)

    self.transaction.status = 'FAILURE'

    # Failure Event
    if _check_handler_method(self, 'event'):
        for handler in self.handler['event']['handlers']:
            try:
                handler.notify('FAILURE', {'error_code': error.error_code, 'message': error.message})
            except Exception as e:
                _LOGGER.error(f'{handler.__class__.__name__} Error (FAILURE): {e}')

    _LOGGER.error(f'(Error) => {error.message} {error}',
                  extra={'error_code': error.error_code,
                         'error_message': error.message,
                         'traceback': traceback.format_exc()})
    self.transaction.execute_rollback()

    return error


def _success_handler(self, response):
    if _check_handler_method(self, 'event'):
        for handler in self.handler['event']['handlers']:
            try:
                handler.notify('SUCCESS', response)
            except Exception as e:
                _LOGGER.error(f'{handler.__class__.__name__} Error (SUCCESS): {e}')


def _response_mutation_handler(self, response):
    if _check_handler_method(self, 'mutation'):
        for handler in list(reversed(self.handler['mutation']['handlers'])):
            response = handler.response(response)

    return response


def _generate_response(self, response_iterator):
    for response in response_iterator:
        response = _response_mutation_handler(self, response)
        _success_handler(self, response)
        yield response


def authentication_handler(func=None, methods='*', exclude=None):
    return _set_handler(func, 'authentication', methods, exclude)


def authorization_handler(func=None, methods='*', exclude=None):
    return _set_handler(func, 'authorization', methods, exclude)


def mutation_handler(func=None, methods='*', exclude=None):
    return _set_handler(func, 'mutation', methods, exclude)


def event_handler(func=None, methods='*', exclude=None):
    return _set_handler(func, 'event', methods, exclude)


def _set_handler(func, handler_type, methods, exclude=None):
    if exclude is None:
        exclude = []

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
        handlers = config.get_handler(handler_type)
        for handler in handlers:
            module_name, class_name = handler['backend'].rsplit('.', 1)
            handler_module = __import__(module_name, fromlist=[class_name])
            handler_conf = handler.copy()
            del handler_conf['backend']

            self.handler[handler_type]['handlers'].append(
                getattr(handler_module, class_name)(self.transaction, handler_conf))

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
