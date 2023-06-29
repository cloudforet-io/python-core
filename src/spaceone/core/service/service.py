import sys
import contextlib
import functools
import inspect
import logging
import types
import copy
import traceback

from opentelemetry import trace, metrics
from opentelemetry.trace import NonRecordingSpan, format_trace_id
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator

from spaceone.core import config, utils
from spaceone.core.base import CoreObject
from spaceone.core.error import *
from spaceone.core.locator import Locator
from spaceone.core.transaction import Transaction, get_transaction, create_transaction, delete_transaction

from opentelemetry import trace

_LOGGER = logging.getLogger(__name__)
_TRACER = trace.get_tracer(__name__)


class BaseService(CoreObject):

    def __init__(self, metadata: dict = None, **kwargs):
        super().__init__(**kwargs)
        self.func_name = None
        # self.is_with_statement = False
        self.current_span_context = None
        self._metadata = metadata or {}

        self.handler = {
            'authentication': {'handlers': [], 'methods': []},
            'authorization': {'handlers': [], 'methods': []},
            'mutation': {'handlers': [], 'methods': []},
            'event': {'handlers': [], 'methods': []},
        }

        self.handler_exclude_apis = config.get_global('HANDLER_EXCLUDE_APIS', {})
        self.enable_stack_info = config.get_global('ENABLE_STACK_INFO', False)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type:
            raise exc_val


def transaction(func=None, verb=None, append_meta=None):
    def wrapper(func):
        @functools.wraps(func)
        def wrapped_func(self, params):
            _resource = self._metadata.get('resource') or self.__class__.__name__ or getattr(self, 'resource')
            _verb = verb or func.__name__
            with _TRACER.start_as_current_span(f'{_resource}.{_verb}',
                                               context=_get_span_context(self._metadata)) as span:
                self.current_span_context = span.get_span_context()
                trace_id = format_trace_id(self.current_span_context.trace_id)
                transaction = create_transaction(_resource, _verb, trace_id, self._metadata)
                return _pipeline(func, self, params, append_meta)

        return wrapped_func

    return wrapper(func) if func else wrapper


def _get_span_context(metadata):
    if traceparent := metadata.get('traceparent'):
        carrier = {'traceparent': traceparent}
        return TraceContextTextMapPropagator().extract(carrier)
    else:
        return None


def _pipeline(func, self, params, append_meta):
    try:
        self.func_name = func.__name__

        # 0. Set Extra Metadata
        if append_meta and isinstance(append_meta, dict):
            for key, value in append_meta.items():
                self.transaction.set_meta(key, value)
        with _TRACER.start_as_current_span('PreProcessing') as span:
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
                    with _TRACER.start_as_current_span(handler.__class__.__name__) as span:
                        handler.verify(params)

            # 3. Authorization
            if _check_handler_method(self, 'authorization'):
                for handler in self.handler['authorization']['handlers']:
                    with _TRACER.start_as_current_span(handler.__class__.__name__) as span:
                        handler.verify(params)

            # 4. Print Info Log
            disable_info_log = str(self.transaction.get_meta('disable_info_log', 'false')).lower()
            if disable_info_log != 'true':
                _LOGGER.info('(REQUEST) =>', extra={'parameter': copy.deepcopy(params)})

            # 5. Mutation
            if _check_handler_method(self, 'mutation'):
                for handler in self.handler['mutation']['handlers']:
                    with _TRACER.start_as_current_span(handler.__class__.__name__) as span:
                        params = handler.request(params)

        # 6. Service Body
        with _TRACER.start_as_current_span(f'ServiceBody',
                                           links=[trace.Link(self.current_span_context)]) as span:
            response_or_iterator = func(self, params)

            # debug code for memory leak
            # local_storage = LOCAL_STORAGE.__dict__
            # _LOGGER.info(
            #     f'[BaseService] {get_transaction()} / number of items in local storage: {len(local_storage)} / items => {local_storage}')

            return response_or_iterator

        # # 7. Response Handlers
        # with _TRACER.start_as_current_span('PostProcessing') as span:
        #     if isinstance(response_or_iterator, types.GeneratorType):
        #         return _generate_response(self, response_or_iterator)
        #     else:
        #         response_or_iterator = _response_mutation_handler(self, response_or_iterator)
        #         _success_handler(self, response_or_iterator)
        #         return response_or_iterator

    except ERROR_INVALID_ARGUMENT as e:
        _error_handler(self, e)
        raise e

    except ERROR_BASE as e:
        _error_handler(self, e)
        raise e

    except Exception as e:
        error = ERROR_UNKNOWN(message=e)
        _error_handler(self, error)
        raise error

    finally:
        delete_transaction()


def _error_handler(self, error):
    if not isinstance(error, ERROR_BASE):
        error = ERROR_UNKNOWN(message=error)

    error.meta['skip_error_log'] = True

    # Failure Event
    if _check_handler_method(self, 'event'):
        for handler in self.handler['event']['handlers']:
            try:
                handler.notify('FAILURE', {'error_code': error.error_code, 'message': error.message})
            except Exception as e:
                _LOGGER.error(f'{handler.__class__.__name__} Error (FAILURE): {e}')

    _LOGGER.error(f'(Error) => {error.message} {error}', exc_info=True, stack_info=self.enable_stack_info)

    if transaction := get_transaction(is_create=False):
        transaction.execute_rollback()


def _success_handler(self, response):
    if _check_handler_method(self, 'event'):
        for handler in self.handler['event']['handlers']:
            try:
                with _TRACER.start_as_current_span(handler.__class__.__name__) as span:
                    handler.notify('SUCCESS', response)
            except Exception as e:
                _LOGGER.error(f'{handler.__class__.__name__} Error (SUCCESS): {e}')


def _response_mutation_handler(self, response):
    if _check_handler_method(self, 'mutation'):
        for handler in list(reversed(self.handler['mutation']['handlers'])):
            with _TRACER.start_as_current_span(handler.__class__.__name__) as span:
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
        tnx_method = f'{self.transaction.resource}.{self.transaction.verb}'
        exclude_apis = self.handler_exclude_apis.get(handler_type, [])

        if tnx_method in exclude_apis:
            return False
        else:
            return True
    else:
        return False
