import functools
import logging
import copy
from typing import Generator, Union, Literal

from opentelemetry import trace
from opentelemetry.trace import format_trace_id
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator

from spaceone.core.base import CoreObject
from spaceone.core.error import *
from spaceone.core.service.utils import *
from spaceone.core.transaction import get_transaction, create_transaction, delete_transaction
from spaceone.core.handler import *
from spaceone.core.service import utils as service_utils

_HANDLER_TYPE = Literal['authentication', 'authorization', 'mutation', 'event']
_ERROR_TYPE = Literal['INVALID_ARGUMENT', 'UNKNOWN']
_LOGGER = logging.getLogger(__name__)
_TRACER = trace.get_tracer(__name__)

__all__ = [
    'BaseService',
    'transaction',
    'authorization_handler',
    'authentication_handler',
    'mutation_handler',
    'event_handler',
    'convert_model',
    'change_only_key',
    'check_required',
    'set_query_page_limit',
    'append_query_filter',
    'change_tag_filter',
    'append_keyword_filter',
    'change_timestamp_value',
    'change_date_value',
    'change_timestamp_filter'
]


class BaseService(CoreObject):

    _plugin_methods = {}
    service = None
    resource = None
    permission_group = None
    _handler_state = {
        'authentication': False,
        'authorization': False,
        'mutation': False,
        'event': False,
    }

    def __init__(self, metadata: dict = None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.current_span_context = None
        self._metadata = metadata or {}
        self._methods = {}

    @property
    def metadata(self) -> dict:
        return self._metadata

    @classmethod
    def set_plugin_method(cls, method_name: str, func: callable) -> None:
        cls._plugin_methods[method_name] = func

    @classmethod
    def get_plugin_method(cls, method_name: str) -> Union[callable, None]:
        return cls._plugin_methods.get(method_name)

    @classmethod
    def enable_handler(cls, handler_type: _HANDLER_TYPE) -> None:
        cls._handler_state[handler_type] = True

    @classmethod
    def get_handler_state(cls, handler_type: _HANDLER_TYPE) -> bool:
        return cls._handler_state[handler_type]


def transaction(scope: str = None, verb: str = None) -> callable:
    def wrapper(func: callable):
        @functools.wraps(func)
        def wrapped_func(self, params: dict):
            _verb = verb or func.__name__
            _traceparent = self.metadata.get('traceparent')
            _scope = _make_scope_full_string(self.service, self.resource, _verb, scope)

            with _TRACER.start_as_current_span(
                    f'{self.resource}.{_verb}',
                    context=_get_span_context(_traceparent)
            ) as span:
                self.current_span_context = span.get_span_context()
                trace_id = format_trace_id(self.current_span_context.trace_id)
                create_transaction(
                    self.service, self.resource, _verb, trace_id, self._metadata
                )
                return _pipeline(func, self, params, _scope, self._handler_state)

        return wrapped_func

    return wrapper


def _make_scope_full_string(
        service: str, resource: str, verb: str, scope: Union[str, None]
) -> Union[str, None]:

    if scope is None or scope in ['system', 'public']:
        return scope
    else:
        return f'{service}.{resource}.{verb}:{scope}'


def _get_span_context(traceparent: str = None):
    if traceparent:
        carrier = {'traceparent': traceparent}
        return TraceContextTextMapPropagator().extract(carrier)
    else:
        return None


def _pipeline(
        func: callable,
        self: BaseService,
        params: Union[Generator, dict, None],
        scope: str,
        handler_state: dict,
) -> Union[Generator, dict, None]:
    try:
        print_info_log = str(self.metadata.get('disable_info_log', 'false')).lower() != 'true'

        with _TRACER.start_as_current_span('PreProcessing'):
            # 1. Event - Start
            if handler_state['event']:
                for handler in get_event_handlers():
                    with _TRACER.start_as_current_span(handler.__class__.__name__):
                        handler.notify('STARTED', params)

            # 2. Authentication
            if handler_state['authentication']:
                for handler in get_authentication_handlers():
                    with _TRACER.start_as_current_span(handler.__class__.__name__):
                        handler.verify(scope, params)

            # 3. Authorization
            if handler_state['authorization']:
                for handler in get_authentication_handlers():
                    with _TRACER.start_as_current_span(handler.__class__.__name__):
                        handler.verify(scope, params)

            # 4. Print Request Info Log
            if print_info_log:
                _LOGGER.info('(REQUEST) =>', extra={'parameter': copy.deepcopy(params)})

            # 5. Request Mutation
            if handler_state['mutation']:
                for handler in get_mutation_handlers():
                    with _TRACER.start_as_current_span(handler.__class__.__name__):
                        params = handler.request(params)

        # 6. Service Body
        with _TRACER.start_as_current_span(f'Body', links=[trace.Link(self.current_span_context)]):
            # 6. Event - In Progress
            if handler_state['event']:
                for handler in get_event_handlers():
                    with _TRACER.start_as_current_span(handler.__class__.__name__):
                        handler.notify('IN_PROGRESS', params)

            response_or_iterator = func(self, params)
            if isinstance(response_or_iterator, Generator):
                # Skip PostProcessing
                return response_or_iterator

        with _TRACER.start_as_current_span('PostProcessing'):
            # 7. Response Mutation
            if handler_state['mutation']:
                for handler in get_mutation_handlers(reverse=True):
                    with _TRACER.start_as_current_span(handler.__class__.__name__):
                        response_or_iterator = handler.response(response_or_iterator)

            # 8. Event - Success
            if handler_state['event']:
                for handler in get_event_handlers():
                    with _TRACER.start_as_current_span(handler.__class__.__name__):
                        handler.notify('SUCCESS', response_or_iterator)

            # 9. Print Response Info Log
            if print_info_log:
                _LOGGER.info(f'(RESPONSE) => SUCCESS')

        return response_or_iterator

    except ERROR_INVALID_ARGUMENT as e:
        _error_handler('INVALID_ARGUMENT', e, handler_state['event'])
        raise e

    except ERROR_BASE as e:
        _error_handler('UNKNOWN', e, handler_state['event'])
        raise e

    except Exception as e:
        error = ERROR_UNKNOWN(message=e)
        _error_handler('UNKNOWN', error, handler_state['event'])
        raise error

    finally:
        delete_transaction()


def _error_handler(
        error_type: _ERROR_TYPE,
        error: ERROR_BASE,
        event_handler_state: bool,
) -> None:
    error.meta['skip_error_log'] = True

    # Event - Failure
    if event_handler_state:
        for handler in get_event_handlers():
            handler.notify(
                'FAILURE',
                {
                    'error_code': error.error_code,
                    'message': error.message
                }
            )

    _LOGGER.error(f'(Error) => {error.message} {error}', exc_info=True)

    if tnx := get_transaction(is_create=False):
        tnx.execute_rollback()


def authentication_handler(cls: BaseService):
    return _set_handler(cls, 'authentication')


def authorization_handler(cls: BaseService):
    return _set_handler(cls, 'authorization')


def mutation_handler(cls: BaseService):
    return _set_handler(cls, 'mutation')


def event_handler(cls: BaseService):
    return _set_handler(cls, 'event')


def _set_handler(cls: BaseService, handler_type: _HANDLER_TYPE):
    cls.enable_handler(handler_type)
    return cls