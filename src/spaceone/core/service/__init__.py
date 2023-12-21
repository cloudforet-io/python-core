import functools
import logging
import copy
from typing import Generator, Union, Literal

from opentelemetry import trace
from opentelemetry.trace import format_trace_id
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator

from spaceone.core.base import CoreObject
from spaceone.core import config
from spaceone.core.error import *
from spaceone.core.service.utils import *
from spaceone.core.transaction import (
    get_transaction,
    create_transaction,
    delete_transaction,
)
from spaceone.core.handler import *

_HANDLER_TYPE = Literal["authentication", "authorization", "mutation", "event"]
_ERROR_TYPE = Literal["INVALID_ARGUMENT", "UNKNOWN"]
_LOGGER = logging.getLogger(__name__)
_TRACER = trace.get_tracer(__name__)

__all__ = [
    "BaseService",
    "transaction",
    "authorization_handler",
    "authentication_handler",
    "mutation_handler",
    "event_handler",
    "convert_model",
    "change_value_by_rule",
    "change_only_key",
    "check_required",
    "set_query_page_limit",
    "append_query_filter",
    "change_tag_filter",
    "append_keyword_filter",
    "change_timestamp_value",
    "change_date_value",
    "change_timestamp_filter",
]
_METADATA_KEYS = [
    "token",
    "x_domain_id",
    "x_workspace_id",
    "traceparent",
    "peer",
]


class BaseService(CoreObject):
    service = None
    resource = None
    _plugin_methods = {}
    _handler_state = {}

    def __init__(self, metadata: dict = None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.current_span_context = None
        self.service = config.get_service()
        self._metadata = {}
        self._set_metadata(metadata)

    def _set_metadata(self, metadata: dict = None) -> None:
        metadata = metadata or self.transaction.meta
        for key in _METADATA_KEYS:
            if value := metadata.get(key):
                self._metadata[key] = value

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
        if cls._handler_state.get(cls.__name__) is None:
            cls._handler_state[cls.__name__] = {
                "authentication": False,
                "authorization": False,
                "mutation": False,
                "event": False,
            }

        cls._handler_state[cls.__name__][handler_type] = True

    @classmethod
    def get_handler_state(cls) -> dict:
        return cls._handler_state.get(
            cls.__name__,
            {
                "authentication": False,
                "authorization": False,
                "mutation": False,
                "event": False,
            },
        )

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass


def transaction(
    cls_func: callable = None,
    permission: str = None,
    role_types: list = None,
    exclude: list = None,
    verb: str = None,
) -> callable:
    def wrapper(func: callable):
        @functools.wraps(func)
        def wrapped_func(self, params: dict):
            _verb = verb or func.__name__
            _traceparent = self.metadata.get("traceparent")

            with _TRACER.start_as_current_span(
                f"{self.resource}.{_verb}", context=_get_span_context(_traceparent)
            ) as span:
                self.current_span_context = span.get_span_context()
                trace_id = format_trace_id(self.current_span_context.trace_id)
                create_transaction(
                    self.service, self.resource, _verb, trace_id, self.metadata
                )
                return _pipeline(
                    func,
                    self,
                    params,
                    permission,
                    role_types,
                    exclude or [],
                    self.get_handler_state(),
                )

        return wrapped_func

    return wrapper(cls_func) if cls_func else wrapper


def _get_span_context(traceparent: str = None):
    if traceparent:
        carrier = {"traceparent": traceparent}
        return TraceContextTextMapPropagator().extract(carrier)
    else:
        return None


def _pipeline(
    func: callable,
    self: BaseService,
    params: Union[Generator, dict, None],
    permission: str,
    role_types: list,
    exclude: list,
    handler_state: dict,
) -> Union[Generator, dict, None]:
    print_info_log = (
        str(self.metadata.get("disable_info_log", "false")).lower() != "true"
    )

    authentication_handler_state = (
        handler_state["authentication"] and "authentication" not in exclude
    )
    authorization_handler_state = (
        handler_state["authorization"] and "authorization" not in exclude
    )
    mutation_handler_state = handler_state["mutation"] and "mutation" not in exclude
    event_handler_state = handler_state["event"] and "event" not in exclude

    try:
        with _TRACER.start_as_current_span("PreProcessing"):
            # 1. Event - Start
            if event_handler_state:
                for handler in get_event_handlers():
                    with _TRACER.start_as_current_span(handler.__class__.__name__):
                        handler.notify("STARTED", params)

            # 2. Authentication
            if authentication_handler_state:
                for handler in get_authentication_handlers():
                    with _TRACER.start_as_current_span(handler.__class__.__name__):
                        handler.verify(params)

            # 3. Authorization
            if authorization_handler_state:
                for handler in get_authorization_handlers():
                    with _TRACER.start_as_current_span(handler.__class__.__name__):
                        handler.verify(params, permission, role_types)

            # 4. Print Request Info Log
            if print_info_log:
                _LOGGER.info("(REQUEST) =>", extra={"parameter": copy.deepcopy(params)})

            # 5. Request Mutation
            if mutation_handler_state:
                for handler in get_mutation_handlers():
                    with _TRACER.start_as_current_span(handler.__class__.__name__):
                        params = handler.request(params)

        # 6. Service Body
        with _TRACER.start_as_current_span(
            f"Body", links=[trace.Link(self.current_span_context)]
        ):
            # 6. Event - In Progress
            if event_handler_state:
                for handler in get_event_handlers():
                    with _TRACER.start_as_current_span(handler.__class__.__name__):
                        handler.notify("IN_PROGRESS", params)

            response_or_iterator = func(self, params)
            if isinstance(response_or_iterator, Generator):
                # Skip PostProcessing
                return response_or_iterator

        with _TRACER.start_as_current_span("PostProcessing"):
            # 7. Response Mutation
            if mutation_handler_state:
                for handler in get_mutation_handlers(reverse=True):
                    with _TRACER.start_as_current_span(handler.__class__.__name__):
                        response_or_iterator = handler.response(response_or_iterator)

            # 8. Event - Success
            if event_handler_state:
                for handler in get_event_handlers():
                    with _TRACER.start_as_current_span(handler.__class__.__name__):
                        handler.notify("SUCCESS", response_or_iterator)

            # 9. Print Response Info Log
            if print_info_log:
                _LOGGER.info(f"(RESPONSE) => SUCCESS")

        return response_or_iterator

    except ERROR_INVALID_ARGUMENT as e:
        _error_handler("INVALID_ARGUMENT", e, event_handler_state)
        raise e

    except ERROR_BASE as e:
        _error_handler("UNKNOWN", e, event_handler_state)
        raise e

    except Exception as e:
        error = ERROR_UNKNOWN(message=e)
        _error_handler("UNKNOWN", error, event_handler_state)
        raise error

    finally:
        delete_transaction()


def _error_handler(
    error_type: _ERROR_TYPE,
    error: ERROR_BASE,
    event_handler_state: bool,
) -> None:
    error.meta["skip_error_log"] = True

    # Event - Failure
    if event_handler_state:
        for handler in get_event_handlers():
            handler.notify(
                "FAILURE", {"error_code": error.error_code, "message": error.message}
            )

    _LOGGER.error(f"(Error) => {error.message} {error}", exc_info=True)

    if tnx := get_transaction(is_create=False):
        tnx.execute_rollback()


def authentication_handler(cls: BaseService):
    return _set_handler(cls, "authentication")


def authorization_handler(cls: BaseService):
    return _set_handler(cls, "authorization")


def mutation_handler(cls: BaseService):
    return _set_handler(cls, "mutation")


def event_handler(cls: BaseService):
    return _set_handler(cls, "event")


def _set_handler(cls: BaseService, handler_type: _HANDLER_TYPE):
    cls.enable_handler(handler_type)
    return cls
