import threading
import traceback
import logging
from threading import local

from spaceone.core import utils
from opentelemetry import trace
from opentelemetry.trace import format_trace_id
from opentelemetry.trace.span import TraceFlags

__all__ = [
    "LOCAL_STORAGE",
    "get_transaction",
    "create_transaction",
    "delete_transaction",
    "Transaction",
]

_LOGGER = logging.getLogger(__name__)
LOCAL_STORAGE = local()


class Transaction(object):
    def __init__(
        self,
        service: str = None,
        resource: str = None,
        verb: str = None,
        trace_id: str = None,
        meta=None,
    ):
        self._id = None
        self._thread_id = str(threading.current_thread().ident)
        self._service = service
        self._resource = resource
        self._verb = verb
        self._rollbacks = []
        self._init_meta(meta)
        self._set_trace_id(trace_id)

    def __repr__(self) -> str:
        return f"<Transaction ({self._resource}.{self._verb})>"

    def _set_trace_id(self, trace_id: str = None) -> None:
        if trace_id:
            self._id = trace_id
        else:
            self._id = format_trace_id(utils.generate_trace_id())

    def _init_meta(self, meta: dict = None):
        if meta:
            self._meta = meta.copy()
        else:
            self._meta = {}

    @property
    def id(self) -> str:
        return self._id

    @property
    def thread_id(self) -> str:
        return self._thread_id

    @property
    def service(self) -> str:
        return self._service

    @property
    def resource(self) -> str:
        return self._resource

    @property
    def verb(self) -> str:
        return self._verb

    def add_rollback(self, fn: callable, *args, **kwargs) -> None:
        self._rollbacks.insert(0, {"fn": fn, "args": args, "kwargs": kwargs})

    def execute_rollback(self) -> None:
        for rollback in self._rollbacks:
            try:
                rollback["fn"](*rollback["args"], **rollback["kwargs"])
            except Exception as e:
                _LOGGER.info(f"[ROLLBACK-ERROR] {self}: {e}")
                _LOGGER.info(traceback.format_exc())

    @property
    def meta(self) -> dict:
        return self._meta

    def set_meta(self, key: str, value: any) -> None:
        self._meta[key] = value

    def get_meta(self, key: str, default: any = None):
        return self._meta.get(key, default)


def get_transaction(is_create: bool = True) -> [Transaction, None]:
    current_span_context = trace.get_current_span().get_span_context()
    thread_id = str(threading.current_thread().ident)

    if current_span_context.trace_flags == TraceFlags.SAMPLED:
        trace_id_from_current_span = format_trace_id(current_span_context.trace_id)
        return getattr(LOCAL_STORAGE, trace_id_from_current_span, None)
    elif hasattr(LOCAL_STORAGE, thread_id):
        return getattr(LOCAL_STORAGE, thread_id, None)
    elif is_create:
        return create_transaction(thread_id=thread_id)
    else:
        return None


def create_transaction(
    service: str = None,
    resource: str = None,
    verb: str = None,
    trace_id: str = None,
    meta: dict = None,
    thread_id: str = None,
) -> Transaction:
    transaction = Transaction(service, resource, verb, trace_id, meta)

    if thread_id:
        setattr(LOCAL_STORAGE, thread_id, transaction)
    else:
        setattr(LOCAL_STORAGE, transaction.id, transaction)

    return transaction


def delete_transaction() -> None:
    if transaction := get_transaction(is_create=False):
        if hasattr(LOCAL_STORAGE, transaction.id):
            delattr(LOCAL_STORAGE, transaction.id)

    thread_id = str(threading.current_thread().ident)
    if hasattr(LOCAL_STORAGE, thread_id):
        delattr(LOCAL_STORAGE, thread_id)
