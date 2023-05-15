import traceback
import logging
from threading import local

from spaceone.core import utils, config
from spaceone.core.error import *
from opentelemetry import trace
from opentelemetry.trace import format_trace_id

__all__ = ['LOCAL_STORAGE', 'get_trace_id', 'get_transaction', 'create_transaction', 'delete_transaction',
           'Transaction']

_LOGGER = logging.getLogger(__name__)
LOCAL_STORAGE = local()


class Transaction(object):

    def __init__(self, meta=None):
        if meta:
            self._meta = meta.copy()
        else:
            self._meta = {}

        self._rollbacks = []
        self._status = 'STARTED'
        self._set_trace_id()
        self._event_handlers = []

        LOCAL_STORAGE.transaction = self

    def __repr__(self):
        return f"<Transaction ({self.resource}.{self.verb})>"

    def _set_trace_id(self):
        traceparent = self._meta.get('traceparent')
        if traceparent:
            self._meta['trace_id'] = traceparent.split('-')[1]
        else:
            trace_id = utils.generate_trace_id()
            self._meta['trace_id'] = format(trace_id, 'x')

    def _set_meta(self, meta: dict = None):
        if meta:
            self._meta = meta.copy()
        else:
            self._meta = {}

    @property
    def id(self):
        return self._meta['trace_id']

    @property
    def service(self):
        return self._meta.get('service')

    @service.setter
    def service(self, value):
        self._meta['service'] = value

    @property
    def resource(self):
        return self._meta.get('resource')

    @resource.setter
    def resource(self, value):
        self._meta['resource'] = value

    @property
    def verb(self):
        return self._meta.get('verb')

    @verb.setter
    def verb(self, value):
        self._meta['verb'] = value

    @property
    def url(self):
        return self._meta.get('url')

    @url.setter
    def url(self, value):
        self._meta['url'] = value

    @property
    def status(self):
        return self._status

    @status.setter
    def status(self, value):
        if value not in ['IN_PROGRESS', 'SUCCESS', 'FAILURE']:
            raise ERROR_TRANSACTION_STATUS(status=value)
        self._status = value

    def add_rollback(self, fn, *args, **kwargs):
        self._rollbacks.insert(0, {
            'fn': fn,
            'args': args,
            'kwargs': kwargs
        })

    def execute_rollback(self):
        for rollback in self._rollbacks:
            try:
                rollback['fn'](*rollback['args'], **rollback['kwargs'])
            except Exception:
                _LOGGER.info(f'[ROLLBACK-ERROR] {self}')
                _LOGGER.info(traceback.format_exc())

    @property
    def meta(self):
        return self._meta

    def set_meta(self, key, value):
        self._meta[key] = value

    def get_meta(self, key, default=None):
        return self._meta.get(key, default)

    def get_connection_meta(self) -> list:
        keys = ['token', 'domain_id']
        result = []
        for key in keys:
            result.append((key, self.get_meta(key)))
        return result

    def notify_event(self, message):
        for handler in self._event_handlers:
            if not isinstance(message, dict):
                message = {'message': str(message)}

            handler.notify(self, 'IN_PROGRESS', message)


def get_trace_id() -> [str, None]:
    if trace_id := format_trace_id(trace.get_current_span().get_span_context().trace_id):
        return trace_id
    else:
        return None


def get_transaction(trace_id: str = None) -> [Transaction, None]:
    if trace_id:
        return getattr(LOCAL_STORAGE, trace_id, None)
    else:
        return getattr(LOCAL_STORAGE, get_trace_id(), None) if get_trace_id() else None


def create_transaction(resource: str = None, verb: str = None, trace_id: str = None,
                       meta: dict = None) -> Transaction:
    transaction = Transaction(resource, verb, trace_id, meta)
    setattr(LOCAL_STORAGE, trace_id, transaction)
    return transaction


def delete_transaction(trace_id: str = None, execute_rollback=False) -> None:
    transaction = get_transaction(trace_id)
    if transaction:
        if execute_rollback:
            transaction.execute_rollback()
        delattr(LOCAL_STORAGE, trace_id)
