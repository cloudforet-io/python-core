# -*- coding: utf-8 -*-

import traceback
import logging
from spaceone.core import utils
from spaceone.core.error import *
from spaceone.core.locator import Locator


_LOGGER = logging.getLogger(__name__)


class Transaction(object):

    def __init__(self, meta={}):
        self._rollbacks = []
        self._meta = meta.copy()
        self._state = 'STARTED'
        self._set_transaction_id()
        self._event_handlers = []

    def __repr__(self):
        return f"<Transaction ({self.api_class}.{self.method})>"

    def _set_transaction_id(self):
        if 'transaction_id' not in self._meta:
            self._meta['transaction_id'] = utils.generate_id('tnx')

    @property
    def id(self):
        return self._meta['transaction_id']

    @property
    def service(self):
        return self._meta.get('service')

    @service.setter
    def service(self, value):
        self._meta['service'] = value

    @property
    def api_class(self):
        return self._meta.get('api_class')

    @api_class.setter
    def api_class(self, value):
        self._meta['api_class'] = value

    @property
    def method(self):
        return self._meta.get('method')

    @method.setter
    def method(self, value):
        self._meta['method'] = value

    @property
    def state(self):
        return self._state

    @state.setter
    def state(self, value):
        if value not in ['IN-PROGRESS', 'SUCCESS', 'FAILURE']:
            raise ERROR_TRANSACTION_STATE(state=value)
        self._state = value

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

    def get_meta(self, key):
        return self._meta.get(key)

    def get_connection_meta(self) -> list:
        """ metadata for MS call
        token, domain_id ...

        Returns:
            - list of tuple
            ex) [('token','xxxxx'),('domain_id','yyyy') ...]
        """
        keys = ['token', 'domain_id', 'transaction_id']
        result = []
        for key in keys:
            result.append((key, self.get_meta(key)))
        return result

    def notify_event(self, message):
        for handler in self._event_handlers:
            if not isinstance(message, dict):
                message = {'message': str(message)}

            handler.notify(self, 'IN-PROGRESS', message)
