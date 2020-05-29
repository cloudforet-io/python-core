# -*- coding: utf-8 -*-

import logging
from spaceone.core import config
from spaceone.core.error import *

__init__ = ['put', 'get']

_QUEUE_CONNECTIONS = {}
LOGGER = logging.getLogger(__name__)


def _create_connection(topic):
    global_conf = config.get_global()
    if 'QUEUES' not in global_conf:
        raise ERROR_CONFIGURATION(key='QUEUES')

    queue_conf = global_conf['QUEUES'][topic].copy()
    backend = queue_conf.pop('backend', None)
    module_name, class_name = backend.rsplit('.', 1)
    queue_module = __import__(module_name, fromlist=[class_name])
    return getattr(queue_module, class_name)(queue_conf)


def connection(func):
    def wrapper(*args, **kwargs):
        topic = args[0]
        new_args = tuple(list(args[1:]))
        if topic not in _QUEUE_CONNECTIONS:
            _QUEUE_CONNECTIONS[topic] = _create_connection(topic)

        return func(_QUEUE_CONNECTIONS[topic], *new_args, **kwargs)

    return wrapper


@connection
def get(queue_cls):
    return queue_cls.get()


@connection
def put(queue_cls, key):
    return queue_cls.put(key)


class BaseQueue(object):
    initialized = None
    def initialize(self):
        """
        Re initilize if there are problem
        """
        pass

    def get(self):
        """
        Returns:
            queue_value(any)
        """
        raise NotImplementedError('queue.get not implemented!')

    def put(self, key):
        """
        Args:
            key(any)

        Returns:
            True | False
        """
        raise NotImplementedError('queue.set not implemented!')
