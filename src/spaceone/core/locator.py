# -*- coding: utf-8 -*-

import logging

from spaceone.core import config
from spaceone.core.error import *

_LOGGER = logging.getLogger(__name__)


def _get_module(service, target):
    return __import__(f'spaceone.{service}.{target}', fromlist=[f'{target}'])


class Locator(object):

    def __init__(self, transaction=None):
        self.transaction = transaction

    def get_service(self, name, metadata={}):
        service = config.get_service()
        try:
            service_module = _get_module(service, 'service')
            return getattr(service_module, name)(metadata)

        except ERROR_BASE as e:
            raise e

        except Exception as e:
            raise ERROR_LOCATOR(name=name, reason=e, _meta={'type': 'service'})

    def get_manager(self, name, **kwargs):
        service = config.get_service()
        try:
            manager_module = _get_module(service, 'manager')
            return getattr(manager_module, name)(self.transaction, **kwargs)

        except ERROR_BASE as e:
            raise e

        except Exception as e:
            raise ERROR_LOCATOR(name=name, reason=e, _meta={'type': 'manager'})

    def get_connector(self, name, **kwargs):
        service = config.get_service()
        connector_conf = config.get_connector(name)
        try:
            connector_module = _get_module(service, 'connector')
            return getattr(connector_module, name)(self.transaction, connector_conf, **kwargs)

        except ERROR_BASE as e:
            raise e

        except Exception as e:
            raise ERROR_LOCATOR(name=name, reason=e, _meta={'type': 'connector'})

    def get_info(self, name, *args, **kwargs):
        service = config.get_service()
        try:
            info_module = _get_module(service, 'info')
            return getattr(info_module, name)(*args, **kwargs)

        except ERROR_BASE as e:
            raise e

        except Exception as e:
            raise ERROR_LOCATOR(name=name, reason=e, _meta={'type': 'info'})

    def get_model(self, name):
        service = config.get_service()
        try:
            model_module = _get_module(service, 'model')
            model = getattr(model_module, name)
            model.connect()
            return model

        except ERROR_BASE as e:
            raise e

        except Exception as e:
            raise ERROR_LOCATOR(name=f'{name} Model', reason=e, _meta={'type': 'info'})

    def get_actuator(self, name, *args, **kwargs):
        service = config.get_service()
        actuator = self._load_actuator('core', name, *args, **kwargs)
        try:
            service_actuator = self._load_actuator(service, name, *args, **kwargs)
            if issubclass(service_actuator.__class__, actuator.__class__):
                actuator = service_actuator
        except Exception as e:
            message = getattr(e, 'message', e)
            _LOGGER.warning(f'Actuator Load Error: {message}')

        return actuator

    @staticmethod
    def _load_actuator(service, name, *args, **kwargs):
        actuator_instance = None
        try:
            actuator_module = _get_module(service, 'actuator')
            actuator_instance = getattr(actuator_module, name)(*args, **kwargs)

        except ERROR_BASE as e:
            raise e

        except Exception as e:
            raise ERROR_LOCATOR(name=f'{name} Actuator', reason=e, meta={'type': 'actuator'})

        return actuator_instance
