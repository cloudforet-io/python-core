import logging
from typing import Union

from spaceone.core import config
from spaceone.core.error import *

_LOGGER = logging.getLogger(__name__)


def _get_module(package, target):
    return __import__(f'{package}.{target}', fromlist=[f'{target}'])


class Locator(object):

    def __init__(self, transaction=None):
        self.transaction = transaction

    def get_service(self, name: [str, object], metadata: dict = {}, **kwargs):
        package = config.get_package()
        try:
            if isinstance(name, str):
                service_module = _get_module(package, 'service')
                return getattr(service_module, name)(metadata=metadata, **kwargs)
            else:
                return name(metadata=metadata, **kwargs)

        except ERROR_BASE as e:
            e.set_meta['type'] = 'service'
            raise e

        except Exception as e:
            raise ERROR_LOCATOR(name=name, reason=e, _meta={'type': 'service'})

    def get_manager(self, name: [str, object], **kwargs):
        package = config.get_package()
        try:
            if isinstance(name, str):
                manager_module = _get_module(package, 'manager')
                return getattr(manager_module, name)(transaction=self.transaction, **kwargs)
            else:
                return name(transaction=self.transaction, **kwargs)

        except ERROR_BASE as e:
            raise e

        except Exception as e:
            raise ERROR_LOCATOR(name=name, reason=e, _meta={'type': 'manager'})

    def get_info(self, name: [str, object], *args, **kwargs):
        package = config.get_package()
        try:
            if isinstance(name, str):
                info_module = _get_module(package, 'info')
                return getattr(info_module, name)(*args, **kwargs)
            else:
                return name(*args, **kwargs)

        except ERROR_BASE as e:
            raise e

        except Exception as e:
            raise ERROR_LOCATOR(name=name, reason=e, _meta={'type': 'info'})

    def get_model(self, name: [str, object]):
        package = config.get_package()
        try:
            if isinstance(name, str):
                model_module = _get_module(package, 'model')
                model = getattr(model_module, name)
                model.init()
                return model
            else:
                name.init()
                return name

        except ERROR_BASE as e:
            raise e

        except Exception as e:
            raise ERROR_LOCATOR(name=f'{name} Model', reason=e, _meta={'type': 'model'})

    def get_connector(self, name: [str, object], **kwargs):
        package = config.get_package()

        try:
            if isinstance(name, str):
                connector_conf = config.get_connector(name)
                backend = connector_conf.get('backend')

                if backend:
                    connector_module, name = backend.rsplit('.', 1)
                    connector_module = __import__(connector_module, fromlist=[name])
                else:
                    connector_module = _get_module(package, 'connector')

                return getattr(connector_module, name)(transaction=self.transaction, config=connector_conf, **kwargs)
            else:
                return name(transitions=self.transaction, **kwargs)

        except ERROR_BASE as e:
            raise e

        except Exception as e:
            raise ERROR_LOCATOR(name=name, reason=e, _meta={'type': 'connector'})
