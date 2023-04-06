import logging

from spaceone.core import config
from spaceone.core.error import *

_LOGGER = logging.getLogger(__name__)


def _get_module(package, target):
    return __import__(f'{package}.{target}', fromlist=[f'{target}'])


class Locator(object):

    def __init__(self, transaction=None):
        self.transaction = transaction

    def get_service(self, name_or_object: [str, object], metadata: dict = None, **kwargs):
        metadata = metadata or {}
        package = config.get_package()
        try:
            if isinstance(name_or_object, str):
                service_module = _get_module(package, 'service')
                return getattr(service_module, name_or_object)(metadata=metadata, **kwargs)
            else:
                return name_or_object(metadata=metadata, **kwargs)

        except ERROR_BASE as e:
            e.meta['type'] = 'service'
            raise e

        except Exception as e:
            raise ERROR_LOCATOR(name=name_or_object, reason=e, _meta={'type': 'service'})

    def get_manager(self, name_or_object: [str, object], **kwargs):
        package = config.get_package()
        try:
            if isinstance(name_or_object, str):
                manager_module = _get_module(package, 'manager')
                return getattr(manager_module, name_or_object)(transaction=self.transaction, **kwargs)
            else:
                return name_or_object(transaction=self.transaction, **kwargs)

        except ERROR_BASE as e:
            raise e

        except Exception as e:
            raise ERROR_LOCATOR(name=name_or_object, reason=e, _meta={'type': 'manager'})

    def get_info(self, name_or_object: [str, object], *args, **kwargs):
        package = config.get_package()
        try:
            if isinstance(name_or_object, str):
                info_module = _get_module(package, 'info')
                return getattr(info_module, name_or_object)(*args, **kwargs)
            else:
                return name_or_object(*args, **kwargs)

        except ERROR_BASE as e:
            raise e

        except Exception as e:
            raise ERROR_LOCATOR(name=name_or_object, reason=e, _meta={'type': 'info'})

    def get_model(self, name_or_object: [str, object]):
        package = config.get_package()
        try:
            if isinstance(name_or_object, str):
                model_module = _get_module(package, 'model')
                model = getattr(model_module, name_or_object)
                model.init()
                return model
            else:
                name_or_object.init()
                return name_or_object

        except ERROR_BASE as e:
            raise e

        except Exception as e:
            raise ERROR_LOCATOR(name=f'{name_or_object} Model', reason=e, _meta={'type': 'model'})

    def get_connector(self, name_or_object: [str, object], **kwargs):
        package = config.get_package()

        try:
            if isinstance(name_or_object, str):
                connector_conf = config.get_connector(name_or_object)
                backend = connector_conf.get('backend')

                if backend:
                    connector_module, name = backend.rsplit('.', 1)
                    connector_module = __import__(connector_module, fromlist=[name])
                else:
                    connector_module = _get_module(package, 'connector')

                return getattr(connector_module, name_or_object)(transaction=self.transaction, config=connector_conf,
                                                              **kwargs)
            else:
                connector_conf = config.get_connector(name_or_object.__name__)
                return name_or_object(transaction=self.transaction, config=connector_conf, **kwargs)

        except ERROR_BASE as e:
            raise e

        except Exception as e:
            raise ERROR_LOCATOR(name=name_or_object, reason=e, _meta={'type': 'connector'})
