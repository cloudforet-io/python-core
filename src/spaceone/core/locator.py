import logging

from spaceone.core import config
from spaceone.core.error import *

_LOGGER = logging.getLogger(__name__)


def _get_module(package, target):
    return __import__(f"{package}.{target}", fromlist=[f"{target}"])


class Locator(object):
    @staticmethod
    def get_service(name_or_object: [str, object], metadata: dict = None, **kwargs):
        metadata = metadata or {}
        package = config.get_package()

        if isinstance(name_or_object, str):
            service_module = _get_module(package, "service")
            return getattr(service_module, name_or_object)(metadata=metadata, **kwargs)
        else:
            return name_or_object(metadata=metadata, **kwargs)

    @staticmethod
    def get_manager(name_or_object: [str, object], **kwargs):
        package = config.get_package()
        try:
            if isinstance(name_or_object, str):
                manager_module = _get_module(package, "manager")
                return getattr(manager_module, name_or_object)(**kwargs)
            else:
                return name_or_object(**kwargs)

        except ERROR_BASE as e:
            raise e

        except Exception as e:
            raise ERROR_LOCATOR(
                name=name_or_object, reason=e, _meta={"type": "manager"}
            )

    @staticmethod
    def get_info(name_or_object: [str, object], *args, **kwargs):
        package = config.get_package()
        try:
            if isinstance(name_or_object, str):
                info_module = _get_module(package, "info")
                return getattr(info_module, name_or_object)(*args, **kwargs)
            else:
                return name_or_object(*args, **kwargs)

        except ERROR_BASE as e:
            raise e

        except Exception as e:
            raise ERROR_LOCATOR(name=name_or_object, reason=e, _meta={"type": "info"})

    @staticmethod
    def get_model(name_or_object: [str, object]):
        package = config.get_package()
        try:
            if isinstance(name_or_object, str):
                model_module = _get_module(package, "model")
                return getattr(model_module, name_or_object)
            else:
                return name_or_object

        except ERROR_BASE as e:
            raise e

        except Exception as e:
            raise ERROR_LOCATOR(
                name=f"{name_or_object} Model", reason=e, _meta={"type": "model"}
            )

    @staticmethod
    def get_connector(name_or_object: [str, object], **kwargs):
        package = config.get_package()

        try:
            if isinstance(name_or_object, str):
                connector_conf = config.get_connector(name_or_object)
                backend = connector_conf.get("backend")

                if backend:
                    connector_module_path, connector_name = backend.rsplit(":", 1)
                    connector_module = __import__(
                        connector_module_path, fromlist=[connector_name]
                    )
                else:
                    connector_module = _get_module(package, "connector")
                    connector_name = name_or_object

                return getattr(connector_module, connector_name)(
                    connector_name=name_or_object, **kwargs
                )
            else:
                return name_or_object(**kwargs)

        except ERROR_BASE as e:
            raise e

        except Exception as e:
            raise ERROR_LOCATOR(
                name=name_or_object, reason=e, _meta={"type": "connector"}
            )
