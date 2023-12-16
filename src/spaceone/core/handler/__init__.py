import abc
import logging
from typing import List
from spaceone.core.base import CoreObject
from spaceone.core import config
from spaceone.core.error import *

__all__ = [
    "BaseHandler",
    "BaseAuthenticationHandler",
    "BaseAuthorizationHandler",
    "BaseMutationHandler",
    "BaseEventHandler",
    "get_authentication_handlers",
    "get_authorization_handlers",
    "get_mutation_handlers",
    "get_event_handlers",
]

_HANDLER_TYPE = ["authentication", "authorization", "mutation", "event"]
_HANDLER_INFO = {
    "init": False,
    "authentication": [],
    "authorization": [],
    "mutation": [],
    "event": [],
}

_LOGGER = logging.getLogger(__name__)


class BaseHandler(CoreObject):
    def __init__(self, handler_config: dict):
        super().__init__()

        self.config = handler_config


class BaseAuthenticationHandler(abc.ABC, BaseHandler):
    @abc.abstractmethod
    def verify(self, params: dict) -> None:
        """
        Args:
            params (dict): Request Parameter

        Returns:
            None
        """
        raise NotImplementedError("verify method not implemented!")


class BaseAuthorizationHandler(abc.ABC, BaseHandler):
    @abc.abstractmethod
    def verify(
        self, params: dict, permission: str = None, role_types: list = None
    ) -> None:
        """
        Args:
            params (dict): Request Parameter
            permission (str): API Permission
            role_types (list): Allowed Role Types

        Returns:
            None
        """
        raise NotImplementedError("verify method not implemented!")


class BaseMutationHandler(BaseHandler):
    def request(self, params: dict) -> dict:
        """
        Args:
            params (dict): Request Parameter

        Returns:
            params (dict): Changed Parameter
        """
        return params

    def response(self, result: any) -> any:
        """
        Args:
            result (dict): Result data

        Returns:
            result (dict): Changed Result Data
        """
        return result


class BaseEventHandler(abc.ABC, BaseHandler):
    @abc.abstractmethod
    def notify(self, status: str, message: dict) -> None:
        """
        Args:
            status (str): 'Transaction Status: STARTED or IN_PROGRESS or SUCCESS or FAILURE'
            message (dict): 'Request Parameter or Response Data or Error Message'

        Returns:
            None
        """
        raise NotImplementedError("notify method not implemented!")


def _init_handlers() -> None:
    registered_handlers = config.get_global("HANDLERS", {})
    for handler_type in _HANDLER_TYPE:
        for handler_conf in registered_handlers.get(handler_type, []):
            try:
                module_name, class_name = handler_conf["backend"].rsplit(":", 1)
                _LOGGER.debug(f"[_init_handlers] {handler_type} handler: {class_name}")

                handler_module = __import__(module_name, fromlist=[class_name])

                _HANDLER_INFO[handler_type].append(
                    handler_module.__dict__[class_name](handler_conf)
                )

            except Exception as e:
                raise ERROR_HANDLER_CONFIGURATION(handler=handler_type, reason=str(e))


def get_authentication_handlers() -> List[BaseAuthenticationHandler]:
    _check_init_state()
    return _HANDLER_INFO.get("authentication", [])


def get_authorization_handlers() -> List[BaseAuthorizationHandler]:
    _check_init_state()
    return _HANDLER_INFO.get("authorization", [])


def get_mutation_handlers(reverse: bool = False) -> List[BaseMutationHandler]:
    _check_init_state()
    if reverse:
        return _HANDLER_INFO.get("mutation", [])[::-1]
    else:
        return _HANDLER_INFO.get("mutation", [])


def get_event_handlers() -> List[BaseEventHandler]:
    _check_init_state()
    return _HANDLER_INFO.get("event", [])


def _check_init_state() -> None:
    if not _HANDLER_INFO["init"]:
        _init_handlers()
        _HANDLER_INFO["init"] = True
