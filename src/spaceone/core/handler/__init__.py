import abc
from typing import Any
from spaceone.core.transaction import Transaction
from spaceone.core.base import CoreObject


class BaseHandler(CoreObject):

    def __init__(self, transaction: Transaction, config: dict):
        super().__init__(transaction=transaction)

        self.config = config


class BaseAuthenticationHandler(abc.ABC, BaseHandler):

    @abc.abstractmethod
    def verify(self, params: dict) -> None:
        """
        Args:
            params (dict): Request Parameter

        Returns:
            None
        """
        raise NotImplementedError('verify method not implemented!')


class BaseAuthorizationHandler(abc.ABC, BaseHandler):

    @abc.abstractmethod
    def verify(self, params: dict) -> None:
        """
        Args:
            params (dict): Request Parameter

        Returns:
            None
        """
        raise NotImplementedError('verify method not implemented!')


class BaseMutationHandler(BaseHandler):

    def request(self, params) -> Any:
        """
        Args:
            params (dict): Request Parameter

        Returns:
            params (dict): Changed Parameter
        """
        return params

    def response(self, result: Any) -> Any:
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
        raise NotImplementedError('notify method not implemented!')
