import logging
from spaceone.core import pygrpc
from spaceone.core import utils
from spaceone.core.transaction import Transaction
from spaceone.core.handler import BaseEventHandler
from spaceone.core.error import ERROR_HANDLER_CONFIGURATION

_LOGGER = logging.getLogger(__name__)


class EventGRPCHandler(BaseEventHandler):

    def __init__(self, transaction: Transaction, config):
        super().__init__(transaction, config)
        self._initialize()

    def _initialize(self):
        if 'uri' not in self.config:
            _LOGGER.error(f'[_initialize] uri config is undefined.')
            raise ERROR_HANDLER_CONFIGURATION(handler='AuthenticationGRPCHandler')

        try:
            self.uri_info = utils.parse_grpc_uri(self.config['uri'])
        except Exception as e:
            _LOGGER.error(f'[_initialize] AuthenticationGRPCHandler Init Error: {e}')
            raise ERROR_HANDLER_CONFIGURATION(handler='AuthenticationGRPCHandler')

    def notify(self, status: str, message: dict):
        grpc_method = pygrpc.get_grpc_method(self.uri_info)
        grpc_method({
            'service': self.transaction.service,
            'resource': self.transaction.resource,
            'verb': self.transaction.verb,
            'status': status,
            'message': message
        })
