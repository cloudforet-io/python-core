import logging
from spaceone.core.error import *
from spaceone.core import pygrpc
from spaceone.core import utils
from spaceone.core.transaction import Transaction
from spaceone.core.handler import BaseMutationHandler

_LOGGER = logging.getLogger(__name__)


class ProjectRoleHandler(BaseMutationHandler):

    def __init__(self, transaction: Transaction, config):
        super().__init__(transaction, config)
        self._initialize()

    def _initialize(self):
        if 'uri' not in self.config:
            raise ERROR_HANDLER_CONFIGURATION(handler='AuthenticationGRPCHandler')

        try:
            self.uri_info = utils.parse_grpc_uri(self.config['uri'])
        except Exception as e:
            _LOGGER.error(f'[_initialize] AuthenticationGRPCHandler Init Error: {e}')
            raise ERROR_HANDLER_CONFIGURATION(handler='AuthenticationGRPCHandler')

    def request(self, params):
        user_type = self.transaction.get_meta('user_type')
        return params

