import logging

from spaceone.core.error import *
from spaceone.core import pygrpc
from spaceone.core import utils
from spaceone.core.transaction import Transaction
from spaceone.core.handler import BaseMutationHandler

_LOGGER = logging.getLogger(__name__)


class SpaceONEMutationHandler(BaseMutationHandler):

    def __init__(self, transaction: Transaction, config: dict):
        super().__init__(transaction, config)
        self.uri_info = utils.parse_grpc_uri(self.config['uri'])

    def request(self, transaction: Transaction, params):
        user_type = transaction.get_meta('user_type')
        pass

    def response(self, transaction: Transaction, result):
        user_type = transaction.get_meta('user_type')
        pass
