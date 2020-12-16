from spaceone.core import pygrpc
from spaceone.core import utils
from spaceone.core.transaction import Transaction

_STATUS = ['STARTED', 'IN_PROGRESS', 'SUCCESS', 'FAILURE']


class EventGRPCHandler(object):

    def __init__(self, config):
        self._validate(config)
        self.uri_info = utils.parse_grpc_uri(config['uri'])

    def _validate(self, config):
        pass

    def notify(self, transaction: Transaction, status: str, message: dict):
        if status in _STATUS:
            grpc_method = pygrpc.get_grpc_method(self.uri_info)
            grpc_method({
                'service': transaction.service,
                'resource': transaction.resource,
                'verb': transaction.verb,
                'status': status,
                'message': message
            })
