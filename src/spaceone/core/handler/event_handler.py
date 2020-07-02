from google.protobuf.json_format import MessageToDict
from spaceone.core import pygrpc
from spaceone.core import utils
from spaceone.core.transaction import Transaction
from spaceone.api.core.v1 import handler_pb2

_STATE = ['STARTED', 'IN-PROGRESS', 'SUCCESS', 'FAILURE']


class EventGRPCHandler(object):

    def __init__(self, config):
        self._validate(config)
        self.uri_info = utils.parse_grpc_uri(config['uri'])

    def _validate(self, config):
        pass

    def notify(self, transaction: Transaction, state: str, message: dict):
        if state in _STATE:
            grpc_method = pygrpc.get_grpc_method(self.uri_info)
            grpc_method({
                'service': transaction.service,
                'resource': transaction.resource,
                'verb': transaction.verb,
                'state': state,
                'message': message
            })
