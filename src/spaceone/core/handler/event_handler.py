[]# -*- coding: utf-8 -*-

from google.protobuf.json_format import MessageToDict
from spaceone.core import pygrpc
from spaceone.core import utils
from spaceone.core.transaction import Transaction
from spaceone.api.core.v1 import handler_pb2


class EventGRPCHandler(object):

    def __init__(self, config):
        self._validate(config)
        self.uri_info = utils.parse_grpc_uri(config['uri'])
        self.event_type = config.get('event_type',
                                     ['START', 'IN-PROGRESS', 'SUCCESS', 'FAILURE'])

    def _validate(self, config):
        pass

    def notify(self, transaction: Transaction, event_type: str, message: dict) -> dict:
        if event_type in self.event_type:
            grpc_method = pygrpc.get_grpc_method(self.uri_info)
            grpc_method({
                'service': transaction.service,
                'api_class': transaction.api_class,
                'method': transaction.method,
                'event_type': event_type,
                'message': message
            })