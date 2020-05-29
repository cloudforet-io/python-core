# -*- coding: utf-8 -*-
import copy
import logging

from spaceone.core.error import *
from spaceone.core.auth.jwt import JWTUtil
from google.protobuf.json_format import MessageToDict
from spaceone.core import pygrpc
from spaceone.core import utils
from spaceone.core.transaction import Transaction
from spaceone.api.core.v1 import handler_pb2

_LOGGER = logging.getLogger(__name__)


class AuthorizationGRPCHandler(object):

    def __init__(self, config):
        self._validate(config)
        self.uri_info = utils.parse_grpc_uri(config['uri'])

    def _validate(self, config):
        pass

    def notify(self, transaction: Transaction, params: dict) -> dict:
        return params
        # token_type = transaction.get_meta('token_type')
        #
        # if token_type == 'DOMAIN_OWNER':
        #     print('')
        #     changed_params = self._verify_domain_owner(transaction, params)
        # else:
        #     changed_params = self._verify_auth(transaction, params)
        #
        # return changed_params

    def _verify_domain_owner(self, transaction, params):
        domain_id = transaction.get_meta('domain_id')
        if domain_id is None:
            raise ERROR_AUTHENTICATE_FAILURE(message='domain_id not set.')

        print('_update_domain_owner_params', domain_id)
        params['domain_id'] = domain_id
        return params

    def _verify_auth(self, transaction, params):
        grpc_method = pygrpc.get_grpc_method(self.uri_info)
        response = grpc_method(
            {
                'service': transaction.service,
                'api_class': transaction.api_class,
                'method': transaction.method,
                'parameter': params
            },
            metadata=transaction.get_connection_meta()
        )

        transaction.set_meta('role_type', response.role_type)
        return response.changed_parameter
