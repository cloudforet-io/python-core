import logging
from spaceone.core import pygrpc
from spaceone.core import utils
from spaceone.core.transaction import Transaction
from spaceone.core.handler import BaseAuthorizationHandler
from spaceone.core.error import ERROR_AUTHENTICATE_FAILURE, ERROR_HANDLER_CONFIGURATION

_LOGGER = logging.getLogger(__name__)


class AuthorizationGRPCHandler(BaseAuthorizationHandler):

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

    def verify(self, transaction: Transaction, params=None):
        pass
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
                'resource': transaction.resource,
                'verb': transaction.verb,
                'parameter': params
            },
            metadata=transaction.get_connection_meta()
        )

        transaction.set_meta('role_type', response.role_type)
        return response.changed_parameter
