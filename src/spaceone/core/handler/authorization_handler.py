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
            _LOGGER.error(f'[_initialize] uri config is undefined.')
            raise ERROR_HANDLER_CONFIGURATION(handler='AuthenticationGRPCHandler')

        try:
            self.uri_info = utils.parse_grpc_uri(self.config['uri'])
        except Exception as e:
            _LOGGER.error(f'[_initialize] AuthenticationGRPCHandler Init Error: {e}')
            raise ERROR_HANDLER_CONFIGURATION(handler='AuthenticationGRPCHandler')

    def verify(self, params=None):
        user_type = self.transaction.get_meta('authorization.user_type')
        scope = self.transaction.get_meta('authorization.scope', 'DOMAIN')

        if user_type == 'DOMAIN_OWNER':
            self._verify_domain_owner(params)
        else:
            self._verify_auth(params, scope)

    def _verify_domain_owner(self, params):
        # Pass all methods
        self.transaction.set_meta('authorization.role_type', 'DOMAIN')

    def _verify_auth(self, params, scope):
        grpc_method = pygrpc.get_grpc_method(self.uri_info)
        response = grpc_method(
            {
                'service': self.transaction.service,
                'resource': self.transaction.resource,
                'verb': self.transaction.verb,
                'scope': scope,
                'domain_id': self.transaction.get_meta('domain_id'),
                'project_id': params.get('project_id'),
                'project_group_id': params.get('project_group_id')
            },
            metadata=self.transaction.get_connection_meta()
        )

        self.transaction.set_meta('authorization.role_type', response.role_type)
        self.transaction.set_meta('authorization.projects', list(response.projects))
        self.transaction.set_meta('authorization.project_groups', list(response.project_groups))
