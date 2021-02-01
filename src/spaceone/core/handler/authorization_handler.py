import logging
from spaceone.core import pygrpc
from spaceone.core import utils
from spaceone.core.transaction import Transaction
from spaceone.core.handler import BaseAuthorizationHandler
from spaceone.core.error import ERROR_HANDLER_CONFIGURATION, ERROR_PERMISSION_DENIED

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
        project_id_key = self.transaction.get_meta('authorization.project_id', 'project_id')
        project_group_id_key = self.transaction.get_meta('authorization.project_group_id', 'project_group_id')
        user_id_key = self.transaction.get_meta('authorization.user_id', 'user_id')
        require_project_group_id = self.transaction.get_meta('authorization.require_project_group_id', False)
        require_project_id = self.transaction.get_meta('authorization.require_project_id', False)

        grpc_method = pygrpc.get_grpc_method(self.uri_info)

        try:
            response = grpc_method(
                {
                    'service': self.transaction.service,
                    'resource': self.transaction.resource,
                    'verb': self.transaction.verb,
                    'scope': scope,
                    'domain_id': params.get('domain_id'),
                    'project_id': params.get(project_id_key),
                    'project_group_id': params.get(project_group_id_key),
                    'user_id': params.get(user_id_key),
                    'require_project_id': require_project_id,
                    'require_project_group_id': require_project_group_id
                },
                metadata=self.transaction.get_connection_meta()
            )

            projects = list(response.projects) + [None]
            project_groups = list(response.project_groups) + [None]

            self.transaction.set_meta('authorization.role_type', response.role_type)
            self.transaction.set_meta('authorization.projects', projects)
            self.transaction.set_meta('authorization.project_groups', project_groups)
        except Exception as e:
            _LOGGER.error(f'[_verify_auth] Authorization.verify request failed: {e}')
            raise ERROR_PERMISSION_DENIED()
