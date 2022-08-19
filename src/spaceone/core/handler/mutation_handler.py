import logging
from spaceone.core.handler import BaseMutationHandler

_LOGGER = logging.getLogger(__name__)


class SpaceONEMutationHandler(BaseMutationHandler):

    def request(self, params):
        scope = self.transaction.get_meta('authorization.scope')
        role_type = self.transaction.get_meta('authorization.role_type')
        domain_id = self.transaction.get_meta('domain_id')

        if role_type != 'SYSTEM':
            if scope == 'DOMAIN':
                params = self._apply_domain_scope(params, role_type)
            elif scope == 'PROJECT':
                params = self._apply_project_scope(params, role_type)
            elif scope == 'USER':
                params = self._apply_user_scope(params, role_type)
            elif scope == 'PUBLIC_OR_DOMAIN':
                params = self._apply_domain_or_project_scope(params, role_type)
            elif scope == 'PUBLIC_OR_DOMAIN':
                params = self._apply_public_or_domain_scope(params, role_type)

        return params

    def _apply_domain_scope(self, params, role_type):
        params['domain_id'] = self.transaction.get_meta('domain_id')
        return params

    def _apply_project_scope(self, params, role_type):
        params['domain_id'] = self.transaction.get_meta('domain_id')

        if role_type in 'PROJECT':
            params['user_projects'] = self.transaction.get_meta('authorization.projects')
            params['user_project_groups'] = self.transaction.get_meta('authorization.project_groups')

        return params

    def _apply_user_scope(self, params, role_type):
        params['domain_id'] = self.transaction.get_meta('domain_id')

        if role_type in ['PROJECT', 'USER']:
            params['user_id'] = self.transaction.get_meta('user_id')

        return params

    def _apply_domain_or_project_scope(self, params, role_type):
        params['domain_id'] = self.transaction.get_meta('domain_id')

        if role_type in 'PROJECT':
            params['user_projects'] = self.transaction.get_meta('authorization.projects') + [None]
            params['user_project_groups'] = self.transaction.get_meta('authorization.project_groups') + [None]

        return params

    def _apply_public_or_domain_scope(self, params, role_type):
        params['user_domains'] = [self.transaction.get_meta('domain_id'), None]

        return params
