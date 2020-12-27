import logging
from spaceone.core.handler import BaseMutationHandler

_LOGGER = logging.getLogger(__name__)


class SpaceONEMutationHandler(BaseMutationHandler):

    def request(self, params):
        role_type = self.transaction.get_meta('auth.role_type')
        domain_id = self.transaction.get_meta('domain_id')
        project_role_inject_params = self.transaction.get_meta('auth.project.inject', {})

        if role_type in ['DOMAIN', 'PROJECT']:
            params['domain_id'] = domain_id

        if role_type == 'PROJECT':
            if isinstance(project_role_inject_params, dict):
                for key, value in project_role_inject_params.items():
                    if key not in params:
                        params[key] = self.transaction.get_meta(value)

        return params
