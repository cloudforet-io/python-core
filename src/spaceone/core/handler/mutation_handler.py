import logging
from spaceone.core.handler import BaseMutationHandler

_LOGGER = logging.getLogger(__name__)


class SpaceONEMutationHandler(BaseMutationHandler):

    def request(self, params):
        role_type = self.transaction.get_meta('auth.role_type')
        domain_id = self.transaction.get_meta('domain_id')
        inject_filter = self.transaction.get_meta('auth.inject_filter', {})

        if role_type in ['DOMAIN', 'PROJECT']:
            params['domain_id'] = domain_id

        if role_type == 'PROJECT':
            if isinstance(inject_filter, dict):
                for key, value in inject_filter.items():
                    if key not in params:
                        params[key] = self.transaction.get_meta(value)

        return params
