import logging

from spaceone.core.cache import cacheable
from spaceone.core.handler import BaseAuthenticationHandler
from spaceone.core.connector.space_connector import SpaceConnector
from spaceone.core.error import *

_LOGGER = logging.getLogger(__name__)

_EXCLUDE_METHODS = [
    'identity.APIKey.get',
    'identity.Authorization.verify'
]


class AuthenticationAPIKeyHandler(BaseAuthenticationHandler):

    def __init__(self, config):
        super().__init__(config)
        self._initialize()

    def _initialize(self):
        if 'uri' not in self.config:
            _LOGGER.error(f'[_initialize] uri config is undefined.')
            raise ERROR_HANDLER_CONFIGURATION(handler='AuthenticationGRPCHandler')

        self.client: SpaceConnector = self.locator.get_connector(SpaceConnector, endpoint=self.config['uri'])

    def verify(self, params=None):
        request_method = f'{self.transaction.service}.{self.transaction.resource}.{self.transaction.verb}'
        if request_method not in _EXCLUDE_METHODS:
            token_type = self.transaction.get_meta('token_type')
            api_key_id = self.transaction.get_meta('api_key_id')
            api_key_ver = self.transaction.get_meta('api_key_ver')
            domain_id = self.transaction.get_meta('domain_id')

            if token_type == 'API_KEY':
                if api_key_id and domain_id:
                    self._check_api_key(api_key_id, domain_id)
                else:
                    raise ERROR_AUTHENTICATE_FAILURE(message='This API Key is no longer supported.')

    @cacheable(key='api-key:{domain_id}:{api_key_id}', backend='local')
    def _check_api_key(self, api_key_id, domain_id):
        _LOGGER.debug(f'[_check_api_key] check api key state ({api_key_id})')

        try:
            response = self.client.dispatch(
                'APIKey.get',
                {
                    'api_key_id': api_key_id,
                    'domain_id': domain_id
                }
            )

            if response['state'] != 'ENABLED':
                raise ERROR_AUTHENTICATE_FAILURE(message='The state of the API key has been disabled.')

            return True

        except ERROR_AUTHENTICATE_FAILURE as e:
            raise e
        except ERROR_GRPC_CONNECTION as e:
            raise e
        except Exception as e:
            _LOGGER.error(f'[_check_api_key] API Call Error: {e}')
            raise ERROR_AUTHENTICATE_FAILURE(message='API key is invalid.')
