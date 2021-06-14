import json
import logging
from spaceone.core import pygrpc
from spaceone.core import utils
from spaceone.core.cache import cacheable
from spaceone.core.transaction import Transaction
from spaceone.core.handler import BaseAuthenticationHandler
from spaceone.core.error import *

_LOGGER = logging.getLogger(__name__)

_EXCLUDE_METHODS = [
    'identity.APIKey.get',
    'identity.Authorization.verify'
]

class AuthenticationAPIKeyHandler(BaseAuthenticationHandler):

    def __init__(self, transaction: Transaction, config):
        super().__init__(transaction, config)
        self._initialize()

    def _initialize(self):
        if 'uri' not in self.config:
            _LOGGER.error(f'[_initialize] uri config is undefined.')
            raise ERROR_HANDLER_CONFIGURATION(handler='AuthenticationGRPCHandler')

        try:
            uri_info = utils.parse_grpc_uri(self.config['uri'])
        except Exception as e:
            _LOGGER.error(f'[_initialize] AuthenticationAPIKeyCHandler Init Error: {e}')
            raise ERROR_HANDLER_CONFIGURATION(handler='AuthenticationGRPCHandler')

        self.grpc_method = pygrpc.get_grpc_method(uri_info)

    def verify(self, params=None):
        request_method = f'{self.transaction.service}.{self.transaction.resource}.{self.transaction.verb}'
        if request_method not in _EXCLUDE_METHODS:
            token_type = self.transaction.get_meta('token_type')
            api_key_id = self.transaction.get_meta('api_key_id')
            domain_id = self.transaction.get_meta('domain_id')

            if token_type == 'API_KEY' and api_key_id and domain_id:
                self._check_api_key(api_key_id, domain_id)

    @cacheable(key='api-key:{domain_id}:{api_key_id}', backend='local')
    def _check_api_key(self, api_key_id, domain_id):
        _LOGGER.debug(f'[_check_api_key] check api key state ({api_key_id})')

        try:
            response = self.grpc_method({
                    'api_key_id': api_key_id,
                    'domain_id': domain_id
                },
                metadata=self.transaction.get_connection_meta()
            )

            # Check api key state (1: ENABLED)
            if response.state != 1:
                raise ERROR_AUTHENTICATE_FAILURE(message='The state of the API key has been disabled.')

            return True

        except ERROR_AUTHENTICATE_FAILURE as e:
            raise e
        except ERROR_GRPC_CONNECTION as e:
            raise e
        except Exception as e:
            _LOGGER.error(f'[_check_api_key] API Call Error: {e}')
            raise ERROR_AUTHENTICATE_FAILURE(message='API key is invalid.')
