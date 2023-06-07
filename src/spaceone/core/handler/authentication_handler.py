import json
import logging

from spaceone.core.cache import cacheable
from spaceone.core.connector.space_connector import SpaceConnector
from spaceone.core.auth.jwt import JWTAuthenticator, JWTUtil
from spaceone.core.transaction import get_transaction
from spaceone.core.handler import BaseAuthenticationHandler
from spaceone.core.error import ERROR_AUTHENTICATE_FAILURE, ERROR_HANDLER_CONFIGURATION

_LOGGER = logging.getLogger(__name__)


class AuthenticationGRPCHandler(BaseAuthenticationHandler):

    def __init__(self, config):
        super().__init__(config)
        self.client = None
        self._initialize()

    def _initialize(self):
        if 'uri' not in self.config:
            _LOGGER.error(f'[_initialize] uri config is undefined.')
            raise ERROR_HANDLER_CONFIGURATION(handler='AuthenticationGRPCHandler')

        self.client: SpaceConnector = self.locator.get_connector(SpaceConnector, endpoint=self.config['uri'])

    def verify(self, params=None):
        token = self._get_token()
        domain_id = self._extract_domain_id(token)

        token_info = self._authenticate(token, domain_id)
        self._update_meta(token_info)

    @cacheable(key='public-key:{domain_id}', backend='local')
    def _get_public_key(self, domain_id):
        _LOGGER.debug(f'[_get_public_key] get jwk from identity service ({domain_id})')
        response = self.client.dispatch('Domain.get_public_key', {'domain_id': domain_id})

        return response['public_key']

    def _authenticate(self, token, domain_id):
        public_key = self._get_public_key(domain_id)

        payload = JWTAuthenticator(json.loads(public_key)).validate(token)
        # TODO: if payload is api_key type and record is delete in database, raise ERROR_AUTHENTICATE_FAILURE exception.

        return payload

    @staticmethod
    def _get_token():
        transaction = get_transaction()
        token = transaction.meta.get('token')
        if not isinstance(token, str) \
                or token is None \
                or len(token) == 0:
            raise ERROR_AUTHENTICATE_FAILURE(message='Empty token provided.')
        return token

    @staticmethod
    def _extract_domain_id(token):
        try:
            decoded = JWTUtil.unverified_decode(token)
        except Exception:
            _LOGGER.debug(f'[_extract_domain_id] failed to decode token: {token[:10]}')
            raise ERROR_AUTHENTICATE_FAILURE(message='Cannot decode token.')

        domain_id = decoded.get('did')

        if domain_id is None:
            raise ERROR_AUTHENTICATE_FAILURE(message='Empty domain_id provided.')

        return domain_id

    def _update_meta(self, token_info):
        domain_id = token_info.get('did')
        user_id = token_info.get('aud')
        user_type = token_info.get('user_type')
        token_type = token_info.get('cat')
        api_key_id = token_info.get('api_key_id')
        permissions = token_info.get('permissions')

        self.transaction.set_meta('domain_id', domain_id)
        self.transaction.set_meta('user_id', user_id)
        self.transaction.set_meta('authorization.user_type', user_type)
        self.transaction.set_meta('authorization.permissions', permissions)
        self.transaction.set_meta('token_type', token_type)
        self.transaction.set_meta('api_key_id', api_key_id)
