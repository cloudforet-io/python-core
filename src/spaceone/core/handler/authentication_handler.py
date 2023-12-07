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

    def __init__(self, handler_config):
        super().__init__(handler_config)
        self.identity_client = None
        self._initialize()

    def _initialize(self) -> None:
        self.identity_conn: SpaceConnector = SpaceConnector(service='identity')

    def verify(self, scope: str, params: dict) -> None:
        if scope not in ['system', 'public']:
            token = self._get_token()
            domain_id = self._extract_domain_id(token)

            token_info = self._authenticate(token, domain_id)
            self._update_meta(token_info)

    @cacheable(key='handler:authentication:{domain_id}public-key', alias='local')
    def _get_public_key(self, domain_id: str) -> str:
        _LOGGER.debug(f'[_get_public_key] get jwk from identity service: {domain_id}')
        response = self.identity_conn.dispatch(
            'Domain.get_public_key',
            {'domain_id': domain_id}
        )

        return response['public_key']

    def _authenticate(self, token: str, domain_id: str) -> dict:
        public_key = self._get_public_key(domain_id)
        return JWTAuthenticator(json.loads(public_key)).validate(token)

    @staticmethod
    def _get_token() -> str:
        transaction = get_transaction()
        token = transaction.meta.get('token')
        if not isinstance(token, str) or len(token) == 0:
            raise ERROR_AUTHENTICATE_FAILURE(message='empty token provided.')

        return token

    @staticmethod
    def _extract_domain_id(token):
        try:
            decoded = JWTUtil.unverified_decode(token)
        except Exception:
            _LOGGER.debug(f'[_extract_domain_id] failed to decode token: {token[:10]}')
            raise ERROR_AUTHENTICATE_FAILURE(message='failed to decode token.')

        if domain_id := decoded.get('did'):
            return domain_id
        else:
            raise ERROR_AUTHENTICATE_FAILURE(message='empty domain_id provided.')

    def _update_meta(self, token_info: dict) -> None:
        """
        Args:
            token_info(dict): {
                'iss': 'str',   # issuer (spaceone.identity)
                'typ': 'str',   # token type (ACCESS_TOKEN | REFRESH_TOKEN | API_KEY)
                'own': 'str',   # owner (USER | APP)
                'did': 'str',   # domain_id
                'sub': 'str',   # subject (api_key_id), Optional
                'aud': 'str',   # audience (user_id | app_id)
                'exp': 'int',   # expiration time
                'iat': 'int',   # issued at
                'ttl': 'int',   # max refresh time, Optional
                'jti': 'str',   # jwt id (token_key), Optional
                'ver': 'str',   # jwt version
        """

        domain_id = token_info.get('did')
        owner_type = token_info.get('own')
        api_key_id = token_info.get('sub')

        self.transaction.set_meta('domain_id', domain_id)
        self.transaction.set_meta('owner_type', owner_type)
        self.transaction.set_meta('api_key_id', api_key_id)

        if owner_type == 'USER':
            user_id = token_info.get('aud')
            self.transaction.set_meta('user_id', user_id)
        else:
            app_id = token_info.get('aud')
            self.transaction.set_meta('app_id', app_id)
