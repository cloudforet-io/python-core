import json
import logging
from spaceone.core import pygrpc
from spaceone.core import utils
from spaceone.core.cache import cacheable
from spaceone.core.auth.jwt import JWTAuthenticator, JWTUtil
from spaceone.core.transaction import Transaction
from spaceone.core.handler import BaseAuthenticationHandler
from spaceone.core.error import ERROR_AUTHENTICATE_FAILURE, ERROR_HANDLER_CONFIGURATION

_LOGGER = logging.getLogger(__name__)


class AuthenticationGRPCHandler(BaseAuthenticationHandler):

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
            _LOGGER.error(f'[_initialize] AuthenticationGRPCHandler Init Error: {e}')
            raise ERROR_HANDLER_CONFIGURATION(handler='AuthenticationGRPCHandler')

        self.grpc_method = pygrpc.get_grpc_method(uri_info)

    def verify(self, params=None):
        token = self._get_token()
        domain_id = self._extract_domain_id(token)

        token_info = self._authenticate(token, domain_id)
        self._update_meta(token_info)

    @cacheable(key='public-key:{domain_id}', backend='local')
    def _get_public_key(self, domain_id):
        _LOGGER.debug(f'[_get_public_key] get jwk from identity service ({domain_id})')

        response = self.grpc_method({
                'domain_id': domain_id
            },
            metadata=self.transaction.get_connection_meta()
        )
        return response.public_key

    def _authenticate(self, token, domain_id):
        public_key = self._get_public_key(domain_id)

        payload = JWTAuthenticator(json.loads(public_key)).validate(token)
        # TODO: if payload is api_key type and record is delete in database, raise ERROR_AUTHENTICATE_FAILURE exception.

        return payload

    def _get_token(self):
        token = self.transaction.meta.get('token')
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
            _LOGGER.debug(f'[ERROR_AUTHENTICATE_FAILURE] token: {token}')
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

