# -*- coding: utf-8 -*-
import json
import logging

from spaceone.core import pygrpc
from spaceone.core import utils
from spaceone.core.cache import cacheable
from spaceone.core.auth.jwt import JWTAuthenticator, JWTUtil
from spaceone.core.transaction import Transaction, ERROR_AUTHENTICATE_FAILURE

_LOGGER = logging.getLogger(__name__)


class AuthenticationGRPCHandler(object):

    def __init__(self, config):
        self._validate(config)
        self.uri_info = utils.parse_grpc_uri(config['uri'])

    def _validate(self, config):
        pass

    def notify(self, transaction: Transaction, params: dict) -> dict:
        token = self._get_token(transaction.meta)
        domain_id = self._extract_domain_id(token)
        meta: list = transaction.get_connection_meta()

        token_info = self._authenticate(token, domain_id, meta)

        self._update_meta(transaction, token_info)

        return params

    @cacheable(key='public-key:{domain_id}', backend='local')
    def _get_public_key(self, domain_id, meta):
        grpc_method = pygrpc.get_grpc_method(self.uri_info)

        response = grpc_method({
                'domain_id': domain_id
            },
            metadata=meta
        )
        return response.public_key

    def _authenticate(self, token, domain_id, meta):
        public_key = self._get_public_key(domain_id, meta)

        payload = JWTAuthenticator(json.loads(public_key)).validate(token)
        # TODO: if payload is api_key type and record is delete in database, raise ERROR_AUTHENTICATE_FAILURE exception.

        return payload

    @staticmethod
    def _get_token(meta):
        token = meta.get('token')
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

    @staticmethod
    def _update_meta(transaction, token_info):
        domain_id = token_info.get('did')
        user_id = token_info.get('aud')
        token_type = token_info.get('cat')

        if domain_id:
            transaction.set_meta('domain_id', domain_id)

        if user_id:
            transaction.set_meta('user_id', user_id)

        if token_type:
            transaction.set_meta('token_type', token_type)
