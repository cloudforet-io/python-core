from spaceone.core.auth import Authenticator
from spaceone.core.auth.jwt.jwt_util import JWTUtil
from spaceone.core.error import *


class JWTAuthenticator(Authenticator):

    def __init__(self, key):
        self._key = key

    def validate(self, token, options=None):
        if not self._key:
            raise ERROR_AUTHENTICATE_FAILURE(message='Decode key is not set.')

        if not isinstance(token, (str, bytes)):
            raise ERROR_AUTHENTICATE_FAILURE(message='Invalid token format.')

        if options is None:
            options = {}

        try:
            payload = JWTUtil.decode(token, self._key, options=options)
        except Exception:
            raise ERROR_AUTHENTICATE_FAILURE(message='Token is invalid or expired.')

        return payload
