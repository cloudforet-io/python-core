# -*- coding: utf-8 -*-

import json
from jwcrypto import jwk
from jose import jwt


class JWTUtil:

    @staticmethod
    def generate_jwk(key_type='RSA', size=2048):
        key = jwk.JWK.generate(kty=key_type, size=size)
        private_jwk = json.loads(key.export_private())
        public_jwk = json.loads(key.export_public())
        return private_jwk, public_jwk

    @staticmethod
    def encode(payload: dict, private_jwk: dict, algorithm='RS256') -> str:
        return jwt.encode(payload, key=private_jwk, algorithm=algorithm)

    @staticmethod
    def decode(token: str, public_jwk: dict, algorithm=['RS256']) -> dict:
        return jwt.decode(token, key=public_jwk, algorithms=algorithm, options={
            'verify_aud': False
        })

    @staticmethod
    def unverified_decode(token: str) -> dict:
        return jwt.get_unverified_claims(token)
