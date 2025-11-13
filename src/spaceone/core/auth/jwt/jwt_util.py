import json

from jwcrypto import jwk
from jwcrypto import jwt as jwcrypto_jwt
from jwcrypto.jws import JWS


class JWTUtil:
    @staticmethod
    def generate_jwk(key_type="RSA", size=2048):
        key = jwk.JWK.generate(kty=key_type, size=size)
        private_jwk = json.loads(key.export_private())
        public_jwk = json.loads(key.export_public())
        return private_jwk, public_jwk

    @staticmethod
    def encode(payload: dict, private_jwk: dict, algorithm="RS256") -> str:
        # Convert dict to JWK object
        key = jwk.JWK(**private_jwk)

        # Create JWT object with claims and header
        jwt_obj = jwcrypto_jwt.JWT(claims=payload, header={"alg": algorithm})

        # Sign the token
        jwt_obj.make_signed_token(key)

        # Serialize to compact format
        return jwt_obj.serialize()

    @staticmethod
    def decode(token: str, public_jwk: dict, algorithm="RS256", options=None) -> dict:
        if options is None:
            options = {}

        # Convert dict to JWK object
        key = jwk.JWK(**public_jwk)

        # Create JWT object and deserialize
        jwt_obj = jwcrypto_jwt.JWT(jwt=token, key=key, algs=[algorithm])

        # Validate the token
        verify_aud = options.get("verify_aud", False)
        check_claims = None
        if verify_aud and "aud" in options:
            check_claims = {"aud": options["aud"]}

        if check_claims:
            jwt_obj._check_claims = check_claims

        jwt_obj.validate(key)

        # Parse claims from JSON string
        return json.loads(jwt_obj.claims)

    @staticmethod
    def unverified_decode(token: str) -> dict:
        # Deserialize JWS without verification
        jws = JWS()
        jws.deserialize(token, None)

        # Parse payload from JSON string
        payload = jws.payload
        if isinstance(payload, bytes):
            payload = payload.decode("utf-8")

        return json.loads(payload)

    @staticmethod
    def get_value_from_token(token: str, key: str, default: any = None) -> any:
        try:
            return JWTUtil.unverified_decode(token).get(key, default)
        except Exception:
            return default
