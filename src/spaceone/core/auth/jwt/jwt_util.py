import base64
import json

from jwcrypto import jwk
from jwcrypto import jwt as jwcrypto_jwt


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
        parts = token.split(".")
        if len(parts) != 3:
            raise ValueError("Invalid JWT token format")

        # Decode payload part (base64url)
        payload_part = parts[1]

        # Handle base64url padding
        padding = 4 - len(payload_part) % 4
        if padding != 4:
            payload_part += "=" * padding

        # Decode base64url
        payload_bytes = base64.urlsafe_b64decode(payload_part)
        payload_str = payload_bytes.decode("utf-8")

        # Parse JSON
        return json.loads(payload_str)
