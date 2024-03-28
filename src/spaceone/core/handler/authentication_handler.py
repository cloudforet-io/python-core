import json
import logging

from spaceone.core import cache, config
from spaceone.core.connector.space_connector import SpaceConnector
from spaceone.core.auth.jwt import JWTAuthenticator, JWTUtil
from spaceone.core.transaction import get_transaction
from spaceone.core.handler import BaseAuthenticationHandler
from spaceone.core.error import ERROR_AUTHENTICATE_FAILURE

_LOGGER = logging.getLogger(__name__)


class SpaceONEAuthenticationHandler(BaseAuthenticationHandler):
    def __init__(self, handler_config):
        super().__init__(handler_config)
        self.identity_client = None
        self._initialize()

    def _initialize(self) -> None:
        self.identity_conn: SpaceConnector = SpaceConnector(service="identity")

    def verify(self, params: dict) -> None:
        token = self._get_token()
        domain_id = self._extract_domain_id(token)

        token_info = self._authenticate(token, domain_id)

        if token_info.get("typ") == "SYSTEM_TOKEN":
            self._update_system_meta(token_info)
        else:
            version = token_info.get("ver")
            if version not in ["2.0"]:
                raise ERROR_AUTHENTICATE_FAILURE(message="invalid token version.")

            owner_type = token_info.get("own")
            if owner_type == "APP":
                client_id = token_info.get("jti")
                domain_id = token_info.get("did")
                token_info["permissions"] = self._check_app(client_id, domain_id)

            self._update_meta(token_info)

    @cache.cacheable(key="handler:authentication:{domain_id}:public-key", alias="local")
    def _get_public_key(self, domain_id: str) -> str:
        system_token = config.get_global("TOKEN")

        _LOGGER.debug(f"[_get_public_key] get jwk from identity service: {domain_id}")
        response = self.identity_conn.dispatch(
            "Domain.get_public_key", {"domain_id": domain_id}, token=system_token
        )

        return response["public_key"]

    @cache.cacheable(
        key="handler:authentication:{domain_id}:client:{client_id}", alias="local"
    )
    def _check_app(self, client_id, domain_id) -> list:
        system_token = config.get_global("TOKEN")

        _LOGGER.debug(f"[_check_app] check app from identity service: {client_id}")
        response = self.identity_conn.dispatch(
            "App.check",
            {
                "client_id": client_id,
                "domain_id": domain_id,
            },
            token=system_token,
        )

        return response.get("permissions", [])

    def _authenticate(self, token: str, domain_id: str) -> dict:
        public_key = self._get_public_key(domain_id)
        return JWTAuthenticator(json.loads(public_key)).validate(token)

    @staticmethod
    def _get_token() -> str:
        transaction = get_transaction()
        token = transaction.meta.get("token")
        if not isinstance(token, str) or len(token) == 0:
            raise ERROR_AUTHENTICATE_FAILURE(message="empty token provided.")

        return token

    @staticmethod
    def _extract_domain_id(token):
        try:
            decoded = JWTUtil.unverified_decode(token)
        except Exception:
            _LOGGER.debug(f"[_extract_domain_id] failed to decode token: {token[:10]}")
            raise ERROR_AUTHENTICATE_FAILURE(message="failed to decode token.")

        if domain_id := decoded.get("did"):
            return domain_id
        else:
            raise ERROR_AUTHENTICATE_FAILURE(message="empty domain_id provided.")

    def _update_meta(self, token_info: dict) -> None:
        """
        Args:
            token_info(dict): {
                'iss': 'str',   # issuer (spaceone.identity)
                'rol': 'str',   # role type
                'typ': 'str',   # token type (ACCESS_TOKEN | REFRESH_TOKEN | CLIENT_SECRET)
                'own': 'str',   # owner (USER | APP)
                'did': 'str',   # domain_id
                'wid': 'str',   # workspace_id, Optional
                'aud': 'str',   # audience (user_id | app_id)
                'exp': 'int',   # expiration time
                'iat': 'int',   # issued at
                'jti': 'str',   # jwt id (token_key | client_id), Optional
                'permissions': 'list',  # permissions, Optional
                'projects': 'list',     # project_ids, if workspace member, Optional
                'injected_params': 'dict',  # injected parameters, override parameters, Optional
                'ver': 'str',   # jwt version
        """

        token_type = token_info.get("typ")
        role_type = token_info.get("rol")
        owner_type = token_info.get("own")
        audience = token_info.get("aud")
        domain_id = token_info.get("did")
        workspace_id = token_info.get("wid")
        permissions = token_info.get("permissions")
        projects = token_info.get("projects")
        injected_params = token_info.get("injected_params")

        self.transaction.set_meta("authorization.token_type", token_type)
        self.transaction.set_meta("authorization.role_type", role_type)
        self.transaction.set_meta("authorization.owner_type", owner_type)
        self.transaction.set_meta("authorization.domain_id", domain_id)
        self.transaction.set_meta("authorization.audience", audience)
        self.transaction.set_meta("authorization.workspace_id", workspace_id)
        self.transaction.set_meta("authorization.permissions", permissions)
        self.transaction.set_meta("authorization.projects", projects)
        self.transaction.set_meta("authorization.injected_params", injected_params)

        if owner_type == "USER":
            self.transaction.set_meta("authorization.user_id", audience)
        elif owner_type == "APP":
            self.transaction.set_meta("authorization.app_id", audience)

    def _update_system_meta(self, token_info: dict) -> None:
        """
        Args:
            token_info(dict): {
                'iss': 'str',   # issuer (spaceone.identity)
                'typ': 'str',   # token type (SYSTEM_TOKEN)
                'own': 'str',   # owner (SYSTEM)
                'did': 'str',   # domain_id (domain-root)
                'aud': 'str',   # audience (root_domain_user_id)
                'iat': 'int',   # issued at
                'jti': 'str',   # jwt id
                'ver': 'str',   # jwt version
        """

        token_type = token_info.get("typ")
        owner_type = token_info.get("own")
        audience = token_info.get("aud")
        domain_id = self.transaction.get_meta("x_domain_id")
        workspace_id = self.transaction.get_meta("x_workspace_id")

        self.transaction.set_meta("authorization.token_type", token_type)
        self.transaction.set_meta("authorization.role_type", "SYSTEM_TOKEN")
        self.transaction.set_meta("authorization.owner_type", owner_type)
        self.transaction.set_meta("authorization.domain_id", domain_id)
        self.transaction.set_meta("authorization.audience", audience)
        self.transaction.set_meta("authorization.workspace_id", workspace_id)
