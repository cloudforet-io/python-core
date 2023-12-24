import logging

from spaceone.core.handler import BaseAuthorizationHandler
from spaceone.core.error import ERROR_PERMISSION_DENIED

_LOGGER = logging.getLogger(__name__)


class SpaceONEAuthorizationHandler(BaseAuthorizationHandler):
    def verify(
        self, params: dict, permission: str = None, role_types: list = None
    ) -> None:
        token_type = self.transaction.get_meta("authorization.token_type")
        if token_type != "SYSTEM_TOKEN":
            if user_role_type := self.transaction.get_meta("authorization.role_type"):
                if role_types:
                    self._check_role_type(user_role_type, role_types)

                    if resource_group := params.get("resource_group"):
                        self._check_resource_group(resource_group, user_role_type)

            user_permissions = (
                self.transaction.get_meta("authorization.permissions") or []
            )
            if len(user_permissions) > 0 and permission:
                self._check_permissions(user_permissions, permission)

            if user_projects := self.transaction.get_meta("authorization.projects"):
                if request_project_id := params.get("project_id"):
                    self._check_user_projects(user_projects, request_project_id)

    def _check_role_type(self, user_role_type: str, role_types: list) -> None:
        if role_types == ["USER"]:
            owner_type = self.transaction.get_meta("authorization.owner_type")
            if owner_type == "APP":
                raise ERROR_PERMISSION_DENIED()
            else:
                self.transaction.set_meta("authorization.set_user_id", True)

        elif user_role_type not in role_types:
            raise ERROR_PERMISSION_DENIED()

    @staticmethod
    def _check_resource_group(resource_group: str, user_role_type: str) -> None:
        if user_role_type == "DOMAIN_ADMIN":
            if resource_group not in ["DOMAIN", "WORKSPACE", "PROJECT"]:
                raise ERROR_PERMISSION_DENIED()
        elif user_role_type == "WORKSPACE_OWNER":
            if resource_group not in ["WORKSPACE", "PROJECT"]:
                raise ERROR_PERMISSION_DENIED()
        elif user_role_type == "WORKSPACE_MEMBER":
            if resource_group != "PROJECT":
                raise ERROR_PERMISSION_DENIED()
        elif user_role_type == "SYSTEM_ADMIN":
            if resource_group != "SYSTEM":
                raise ERROR_PERMISSION_DENIED()

    @staticmethod
    def _check_permissions(user_permissions: list, permission: str):
        for user_permission in user_permissions:
            if permission.startswith(user_permission):
                return True

        raise ERROR_PERMISSION_DENIED()

    @staticmethod
    def _check_user_projects(user_projects: list, request_project_id: str) -> None:
        if request_project_id not in user_projects:
            raise ERROR_PERMISSION_DENIED()
