import logging

from spaceone.core.handler import BaseAuthorizationHandler
from spaceone.core.error import ERROR_PERMISSION_DENIED

_LOGGER = logging.getLogger(__name__)


class SpaceONEAuthorizationHandler(BaseAuthorizationHandler):
    def verify(
        self, params: dict, permission: str = None, role_types: list = None
    ) -> None:
        if user_role_type := self.transaction.get_meta("authorization.role_type"):
            if role_types:
                self._check_role_type(user_role_type, role_types)

        if user_permissions := self.transaction.get_meta("authorization.permissions"):
            if permission:
                self._check_permissions(user_permissions, permission)

        if user_projects := self.transaction.get_meta("authorization.projects"):
            if request_project_id := params.get("project_id"):
                self._check_user_projects(user_projects, request_project_id)

    def _check_role_type(self, user_role_type: str, role_types: list) -> None:
        if "USER" in role_types:
            self.transaction.set_meta("authorization.set_user_id", True)

        elif user_role_type not in role_types:
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
