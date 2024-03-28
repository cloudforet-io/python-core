import logging
from spaceone.core.handler import BaseMutationHandler

_LOGGER = logging.getLogger(__name__)


class SpaceONEMutationHandler(BaseMutationHandler):
    def request(self, params):
        user_role_type: str = self.transaction.get_meta("authorization.role_type")
        domain_id: str = self.transaction.get_meta("authorization.domain_id")
        workspace_id: str = self.transaction.get_meta("authorization.workspace_id")
        user_projects: list = self.transaction.get_meta("authorization.projects")
        user_id: str = self.transaction.get_meta("authorization.user_id")
        set_user_id: str = self.transaction.get_meta("authorization.set_user_id")
        injected_params: dict = self.transaction.get_meta("authorization.injected_params")

        if user_role_type == "SYSTEM_TOKEN":
            if domain_id:
                params["domain_id"] = domain_id

            if workspace_id:
                params["workspace_id"] = workspace_id

        elif user_role_type == "DOMAIN_ADMIN":
            params["domain_id"] = domain_id
        elif user_role_type == "WORKSPACE_OWNER":
            params["domain_id"] = domain_id
            params["workspace_id"] = workspace_id
        elif user_role_type == "WORKSPACE_MEMBER":
            params["domain_id"] = domain_id
            params["workspace_id"] = workspace_id
            params["user_projects"] = user_projects
        elif user_role_type == "USER":
            params["domain_id"] = domain_id

        if set_user_id:
            params["user_id"] = user_id

        if injected_params:
            params.update(injected_params)

        return params
