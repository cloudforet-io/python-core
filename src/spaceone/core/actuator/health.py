import logging
from enum import Enum

from spaceone.core import config

_LOGGER = logging.getLogger(__name__)


class Health(object):
    _checkers = []

    class Status(Enum):
        UNKNOWN = 'UNKNOWN'
        """When your application's status is indeterminable."""

        SERVING = 'SERVING'
        """When your application is ready."""

        NOT_SERVING = 'NOT_SERVING'
        """When your application is not ready."""

    def check(self):
        status = Health.Status.SERVING
        return status

    def add_health_update(self, obj):
        self._checkers.append(obj)

    def update_status(self, status):
        for obj in self._checkers:
            obj.update_status(status.value)
