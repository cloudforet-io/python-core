import logging
from enum import Enum

from spaceone.core import config
from grpc_health.v1.health import HealthServicer, SERVICE_NAME
from grpc_health.v1 import health_pb2, health_pb2_grpc

_LOGGER = logging.getLogger(__name__)


class GRPCHealth(HealthServicer):

    def __init__(self, experimental_non_blocking=True,
                 experimental_thread_pool=None):
        super().__init__(experimental_non_blocking, experimental_thread_pool)
        self.health_mgr = HealthManager()
        self.health_mgr.add_health_update(self)
        self.health_mgr.check()

    @property
    def name(self):
        return 'Health'

    @property
    def pb2_grpc_module(self):
        return health_pb2_grpc

    @property
    def service_name(self):
        return SERVICE_NAME

    def Check(self, request, context):
        try:
            status = self.health_mgr.check()
            status = status.value
            self.update_status(status)

        except Exception as e:
            _LOGGER.error(f'[Check] Health Check Error: {e}')
            status = 'UNKNOWN'

        return health_pb2.HealthCheckResponse(status=status)

    def update_status(self, status):
        service_name = config.get_service()
        self.set(service_name, status)


class HealthManager(object):
    _checkers = []

    class Status(Enum):
        UNKNOWN = 'UNKNOWN'
        """When your application's status is indeterminable."""

        SERVING = 'SERVING'
        """When your application is ready."""

        NOT_SERVING = 'NOT_SERVING'
        """When your application is not ready."""

    def check(self):
        status = self.Status.SERVING
        return status

    def add_health_update(self, obj):
        self._checkers.append(obj)

    def update_status(self, status):
        for obj in self._checkers:
            obj.update_status(status.value)