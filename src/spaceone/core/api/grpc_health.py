import logging

from spaceone.core import config
from spaceone.core.locator import Locator
from spaceone.core.actuator.health import Health
from grpc_health.v1.health import HealthServicer, SERVICE_NAME
from grpc_health.v1 import health_pb2, health_pb2_grpc

_LOGGER = logging.getLogger(__name__)


class GRPCHealth(HealthServicer):

    def __init__(self, experimental_non_blocking=True,
                 experimental_thread_pool=None):
        super().__init__(experimental_non_blocking, experimental_thread_pool)
        locator = Locator()
        self.actuator = locator.get_actuator('Health')
        self.actuator.add_health_update(self)
        self.actuator.check()

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
            status = self.actuator.check()

            if isinstance(status, Health.Status):
                status = status.value
                self.update_status(status)
            else:
                _LOGGER.debug(f'[Check] status is not type of Health.Status. (status={type(status)})')
        except Exception as e:
            _LOGGER.error(f'[Check] Health Check Error: {e}')

        return health_pb2.HealthCheckResponse(status=status)

    def update_status(self, status):
        service_name = config.get_service()
        self.set(service_name, status)
