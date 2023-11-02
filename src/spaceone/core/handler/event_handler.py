import logging

from spaceone.core import utils
from spaceone.core.handler import BaseEventHandler
from spaceone.core.error import ERROR_HANDLER_CONFIGURATION

_LOGGER = logging.getLogger(__name__)


class EventGRPCHandler(BaseEventHandler):

    def __init__(self, config):
        super().__init__(config)
        self._initialize()

    def _initialize(self):
        if 'uri' not in self.config:
            _LOGGER.error(f'[_initialize] uri config is undefined.')
            raise ERROR_HANDLER_CONFIGURATION(handler='AuthenticationGRPCHandler')

        self.client: SpaceConnector = self.locator.get_connector(SpaceConnector, endpoint=self.config['uri'])

    def notify(self, status: str, message: dict):
        _LOGGER.debug(f'[notify] {status}: {message}')
