from spaceone.core import config
from spaceone.core.base import CoreObject

__all__ = ['BaseConnector']


class BaseConnector(CoreObject):

    name = None

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        connector_name = self.name or self.__class__.__name__
        self.config = config.get_connector(connector_name)
