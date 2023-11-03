from spaceone.core import config
from spaceone.core.base import CoreObject


class BaseConnector(CoreObject):

    def __init__(self, *args, connector_name: str = None, **kwargs):
        super().__init__(*args, **kwargs)
        connector_name = connector_name or self.__class__.__name__
        self.config = config.get_connector(connector_name)
