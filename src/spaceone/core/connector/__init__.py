from spaceone.core.base import CoreObject


class BaseConnector(CoreObject):

    def __init__(self, config: dict = None, **kwargs):
        super().__init__()

        self.config = config or {}

        for key, value in kwargs.items():
            setattr(self, key, value)
