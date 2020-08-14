from spaceone.core.base import CoreObject
from spaceone.core.transaction import Transaction


class BaseConnector(CoreObject):

    def __init__(self, transaction: Transaction = None, config: dict = None, **kwargs):
        super().__init__(transaction=transaction)

        self.config = config

        for key, value in kwargs.items():
            setattr(self, key, value)
