from spaceone.core.base import CoreObject
from spaceone.core.transaction import Transaction


class BaseManager(CoreObject):

    def __init__(self, transaction: Transaction = None, **kwargs):
        super().__init__(transaction=transaction)

        for key, value in kwargs.items():
            setattr(self, key, value)
