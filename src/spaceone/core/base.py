from spaceone.core.locator import Locator
from spaceone.core.transaction import Transaction


class CoreObject(object):

    def __init__(self, transaction: Transaction = None):

        if transaction:
            self.transaction = transaction
        else:
            self.transaction = Transaction()

        self.locator = Locator(self.transaction)
