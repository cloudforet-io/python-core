from spaceone.core.locator import Locator
from spaceone.core.transaction import Transaction, get_transaction


class CoreObject(object):

    def __init__(self, *args, **kwargs):
        self.locator = Locator()
        self.transaction_id = None

    @property
    def transaction(self) -> Transaction:
        transaction = get_transaction()
        if transaction:
            self.transaction_id = transaction.id

        return transaction
