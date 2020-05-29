# -*- coding: utf-8 -*-
from spaceone.core.locator import Locator
from spaceone.core.transaction import Transaction


class BaseManager(object):

    def __init__(self, transaction=None, **kwargs):

        if transaction:
            self.transaction = transaction
        else:
            self.transaction = Transaction()

        self.locator = Locator(self.transaction)

        for k,v in kwargs.items():
            setattr(self, k, v)
