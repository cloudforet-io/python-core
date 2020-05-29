# -*- coding: utf-8 -*-

from spaceone.core.transaction import Transaction

class BaseConnector(object):

    def __init__(self, transaction=None, conf=None, **kwargs):
        if transaction:
            self.transaction = transaction
        else:
            self.transaction = Transaction()

        self.config = conf

        for k,v in kwargs.items():
            setattr(self, k, v)

        # Connector is last mile entity
        # This does not have locator
