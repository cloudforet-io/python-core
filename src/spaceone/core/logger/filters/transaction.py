# -*- coding: utf-8 -*-
import logging


class TransactionFilter(logging.Filter):
    def __init__(self, transaction=None):
        self.transaction = transaction

    def filter(self, record):
        if self.transaction:
            record.service = self.transaction.service
            record.tnx_id = self.transaction.id
            record.tnx_state = self.transaction.state
            record.peer = self.transaction.get_meta('peer')

            if self.transaction.api_class and self.transaction.method:
                record.tnx_method = f'{self.transaction.api_class}.{self.transaction.method}'
            else:
                record.tnx_method = ''
        else:
            record.service = ""
            record.tnx_id = ""
            record.tnx_state = ""
            record.tnx_method = ""
            record.peer = ""

        return True
