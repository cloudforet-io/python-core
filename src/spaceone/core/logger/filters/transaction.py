import logging


class TransactionFilter(logging.Filter):
    def __init__(self, transaction=None):
        self.transaction = transaction

    def filter(self, record):
        if self.transaction:
            record.service = self.transaction.service
            record.tnx_id = self.transaction.id
            record.tnx_status = self.transaction.status
            record.peer = self.transaction.get_meta('peer')

            if self.transaction.resource and self.transaction.verb:
                record.tnx_method = f'{self.transaction.resource}.{self.transaction.verb}'
            else:
                record.tnx_method = ''
        else:
            record.service = ""
            record.tnx_id = ""
            record.tnx_status = ""
            record.tnx_method = ""
            record.peer = ""

        return True
