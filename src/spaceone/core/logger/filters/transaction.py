import logging
from spaceone.core.transaction import LOCAL_STORAGE


class TransactionFilter(logging.Filter):

    def filter(self, record):
        if transaction := getattr(LOCAL_STORAGE, 'transaction', None):
            record.service = transaction.service
            record.tnx_id = transaction.id
            record.domain_id = transaction.get_meta('domain_id', '')
            record.user_id = transaction.get_meta('user_id', '')
            record.tnx_status = transaction.status
            record.peer = transaction.get_meta('peer')
            if transaction.resource and transaction.verb:
                record.tnx_method = f'{transaction.resource}.{transaction.verb}'
            else:
                record.tnx_method = ''
        else:
            record.service = ""
            record.tnx_id = ""
            record.domain_id = ""
            record.user_id = ""
            record.tnx_status = ""
            record.tnx_method = ""
            record.peer = ""
        return True
