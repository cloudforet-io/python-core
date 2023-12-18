import logging
from spaceone.core.transaction import get_transaction


class TransactionFilter(logging.Filter):
    def filter(self, record):
        if transaction := get_transaction(is_create=False):
            record.service = transaction.service
            record.trace_id = transaction.id
            record.domain_id = transaction.get_meta("authorization.domain_id") or ""
            record.audience = transaction.get_meta("authorization.audience") or ""
            record.role_type = transaction.get_meta("authorization.role_type") or ""
            record.workspace_id = (
                transaction.get_meta("authorization.workspace_id") or ""
            )
            record.peer = transaction.get_meta("peer")
            if transaction.resource and transaction.verb:
                record.tnx_method = f"{transaction.resource}.{transaction.verb}"
            else:
                record.tnx_method = ""
        else:
            record.service = ""
            record.trace_id = ""
            record.domain_id = ""
            record.audience = ""
            record.role_type = ""
            record.workspace_id = ""
            record.tnx_status = ""
            record.tnx_method = ""
            record.peer = ""
        return True
