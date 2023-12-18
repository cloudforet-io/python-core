import logging
import json


class ErrorFilter(logging.Filter):
    def filter(self, record):
        if getattr(record, "error_code", None) is None:
            record.error_code = ""

        if getattr(record, "error_message", None) is None:
            record.error_message = ""

        if getattr(record, "traceback", None) is None:
            record.traceback = ""
        else:
            record.traceback = json.dumps(record.traceback)

        return True
