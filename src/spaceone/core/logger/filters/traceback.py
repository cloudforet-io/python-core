import logging


class TracebackFilter(logging.Filter):
    def filter(self, record):
        _traceback = getattr(record, "traceback", None)

        if _traceback is None or _traceback == "":
            record.traceback = ""
        else:
            record.traceback = (
                _traceback.replace("\\n", "\n").replace('\\"', '"').replace("\\t", "\t")
            )

        return True


class TracebackLogFilter(logging.Filter):
    def filter(self, record):
        _traceback = getattr(record, "traceback", None)

        if _traceback is None or _traceback == "":
            record.traceback_log = '""'
        else:
            record.traceback_log = _traceback

        return True
