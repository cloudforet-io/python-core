# -*- coding: utf-8 -*-
import logging
import json


class MessageJsonFilter(logging.Filter):
    def filter(self, record):
        if getattr(record, 'msg', None):
            record.msg_dump = json.dumps(str(record.msg))
        else:
            record.msg_dump = "\"\""

        return True
