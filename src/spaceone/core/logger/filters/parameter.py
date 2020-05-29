# -*- coding: utf-8 -*-
import logging
import json


class ParameterFilter(logging.Filter):
    def filter(self, record):
        params = getattr(record, 'parameter', None)

        if params is None:
            record.params = ''
        else:
            record.params = json.dumps(params)

        return True


class ParameterLogFilter(logging.Filter):
    def filter(self, record):
        params = getattr(record, 'parameter', None)

        if params is None or params == '':
            record.params_log = '{}'
        else:
            record.params_log = json.dumps(params)

        return True
