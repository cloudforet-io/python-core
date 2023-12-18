import logging
import copy
import json
from fnmatch import fnmatch


class MaskingFilter(logging.Filter):
    def __init__(self, rules):
        self.rules = rules

    def filter(self, record):
        tnx_method = getattr(record, "tnx_method", None)
        parameter = getattr(record, "parameter", None)

        if tnx_method and parameter:
            record.parameter = self._check_masking(tnx_method, parameter)
        return True

    def _check_masking(self, tnx_method, params):
        masking_parameter = copy.deepcopy(params)

        if tnx_method in self.rules:
            return self._masking(masking_parameter, self.rules[tnx_method])

        """
        masking_parameter = copy.deepcopy(params)
        
        for _rule in self.rules:
            if fnmatch(tnx_method, _rule):
                masking_parameter = self._masking(masking_parameter, self.rules[_rule])
        """
        return masking_parameter

    @staticmethod
    def _masking(parameter, patterns):
        for _p in parameter:
            if _p in patterns:
                parameter[_p] = "********"

        return parameter
