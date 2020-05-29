# -*- coding: utf-8 -*-
import logging


class ExcludeFilter(logging.Filter):
    def __init__(self, rules):
        self.rules = rules

    def filter(self, record):
        for _rule in self.rules:
            if getattr(record, _rule, None) in self.rules[_rule]:
                return False

        return True
