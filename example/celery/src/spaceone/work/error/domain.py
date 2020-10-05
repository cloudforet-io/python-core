# -*- coding: utf-8 -*-
from spaceone.core.error import *


class ERROR_DOMAIN_ALREADY_LOCKED(ERROR_BASE):
    _message = '{domain_id} has already been locked'

