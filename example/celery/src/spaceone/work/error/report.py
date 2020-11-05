from spaceone.core.error import ERROR_BASE, ERROR_NOT_IMPLEMENTED

class ERROR_REPORT(ERROR_BASE):
    pass

class ERROR_NOT_ENOUGH_DATA(ERROR_REPORT):
    _message = 'There is not enough data collected to create the report.'

