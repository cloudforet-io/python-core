from spaceone.core.error import ERROR_INVALID_ARGUMENT


class ERROR_UNSUPPORT_PROVIDER(ERROR_INVALID_ARGUMENT):
    _message = 'Un Support Provider (provider= {provider})'



class ERROR_UNSUPPORT_SERVICE(ERROR_INVALID_ARGUMENT):
    _message = 'Un Support Service (service= {service}, resource= {resource})'

