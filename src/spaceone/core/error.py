class ERROR_BASE(Exception):
    _status_code = "INTERNAL"
    _error_code = "ERROR_BASE"
    _message = "Base Error Class"
    _meta = {}

    def __init__(self, _error_code=None, _meta=None, **kwargs):
        if isinstance(_meta, dict):
            self._meta = _meta

        if _error_code:
            self._error_code = _error_code.strip()
        else:
            self._error_code = self.__class__.__name__

        try:
            self._message = self.message.format(**kwargs)
        except Exception:
            raise ERROR_MESSAGE_FORMAT(
                error_class=self.__class__.__name__, key=str(kwargs)
            )

    @property
    def message(self):
        return self._message

    @property
    def error_code(self):
        return self._error_code

    @error_code.setter
    def error_code(self, value):
        self._error_code = value

    @property
    def status_code(self):
        return self._status_code

    @status_code.setter
    def status_code(self, value):
        self._status_code = value

    @property
    def meta(self):
        return self._meta

    @meta.setter
    def meta(self, key, value):
        self._meta[key] = value

    def _repr(self):
        return (
            "\n"
            f"\terror_code = {self._error_code}\n"
            f"\tstatus_code = {self._status_code}\n"
            f"\tmessage = {self._message}"
        )

    def __repr__(self):
        return self._repr()

    def __str__(self):
        return self._repr()


class ERROR_INVALID_ARGUMENT(ERROR_BASE):
    _status_code = "INVALID_ARGUMENT"
    _message = "Argument is invalid."


class ERROR_REQUIRED_X_DOMAIN_ID(ERROR_INVALID_ARGUMENT):
    _message = "System token requires 'x_domain_id' in metadata."


class ERROR_REQUIRED_PARAMETER(ERROR_INVALID_ARGUMENT):
    _message = "Required parameter. (key = {key})"


class ERROR_INVALID_PARAMETER_TYPE(ERROR_INVALID_ARGUMENT):
    _message = "Parameter type is invalid. (key = {key}, type = {type})"


class ERROR_INVALID_PARAMETER(ERROR_INVALID_ARGUMENT):
    _message = "Parameter is invalid. (key = {key}, reason = {reason})"


class ERROR_NOT_FOUND(ERROR_INVALID_ARGUMENT):
    _status_code = "NOT_FOUND"
    _message = "Resource cannot be found or access is denied. ({key} = {value})"


class ERROR_NOT_UNIQUE(ERROR_INVALID_ARGUMENT):
    _status_code = "ALREADY_EXISTS"
    _message = "Tried to save duplicate unique key. ({key} = {value})"


class ERROR_SAVE_UNIQUE_VALUES(ERROR_NOT_UNIQUE):
    _message = "Tried to save duplicate unique values. (keys = {keys})"


class ERROR_EXIST_RESOURCE(ERROR_INVALID_ARGUMENT):
    _message = "'{child}' resources is existed in {parent}."


class ERROR_OPERATOR_VALUE_TYPE(ERROR_INVALID_ARGUMENT):
    _message = (
        "The value of '{operator} operator' does not support list type. ({condition})"
    )


class ERROR_OPERATOR_LIST_VALUE_TYPE(ERROR_INVALID_ARGUMENT):
    _message = "The value of '{operator} operator' must be a list type. ({condition})"


class ERROR_OPERATOR_DICT_VALUE_TYPE(ERROR_INVALID_ARGUMENT):
    _message = (
        "The value of '{operator} operator' must be a dictionary type. ({condition})"
    )


class ERROR_OPERATOR_BOOLEAN_TYPE(ERROR_INVALID_ARGUMENT):
    _message = (
        "The value of '{operator} operator' must be a boolean type. ({condition})"
    )


class ERROR_INVALID_FORMAT(ERROR_INVALID_ARGUMENT):
    _message = "Value format is invalid. ({key} = {value} ! {rule})"


class ERROR_JSON_FORMAT(ERROR_INVALID_ARGUMENT):
    _message = "JSON format is invalid. ({key} = {value})"


class ERROR_AUTHENTICATE_FAILURE(ERROR_INVALID_ARGUMENT):
    _status_code = "UNAUTHENTICATED"
    _message = "Authenticate failure. (message = {message})"


class ERROR_PERMISSION_DENIED(ERROR_INVALID_ARGUMENT):
    _status_code = "PERMISSION_DENIED"
    _message = "Permission denied."


class ERROR_UNKNOWN(ERROR_BASE):
    _status_code = "INTERNAL"
    _message = "{message}"


class ERROR_REQUEST_TIMEOUT(ERROR_UNKNOWN):
    _status_code = "DEADLINE_EXCEEDED"
    _message = "Request timeout!"


class ERROR_TRANSACTION_STATUS(ERROR_UNKNOWN):
    _message = "Transaction status is incorrect. (status = {status})"


class ERROR_UNSUPPORTED_API(ERROR_UNKNOWN):
    _message = "Call Unsupported API. (reason = {reason})"


class ERROR_CONFIGURATION(ERROR_UNKNOWN):
    _message = "Configuration is invalid. (key = {key})"


class ERROR_DB_ENGINE_UNDEFINE(ERROR_CONFIGURATION):
    _message = "Database engine is undefined. (alias = {alias})"


class ERROR_CACHE_CONFIGURATION(ERROR_CONFIGURATION):
    _message = "Cache is not configured. (alias = {alias})"


class ERROR_CACHE_ENGINE_UNDEFINE(ERROR_CONFIGURATION):
    _message = "Cache engine is undefined. (alias = {alias})"


class ERROR_CONNECTOR_CONFIGURATION(ERROR_CONFIGURATION):
    _message = "Connector configuration is invalid. (backend = {backend})"


class ERROR_GRPC_CONFIGURATION(ERROR_CONFIGURATION):
    _message = "gRPC client configuration is invalid. ({endpoint}/{service}/{method})"


class ERROR_HANDLER_CONFIGURATION(ERROR_CONFIGURATION):
    _message = (
        "Handler configuration is invalid. (handler = {handler}, reason = {reason})"
    )


class ERROR_CONNECTOR_CONFIGURATION(ERROR_CONFIGURATION):
    _message = "Connector configuration is invalid. (connector = {connector}, reason = {reason})"


class ERROR_LOG_CONFIG(ERROR_CONFIGURATION):
    _message = "Log configuration is invalid. (reason = {reason})"


class ERROR_WRONG_CONFIGURATION(ERROR_CONFIGURATION):
    _message = "Configuration is invalid. ({key})"


class ERROR_DB_QUERY(ERROR_UNKNOWN):
    _message = "Database query failed. (reason = {reason})"


class ERROR_CACHE_OPTION(ERROR_UNKNOWN):
    _message = (
        "Does not support the cache option. (method = {method}, option = {option})"
    )


class ERROR_CACHE_TIMEOUT(ERROR_UNKNOWN):
    _message = "Cache timeout error. (config = {config})"


class ERROR_CACHE_ENCODE(ERROR_UNKNOWN):
    _message = "Cache data encoding failed. (reason = {reason})"


class ERROR_CACHE_DECODE(ERROR_UNKNOWN):
    _message = "Cache data decoding failed. (reason = {reason})"


class ERROR_CACHEABLE_VALUE_TYPE(ERROR_UNKNOWN):
    _message = "The value of cache.cacheable must be a dict type."


class ERROR_INTERNAL_API(ERROR_UNKNOWN):
    _message = "{message}"


class ERROR_UNAVAILAVBLE(ERROR_UNKNOWN):
    _status_code = "UNAVAILABLE"
    _message = "Server is unavailable. (reason = {reason})"


class ERROR_QUEUE_PUT(ERROR_UNAVAILAVBLE):
    _message = "Queue data put failed. (reason = {reason})"


class ERROR_QUEUE_GET(ERROR_UNAVAILAVBLE):
    _message = "Queue data get failed. (reason = {reason})"


class ERROR_GRPC_CONNECTION(ERROR_UNAVAILAVBLE):
    _status_code = "UNAVAILABLE"
    _message = "Server is unavailable. (channel = {channel}, message = {message})"


class ERROR_GRPC_TLS_HANDSHAKE(ERROR_GRPC_CONNECTION):
    _message = "TLS handshake failed. (reason = {reason})"


class ERROR_HANDLER(ERROR_UNKNOWN):
    _message = "'{handler_type} handler' import failed. (reason = {reason})"


class ERROR_CONNECTOR(ERROR_UNKNOWN):
    _message = "'{connector} handler' import failed. (reason = {reason})"


class ERROR_CONNECTOR_LOAD(ERROR_CONNECTOR):
    _message = "Failed to load connector. (connector = {connector}, reason = {reason})"


class ERROR_LOCATOR(ERROR_UNKNOWN):
    _message = "'{name}' load failed. (reason = {reason})"


class ERROR_TASK_LOCATOR(ERROR_LOCATOR):
    _message = "Call locator failure. (locator = {locator}, name = {name})"


class ERROR_TASK_METHOD(ERROR_UNKNOWN):
    _message = "Call method failure. (name = {name}, method = {method})"


class ERROR_NOT_IMPLEMENTED(ERROR_UNKNOWN):
    _status_code = "UNIMPLEMENTED"
    _message = "Not implemented."


class ERROR_MESSAGE_FORMAT(ERROR_UNKNOWN):
    _message = (
        "Error message format is invalid. (error_class = {error_class}, key = {key})"
    )


class ERROR_CACHE_KEY_FORMAT(ERROR_UNKNOWN):
    _message = "Cache key format is invalid. (key = {key})"


class ERROR_SERVICE_ACCOUNT_CANNOT_BE_DELETED_WITH_EXISTING_APP(ERROR_INVALID_ARGUMENT):
    _status_code = "INVALID_OPERATION"
    _message = """Service Account cannot be deleted as long as an associated App exists. 
    Please delete the App before deleting the Service Account. (error_class = {error_class}, key = {key})"""
