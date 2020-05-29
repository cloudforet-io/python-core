import logging
from datetime import datetime

import six
from spaceone.api.core.v1 import query_pb2, handler_pb2
from google.protobuf import struct_pb2
from google.protobuf.empty_pb2 import Empty
from google.protobuf.timestamp_pb2 import Timestamp

__all__ = ['change_value_type', 'change_struct_type', 'change_list_value_type', 'change_timestamp_type',
           'change_handler_authorization_response', 'change_stat_query', 'get_well_known_types']

_INT_OR_FLOAT = six.integer_types + (float,)
LOGGER = logging.getLogger(__name__)


def change_value_type(value):
    if value is None:
        return {'null_value': 'NULL_VALUE'}
    elif isinstance(value, bool):
        return {'bool_value': value}
    elif isinstance(value, six.string_types):
        return {'string_value': value}
    elif isinstance(value, _INT_OR_FLOAT):
        return {'number_value': value}
    elif isinstance(value, dict):
        change_value = {'struct_value': struct_pb2.Struct()}
        change_value['struct_value'].update(value)
        return change_value
    elif isinstance(value, list):
        change_value = {'list_value': struct_pb2.ListValue()}
        change_value['list_value'].extend(value)
        return change_value
    else:
        return value


def change_struct_type(value):
    if isinstance(value, dict):
        change_value = struct_pb2.Struct()
        change_value.update(value)
        return change_value
    else:
        return value


def change_list_value_type(value):
    if isinstance(value, list):
        change_value = struct_pb2.ListValue()
        change_value.extend(value)
        return change_value
    else:
        return value


def change_timestamp_type(value):
    if isinstance(value, datetime):
        ts = Timestamp()
        ts.FromDatetime(value)
        return ts
    elif isinstance(value, str):
        ts = Timestamp()
        ts.FromJsonString(value)
        return ts
    else:
        return value


def change_empty_type(value):
    return Empty()


def _change_filter_type(condition):
    if 'value' in condition:
        condition['value'] = change_value_type(condition['value'])
    elif 'v' in condition:
        condition['v'] = change_value_type(condition['v'])

    return condition


def change_query(value):
    change_value = value.copy()
    change_value['filter'] = map(_change_filter_type, value.get('filter', []))
    change_value['filter_or'] = map(_change_filter_type, value.get('filter_or', []))

    return query_pb2.Query(**change_value)


def change_stat_query(value):
    change_value = value.copy()
    change_value['filter'] = map(_change_filter_type, value.get('filter', []))
    change_value['filter_or'] = map(_change_filter_type, value.get('filter_or', []))

    return query_pb2.StatisticsQuery(**change_value)


def change_handler_authentication_request(value):
    return handler_pb2.AuthenticationRequest(**value)


def change_handler_authorization_request(value):
    change_value = value.copy()
    change_value['parameter'] = change_struct_type(value.get('parameter', {}))
    return handler_pb2.AuthorizationRequest(**change_value)


def change_handler_authorization_response(value):
    change_value = {
        'role_type': value['role_type'],
        'changed_parameter': change_struct_type(value['changed_parameter'])
    }

    return handler_pb2.AuthorizationResponse(**change_value)


def get_well_known_types():
    return {
        '.google.protobuf.Value': change_value_type,
        '.google.protobuf.Struct': change_struct_type,
        '.google.protobuf.ListValue': change_list_value_type,
        '.google.protobuf.Timestamp': change_timestamp_type,
        '.google.protobuf.Empty': change_empty_type,
        '.spaceone.api.core.v1.Query': change_query,
        '.spaceone.api.core.v1.StatisticsQuery': change_stat_query,
        '.spaceone.api.core.v1.AuthorizationRequest': change_handler_authorization_request,
        '.spaceone.api.core.v1.AuthenticationRequest': change_handler_authentication_request,
    }
