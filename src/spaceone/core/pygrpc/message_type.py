import logging
from datetime import datetime

import six
from spaceone.api.core.v1 import query_pb2, handler_pb2
from google.protobuf import struct_pb2
from google.protobuf.empty_pb2 import Empty
from google.protobuf.timestamp_pb2 import Timestamp

__all__ = ['change_value_type', 'change_struct_type', 'change_list_value_type',
           'change_timestamp_type', 'change_empty_type']

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
