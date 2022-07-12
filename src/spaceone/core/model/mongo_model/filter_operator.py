from functools import reduce
from mongoengine.queryset.visitor import Q

from spaceone.core.error import *
from spaceone.core import utils

__all__ = ['FILTER_OPERATORS']


def _default_resolver(key, value, operator, is_multiple):
    if is_multiple:
        return reduce(lambda x, y: x | y,
                      map(lambda i: Q(**{f'{key}__{operator}': i}), value))
    else:
        return Q(**{f'{key}__{operator}': value})


def _eq_resolver(key, value, operator, is_multiple):
    return Q(**{key: value})


def _in_resolver(key, value, operator, is_multiple):
    return Q(**{f'{key}__in': value})


def _exists_resolver(key, value, operator, is_multiple):
    if not isinstance(value, bool):
        raise ERROR_OPERATOR_BOOLEAN_TYPE(operator=operator,
                                          condition={'key': key, 'value': value, 'operator': operator})

    return Q(**{f'{key}__exists': value})


def _not_in_resolver(key, value, operator, is_multiple):
    return Q(**{f'{key}__nin': value})


def _match_resolver(key, value, operator, is_multiple):
    if not isinstance(value, dict):
        raise ERROR_OPERATOR_DICT_VALUE_TYPE(operator=operator,
                                             condition={'key': key, 'value': value, 'operator': operator})

    return Q(**{f'{key}__match': value})


def _regex_resolver(key, value, operator, is_multiple):
    if is_multiple:
        return reduce(lambda x, y: x | y,
                      map(lambda i: Q(**{'__raw__': {key: {'$regex': i, '$options': 'i'}}}), value))
    else:
        return Q(**{'__raw__': {key: {'$regex': value, '$options': 'i'}}})


def _datetime_resolver(key, value, operator, is_multiple):
    try:
        dt = utils.iso8601_to_datetime(value)
    except Exception as e:
        raise ERROR_DB_QUERY(reason=f'The value of datetime_{operator} operator is required ISO 8601 format.')
    return Q(**{f'{key}__{operator}': dt})


def _timediff_resolver(key, value, operator, is_multiple):
    try:
        dt = utils.parse_timediff_query(value)
    except Exception as e:
        raise ERROR_DB_QUERY(reason=f'The value of timediff_{operator} operator is invalid. (value = {value})')
    return Q(**{f'{key}__{operator}': dt})


FILTER_OPERATORS = {
    # model operator : (resolver, mongoengine operator, is_multiple)
    'lt': (_default_resolver, 'lt', False),
    'lte': (_default_resolver, 'lte', False),
    'gt': (_default_resolver, 'gt', False),
    'gte': (_default_resolver, 'gte', False),
    'eq': (_eq_resolver, None, False),
    'not': (_default_resolver, 'ne', False),
    'exists': (_exists_resolver, 'exists', False),
    'contain': (_default_resolver, 'icontains', False),
    'not_contain': (_default_resolver, 'not__icontains', False),
    'in': (_in_resolver, None, True),
    'not_in': (_not_in_resolver, None, True),
    'contain_in': (_default_resolver, 'icontains', True),
    'not_contain_in': (_default_resolver, 'not__icontains', True),
    'match': (_match_resolver, 'match', False),
    'regex': (_regex_resolver, None, False),
    'regex_in': (_regex_resolver, None, True),
    'datetime_gt': (_datetime_resolver, 'gt', False),
    'datetime_lt': (_datetime_resolver, 'lt', False),
    'datetime_gte': (_datetime_resolver, 'gte', False),
    'datetime_lte': (_datetime_resolver, 'lte', False),
    'timediff_gt': (_timediff_resolver, 'gt', False),
    'timediff_lt': (_timediff_resolver, 'lt', False),
    'timediff_gte': (_timediff_resolver, 'gte', False),
    'timediff_lte': (_timediff_resolver, 'lte', False),
}
