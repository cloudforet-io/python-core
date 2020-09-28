from spaceone.core.error import *
from spaceone.core import utils

__all__ = ['STAT_OPERATORS']


def _stat_count_resolver(key, operator, name, *args):
    return {
        'group': {
            name: {'$sum': 1}
        }
    }


def _stat_average_resolver(key, operator, name, *args):
    return {
        'group': {
            name: {'$avg': f'${key}'}
        }
    }


def _stat_add_to_set_resolver(key, operator, name, *args):
    return {
        'group': {
            name: {'$addToSet': f'${key}'}
        }
    }


def _stat_size_resolver(key, operator, name, *args):
    return {
        'group': {
            name: {'$addToSet': f'${key}'}
        },
        'project': {
            name: {'$size': f'${name}'}
        }
    }


def _stat_merge_objects_resolver(key, operator, name, *args):
    return {
        'group': {
            name: {'$mergeObjects': f'${key}'}
        }
    }


def _stat_date_resolver(key, operator, name, value, date_format):
    dt = utils.parse_timediff_query(value)

    rule = {
        'project': {
            name: {'$add': dt}
        }
    }

    if date_format:
        rule['second_project'] = {
            name: {
                '$dateToString': {
                    'format': date_format,
                    'date': f'${name}'
                }
            }
        }

    return rule


def _stat_default_resolver(key, operator, name, *args):
    return {
        'group': {
            name: {f'${operator}': f'${key}'}
        }
    }


STAT_OPERATORS = {
    'count': _stat_count_resolver,
    'sum': _stat_default_resolver,
    'average': _stat_average_resolver,
    'max': _stat_default_resolver,
    'min': _stat_default_resolver,
    'first': _stat_default_resolver,
    'last': _stat_default_resolver,
    'size': _stat_size_resolver,
    'add_to_set': _stat_add_to_set_resolver,
    'merge_objects': _stat_merge_objects_resolver,
    'date': _stat_date_resolver,
}
