from spaceone.core.error import *

__all__ = ['STAT_OPERATORS']


def _stat_count_resolver(key, operator, name):
    return {name: {'$sum': 1}}


def _stat_average_resolver(key, operator, name):
    return {name: {'$avg': f'${key}'}}


def _stat_add_to_set_resolver(key, operator, name):
    return {name: {'$addToSet': f'${key}'}}


def _stat_merge_objects_resolver(key, operator, name):
    return {name: {'$mergeObjects': f'${key}'}}


def _stat_default_resolver(key, operator, name):
    return {name: {f'${operator}': f'${key}'}}


STAT_OPERATORS = {
    'count': _stat_count_resolver,
    'sum': _stat_default_resolver,
    'average': _stat_average_resolver,
    'max': _stat_default_resolver,
    'min': _stat_default_resolver,
    'size': _stat_add_to_set_resolver,
    'add_to_set': _stat_add_to_set_resolver,
    'merge_objects': _stat_merge_objects_resolver
}
