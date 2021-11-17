from spaceone.core.error import *
from spaceone.core import utils

__all__ = ['STAT_OPERATORS']


def _stat_count_resolver(key, operator, name, sub_condition, *args):
    if sub_condition:
        return {
            'group': {
                name: {
                    '$sum': {
                        '$cond': [
                            sub_condition,
                            1,
                            0
                        ]
                    }
                }
            }
        }
    else:
        return {
            'group': {
                name: {'$sum': 1}
            }
        }


def _stat_average_resolver(key, operator, name, sub_condition, *args):
    if sub_condition:
        return {
            'group': {
                name: {
                    '$avg': {
                        '$cond': [
                            sub_condition,
                            f'${key}',
                            0
                        ]
                    }
                }
            }
        }
    else:
        return {
            'group': {
                name: {'$avg': f'${key}'}
            }
        }

def _stat_sum_resolver(key, operator, name, sub_condition, *args):
    if sub_condition:
        return {
            'group': {
                name: {
                    '$sum': {
                        '$cond': [
                            sub_condition,
                            f'${key}',
                            0
                        ]
                    }
                }
            }
        }
    else:
        return {
            'group': {
                name: {'$sum': f'${key}'}
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


def _stat_push_resolver(key, operator, name, sub_condition, sub_fields, *args):
    push_query = {}

    for sub_field in sub_fields:
        f_key = sub_field.get('key', sub_field.get('k'))
        f_name = sub_field.get('name', sub_field.get('n'))

        push_query[f_name] = f'${f_key}'

    return {
        'group': {
            name: {
                '$push': push_query
            }
        }
    }


def _stat_add_to_set_resolver(key, operator, name, *args):
    return {
        'group': {
            name: {'$addToSet': f'${key}'}
        }
    }


def _stat_merge_objects_resolver(key, operator, name, *args):
    return {
        'group': {
            name: {'$mergeObjects': f'${key}'}
        }
    }


def _stat_default_resolver(key, operator, name, *args):
    return {
        'group': {
            name: {f'${operator}': f'${key}'}
        }
    }


STAT_OPERATORS = {
    'count': _stat_count_resolver,
    'sum': _stat_sum_resolver,
    'average': _stat_average_resolver,
    'max': _stat_default_resolver,
    'min': _stat_default_resolver,
    'first': _stat_default_resolver,
    'last': _stat_default_resolver,
    'size': _stat_size_resolver,
    'push': _stat_push_resolver,
    'add_to_set': _stat_add_to_set_resolver,
    'merge_objects': _stat_merge_objects_resolver,
}
