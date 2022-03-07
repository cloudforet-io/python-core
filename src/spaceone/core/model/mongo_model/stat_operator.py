from spaceone.core.error import *
from spaceone.core import utils

__all__ = ['STAT_GROUP_OPERATORS', 'STAT_PROJECT_OPERATORS']


def _group_count_resolver(condition, key, operator, name, sub_conditions, *args):
    if sub_conditions:
        return {
            name: {
                '$sum': {
                    '$cond': [
                        sub_conditions,
                        1,
                        0
                    ]
                }
            }
        }
    else:
        return {
            name: {'$sum': 1}
        }


def _group_push_resolver(condition, key, operator, name, sub_conditions, sub_fields, *args):
    if len(sub_fields) == 0:
        raise ERROR_DB_QUERY(reason=f"'aggregate.group.fields' condition requires fields: {condition}")

    if sub_conditions:
        raise ERROR_DB_QUERY(reason=f"'aggregate.group.fields' condition's conditions not supported: {condition}")

    push_query = {}

    for sub_field in sub_fields:
        f_key = sub_field.get('key', sub_field.get('k'))
        f_name = sub_field.get('name', sub_field.get('n'))

        push_query[f_name] = f'${f_key}'

    return {
        name: {
            '$push': push_query
        }
    }


def _group_average_resolver(condition, key, operator, name, sub_conditions, *args):
    if key is None:
        raise ERROR_DB_QUERY(reason=f"'aggregate.group.fields' condition requires a key: {condition}")

    if sub_conditions:
        return {
            name: {
                '$avg': {
                    '$cond': [
                        sub_conditions,
                        f'${key}',
                        0
                    ]
                }
            }
        }
    else:
        return {
            name: {'$avg': f'${key}'}
        }


def _group_sum_resolver(condition, key, operator, name, sub_conditions, *args):
    if key is None:
        raise ERROR_DB_QUERY(reason=f"'aggregate.group.fields' condition requires a key: {condition}")

    if sub_conditions:
        return {
            name: {
                '$sum': {
                    '$cond': [
                        sub_conditions,
                        f'${key}',
                        0
                    ]
                }
            }
        }
    else:
        return {
            name: {'$sum': f'${key}'}
        }


def _group_add_to_set_resolver(condition, key, operator, name, sub_conditions, *args):
    if key is None:
        raise ERROR_DB_QUERY(reason=f"'aggregate.group.fields' condition requires a key: {condition}")

    if sub_conditions:
        raise ERROR_DB_QUERY(reason=f"'aggregate.group.fields' condition's conditions not supported: {condition}")

    return {
        name: {'$addToSet': f'${key}'}
    }


def _group_merge_objects_resolver(condition, key, operator, name, sub_conditions, *args):
    if key is None:
        raise ERROR_DB_QUERY(reason=f"'aggregate.group.fields' condition requires a key: {condition}")

    if sub_conditions:
        raise ERROR_DB_QUERY(reason=f"'aggregate.group.fields' condition's conditions not supported: {condition}")

    return {
        name: {'$mergeObjects': f'${key}'}
    }


def _group_default_resolver(condition, key, operator, name, sub_conditions, *args):
    if key is None:
        raise ERROR_DB_QUERY(reason=f"'aggregate.group.fields' condition requires a key: {condition}")

    if sub_conditions:
        raise ERROR_DB_QUERY(reason=f"'aggregate.group.fields' condition's conditions not supported: {condition}")

    return {
        name: {f'${operator}': f'${key}'}
    }


def _project_size_resolver(condition, key, operator, name, *args):
    if key is None:
        raise ERROR_DB_QUERY(reason=f"'aggregate.project.fields' condition requires a key: {condition}")

    return {
        name: {'$size': f'${key}'}
    }


def _project_array_to_object_resolver(condition, key, operator, name, *args):
    if key is None:
        raise ERROR_DB_QUERY(reason=f"'aggregate.project.fields' condition requires a key: {condition}")

    return {
        name: {'$arrayToObject': f'${key}'}
    }


def _project_object_to_array_resolver(condition, key, operator, name, *args):
    if key is None:
        raise ERROR_DB_QUERY(reason=f"'aggregate.project.fields' condition requires a key: {condition}")

    return {
        name: {'$objectToArray': f'${key}'}
    }


STAT_GROUP_OPERATORS = {
    'count': _group_count_resolver,
    'sum': _group_sum_resolver,
    'average': _group_average_resolver,
    'max': _group_default_resolver,
    'min': _group_default_resolver,
    'first': _group_default_resolver,
    'last': _group_default_resolver,
    'push': _group_push_resolver,
    'add_to_set': _group_add_to_set_resolver,
    'merge_objects': _group_merge_objects_resolver,
}

STAT_PROJECT_OPERATORS = {
    'size': _project_size_resolver,
    'array_to_object': _project_array_to_object_resolver,
    'object_to_array': _project_object_to_array_resolver,
}