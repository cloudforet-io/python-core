from spaceone.core.error import *
from spaceone.core import utils

__all__ = ['STAT_GROUP_OPERATORS', 'STAT_PROJECT_OPERATORS']


def _group_count_resolver(condition, key, operator, name, data_type, sub_conditions, *args):
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


def _group_push_resolver(condition, key, operator, name, data_type, sub_conditions, sub_fields, datetime_fields, *args):
    if not key and len(sub_fields) == 0:
        raise ERROR_DB_QUERY(reason=f"'aggregate.group.fields' condition requires fields: {condition}")

    if sub_conditions:
        raise ERROR_DB_QUERY(reason=f"'aggregate.group.fields' condition's conditions not supported: {condition}")

    push_query = {}

    if key:
        return {
            name: {
                '$push': f'${key}'
            }
        }
    else:
        for sub_field in sub_fields:
            f_key = sub_field.get('key', sub_field.get('k'))
            f_name = sub_field.get('name', sub_field.get('n'))

            if f_key in datetime_fields:
                push_query[f_name] = {
                    '$dateToString': {
                        'format': '%Y-%m-%dT%H:%M:%SZ',
                        'date': f'${f_key}'
                    }
                }
            else:
                push_query[f_name] = f'${f_key}'


        return {
            name: {
                '$push': push_query
            }
        }


def _group_average_resolver(condition, key, operator, name, data_type, sub_conditions, *args):
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
    elif data_type == 'array':
        return {
            name: {'$avg': {'$avg': f'${key}'}}
        }
    else:
        return {
            name: {'$avg': f'${key}'}
        }


def _group_sum_resolver(condition, key, operator, name, data_type, sub_conditions, *args):
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
    elif data_type == 'array':
        return {
            name: {'$sum': {'$sum': f'${key}'}}
        }
    else:
        return {
            name: {'$sum': f'${key}'}
        }


def _group_add_to_set_resolver(condition, key, operator, name, data_type, sub_conditions, *args):
    if key is None:
        raise ERROR_DB_QUERY(reason=f"'aggregate.group.fields' condition requires a key: {condition}")

    if sub_conditions:
        raise ERROR_DB_QUERY(reason=f"'aggregate.group.fields' condition's conditions not supported: {condition}")

    return {
        name: {'$addToSet': f'${key}'}
    }


def _group_merge_objects_resolver(condition, key, operator, name, data_type, sub_conditions, *args):
    if key is None:
        raise ERROR_DB_QUERY(reason=f"'aggregate.group.fields' condition requires a key: {condition}")

    if sub_conditions:
        raise ERROR_DB_QUERY(reason=f"'aggregate.group.fields' condition's conditions not supported: {condition}")

    return {
        name: {'$mergeObjects': f'${key}'}
    }


def _group_default_resolver(condition, key, operator, name, data_type, sub_conditions, *args):
    if key is None:
        raise ERROR_DB_QUERY(reason=f"'aggregate.group.fields' condition requires a key: {condition}")

    if sub_conditions:
        raise ERROR_DB_QUERY(reason=f"'aggregate.group.fields' condition's conditions not supported: {condition}")

    if data_type == 'array':
        return {
            name: {f'${operator}': {f'${operator}': f'${key}'}}
        }
    else:
        return {
            name: {f'${operator}': f'${key}'}
        }


def _project_size_resolver(condition, key, operator, name, fields, group_keys, *args):
    if key is None:
        raise ERROR_DB_QUERY(reason=f"'aggregate.project.key' condition requires a key: {condition}")

    if key in group_keys:
        key = f'_id.{key}'

    return {
        name: {'$size': f'${key}'}
    }


def _project_sum_resolver(condition, key, operator, name, fields, group_keys, *args):
    if key is None:
        raise ERROR_DB_QUERY(reason=f"'aggregate.project.key' condition requires a key: {condition}")

    if key in group_keys:
        key = f'_id.{key}'

    return {
        name: {'$sum': f'${key}'}
    }


def _project_array_to_object_resolver(condition, key, operator, name, fields, group_keys, *args):
    if key is None:
        raise ERROR_DB_QUERY(reason=f"'aggregate.project.fields' condition requires a key: {condition}")

    if key in group_keys:
        key = f'_id.{key}'

    return {
        name: {'$arrayToObject': f'${key}'}
    }


def _project_object_to_array_resolver(condition, key, operator, name, fields, group_keys, *args):
    if key is None:
        raise ERROR_DB_QUERY(reason=f"'aggregate.project.fields' condition requires a key: {condition}")

    if key in group_keys:
        key = f'_id.{key}'

    return {
        name: {'$objectToArray': f'${key}'}
    }


def _project_calculate_sub_query(condition, operator, fields, group_keys, *args):
    supported_operator = ['add', 'subtract', 'multiply', 'divide']

    if operator is None:
        raise ERROR_DB_QUERY(reason=f"'aggregate.project.operator' condition requires a operator: {condition}")

    if operator not in supported_operator:
        raise ERROR_DB_QUERY(reason=f"'aggregate.project.operator' condition operator are not allowed"
                                    f" (supported_operator={supported_operator}): {condition}")

    if fields is None:
        raise ERROR_DB_QUERY(reason=f"'aggregate.project.fields' condition requires a fields: {condition}")

    if operator in ['subtract', 'divide'] and len(fields) != 2:
        raise ERROR_DB_QUERY(reason=f"'aggregate.project.fields' condition requires two fields: {condition}")

    expressions = []
    for key in fields:
        if isinstance(key, str):
            if key in group_keys:
                key = f'_id.{key}'

            expressions.append(f'${key}')
        elif isinstance(key, (int, float)) and not isinstance(key, bool):
            expressions.append(key)
        elif isinstance(key, dict):
            sub_condition = key
            sub_operator = sub_condition.get('operator', sub_condition.get('o'))
            sub_fields = sub_condition.get('fields', sub_condition.get('f'))
            expressions.append(_project_calculate_sub_query(sub_condition, sub_operator, sub_fields, group_keys))
        else:
            raise ERROR_DB_QUERY(reason=f"'aggregate.project.fields' condition accept only string or object or "
                                        f"numeric types: {condition}")

    return {
        f'${operator}': expressions
    }


def _project_calculate_resolver(condition, key, operator, name, fields, group_keys, *args):
    return {
        name: _project_calculate_sub_query(condition, operator, fields, group_keys)
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
    'sum': _project_sum_resolver,
    'array_to_object': _project_array_to_object_resolver,
    'object_to_array': _project_object_to_array_resolver,
    'add': _project_calculate_resolver,
    'subtract': _project_calculate_resolver,
    'multiply': _project_calculate_resolver,
    'divide': _project_calculate_resolver,
}