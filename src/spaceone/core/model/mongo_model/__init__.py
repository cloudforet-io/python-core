import bson
import re
import logging
from datetime import datetime
from functools import reduce
from mongoengine import EmbeddedDocumentField, Document, QuerySet, register_connection
from pymongo.errors import *
from mongoengine.errors import *
from spaceone.core import config
from spaceone.core import utils
from spaceone.core.error import *
from spaceone.core.model import BaseModel
from spaceone.core.model.mongo_model.filter_operator import FILTER_OPERATORS
from spaceone.core.model.mongo_model.stat_operator import STAT_OPERATORS

_MONGO_CONNECTIONS = []
_UNIQUE_ERROR_FORMAT = r'.*index: (\w+)_1_(\w+)_id_1.*'
_REFERENCE_ERROR_FORMAT = r'Could not delete document \((\w+)\.\w+ refers to it\)'
_LOGGER = logging.getLogger(__name__)


def _raise_unique_error(message, unique_fields):
    m = re.findall(_UNIQUE_ERROR_FORMAT, message)
    if len(m) > 0:
        raise ERROR_NOT_UNIQUE_KEYS(keys=list(m[0]))
    else:
        raise ERROR_NOT_UNIQUE_KEYS(keys=unique_fields)


def _raise_reference_error(class_name, message):
    m = re.findall(_REFERENCE_ERROR_FORMAT, message)
    if len(m) > 0:
        raise ERROR_EXIST_RESOURCE(parent=class_name, child=m[0])


class MongoCustomQuerySet(QuerySet):

    def last(self):
        return self.order_by('-id').first()


class MongoModel(Document, BaseModel):
    meta = {
        'abstract': True,
        'queryset_class': MongoCustomQuerySet
    }

    @classmethod
    def connect(cls):
        db_alias = cls._meta.get('db_alias', 'default')
        if db_alias not in _MONGO_CONNECTIONS:
            global_conf = config.get_global()
            if db_alias not in global_conf.get('DATABASES', {}):
                raise ERROR_DB_CONFIGURATION(backend=db_alias)

            db_conf = global_conf['DATABASES'][db_alias].copy()
            register_connection(db_alias, **db_conf)

            _MONGO_CONNECTIONS.append(db_alias)

    @classmethod
    def create(cls, data):
        create_data = {}
        unique_fields = []

        for name, field in cls._fields.items():
            if field.unique:
                unique_fields.append(name)

            if name in data:
                create_data[name] = data[name]
            else:
                generate_id = getattr(field, 'generate_id', None)
                if generate_id:
                    create_data[name] = utils.generate_id(generate_id)

                if getattr(field, 'auto_now', False):
                    create_data[name] = datetime.utcnow()
                elif getattr(field, 'auto_now_add', False):
                    create_data[name] = datetime.utcnow()

        try:
            new_vo = cls(**create_data).save()
        except NotUniqueError as e:
            _raise_unique_error(str(e), unique_fields)
            raise ERROR_DB_QUERY(reason=e)
        except DuplicateKeyError as e:
            _raise_unique_error(str(e), unique_fields)
            raise ERROR_DB_QUERY(reason=e)
        except Exception as e:
            raise ERROR_DB_QUERY(reason=e)

        return new_vo

    def update(self, data):
        unique_fields = []
        updatable_fields = self._meta.get(
            'updatable_fields', list(
                filter(
                    lambda x: x != self._meta.get('id_field', 'id'), self._fields.keys()
                )
            )
        )

        for key in list(data.keys()):
            if key not in updatable_fields:
                del data[key]

        for name, field in self._fields.items():
            if field.unique:
                unique_fields.append(name)

            if getattr(field, 'auto_now', False):
                if name not in data.keys():
                    data[name] = datetime.utcnow()

        if data != {}:
            try:
                super().update(**data)
                self.reload()
            except NotUniqueError as e:
                _raise_unique_error(str(e), unique_fields)
                raise ERROR_DB_QUERY(reason=e)
            except DuplicateKeyError as e:
                _raise_unique_error(str(e), unique_fields)
                raise ERROR_DB_QUERY(reason=e)
            except Exception as e:
                raise ERROR_DB_QUERY(reason=e)

        return self

    def delete(self):
        try:
            super().delete()
        except OperationError as e:
            _raise_reference_error(self.__class__.__name__, str(e))
            raise ERROR_DB_QUERY(reason=e)
        except Exception as e:
            raise ERROR_DB_QUERY(reason=e)

    def terminate(self):
        super().delete()

    def set_data(self, key, data):
        key = key.replace('.', '__')
        set_data = {
            f'set__{key}': data
        }

        super().update(**set_data)
        self.reload()
        return self

    def unset_data(self, *keys):
        unset_data = {}

        for key in keys:
            key = key.replace('.', '__')
            unset_data[f'unset__{key}'] = 1

        super().update(**unset_data)
        self.reload()
        return self

    def append(self, key, data):
        append_data = {}

        field = getattr(self._fields.get(key, {}), 'field', None)
        if field and isinstance(field, EmbeddedDocumentField):
            reference_model = field.document_type_obj
            append_data[f'push__{key}'] = reference_model(**data)
        else:
            append_data[f'push__{key}'] = data

        super().update(**append_data)
        self.reload()
        return self

    def remove(self, key, data):
        remove_data = {
            f'pull__{key}': data
        }
        super().update(**remove_data)
        self.reload()
        return self

    @classmethod
    def get(cls, only=None, **conditions):
        vos = cls.filter(**conditions)

        if vos.count() == 0:
            keys = tuple(conditions.keys())
            values = tuple(conditions.values())

            if len(keys) == 1:
                raise ERROR_NOT_FOUND(key=keys[0], value=values[0])
            else:
                raise ERROR_NOT_FOUND(key=keys, value=values)

        if only:
            vos = vos.only(*only)

        return vos.first()

    @classmethod
    def filter(cls, **conditions):
        change_conditions = {}
        for key, value in conditions.items():
            if isinstance(value, list):
                change_conditions[f'{key}__in'] = value
            else:
                change_conditions[key] = value

        return cls.objects.filter(**change_conditions)

    def to_dict(self):
        return self.to_mongo()

    @staticmethod
    def _check_operator_value(is_multiple, operator, value, condition):
        if is_multiple:
            if not isinstance(value, list):
                raise ERROR_OPERATOR_LIST_VALUE_TYPE(operator=operator, condition=condition)

        else:
            if isinstance(value, list):
                raise ERROR_OPERATOR_VALUE_TYPE(operator=operator, condition=condition)

    @classmethod
    def _check_exact_field(cls, key, value):
        if key in cls._meta.get('exact_fields', []) or value is None:
            return True
        else:
            return False

    @classmethod
    def _check_reference_field(cls, key):
        ref_keys = cls._meta.get('reference_query_keys', {}).keys()
        if key in ref_keys:
            return False
        else:
            return True

    @classmethod
    def _get_reference_model(cls, key):
        for ref_key, ref_model in cls._meta.get('reference_query_keys', {}).items():
            if key.startswith(ref_key) and key[len(ref_key)] == '.':
                ref_model_key = key.replace(f'{ref_key}.', '')
                if ref_model == 'self':
                    return cls, ref_key, ref_model_key
                else:
                    return ref_model, ref_key, ref_model_key

        return None, None, None

    @classmethod
    def _change_reference_condition(cls, key, value, operator, is_exact_field):
        ref_model, ref_key, ref_model_key = cls._get_reference_model(key)
        if ref_model:
            if value is None:
                return ref_key, value, operator, is_exact_field
            else:
                ref_vos, total_count = ref_model.query(
                    filter=[{'k': ref_model_key, 'v': value, 'o': operator}])
                return ref_key, list(ref_vos), 'in', True

        else:
            return key, value, operator, is_exact_field

    @classmethod
    def _make_condition(cls, condition):
        key = condition.get('key', condition.get('k'))
        value = condition.get('value', condition.get('v'))
        operator = condition.get('operator', condition.get('o'))
        change_query_keys = cls._meta.get('change_query_keys', {})

        if operator not in FILTER_OPERATORS:
            raise ERROR_DB_QUERY(reason=f'Filter operator is not supported. (operator = '
                                        f'{FILTER_OPERATORS.keys()})')

        resolver, mongo_operator, is_multiple = FILTER_OPERATORS.get(operator)

        cls._check_operator_value(is_multiple, operator, value, condition)
        is_exact_field = cls._check_exact_field(key, value)

        if key and operator:
            if key in change_query_keys:
                key = change_query_keys[key]

            if operator not in ['regex', 'regex_in']:
                if cls._check_reference_field(key):
                    key, value, operator, is_exact_field = \
                        cls._change_reference_condition(key, value, operator, is_exact_field)

                    resolver, mongo_operator, is_multiple = FILTER_OPERATORS[operator]

                key = key.replace('.', '__')

            return resolver(key, value, mongo_operator, is_multiple, is_exact_field)
        else:
            raise ERROR_DB_QUERY(reason='Filter condition should have key, value and operator.')

    @classmethod
    def query(cls, *args, only=None, exclude=None, all_fields=False, distinct=None,
              filter=[], filter_or=[], sort={}, page={}, minimal=False, count_only=False, **kwargs):
        _filter = None
        _filter_or = None
        _order_by = None
        minimal_fields = cls._meta.get('minimal_fields')


        if len(filter) > 0:
            _filter = reduce(lambda x, y: x & y, map(cls._make_condition, filter))

        if len(filter_or) > 0:
            _filter_or = reduce(lambda x, y: x | y, map(cls._make_condition, filter_or))

        if _filter and _filter_or:
            _filter = _filter & _filter_or
        else:
            _filter = _filter or _filter_or

        if 'key' in sort:
            if sort.get('desc', False):
                _order_by = f'-{sort["key"]}'
            else:
                _order_by = f'{sort["key"]}'

        try:
            vos = cls.objects.filter(_filter)

            if _order_by:
                vos = vos.order_by(_order_by)

            if only:
                vos = vos.only(*only)

            if exclude:
                vos = vos.exclude(*exclude)

            if minimal and minimal_fields:
                vos = vos.only(*minimal_fields)

            if all_fields:
                vos = vos.all_fields()

            if distinct:
                vos = vos.distinct(distinct)
                total_count = len(vos)
            else:
                total_count = vos.count()

            if count_only:
                vos = []

            else:
                if 'limit' in page and page['limit'] > 0:
                    start = page.get('start', 1)
                    if start < 1:
                        start = 1

                    vos = vos[start - 1:start + page['limit'] - 1]

                # vos.select_related(3)

            return vos, total_count

        except Exception as e:
            raise ERROR_DB_QUERY(reason=e)

    @classmethod
    def _check_well_known_type(cls, value):
        if isinstance(value, datetime):
            return f'{value.isoformat()}Z'
        elif isinstance(value, bson.objectid.ObjectId):
            return str(value)
        else:
            return value

    @classmethod
    def _make_stat_values(cls, cursor):
        values = []
        for row in cursor:
            data = {}
            for key, value in row.items():
                if key == '_id' and isinstance(row[key], dict):
                    for group_key, group_value in row[key].items():
                        data[group_key] = cls._check_well_known_type(group_value)
                else:
                    data[key] = cls._check_well_known_type(value)

            values.append(data)

        return values

    @classmethod
    def _get_group_fields(cls, condition):
        key = condition.get('key', condition.get('k'))
        name = condition.get('name', condition.get('n'))
        operator = condition.get('operator', condition.get('o'))

        if operator not in STAT_OPERATORS:
            raise ERROR_DB_QUERY(reason=f"'aggregate.group.fields' operator is not supported. "
                                        f"(operator = {STAT_OPERATORS.keys()})")

        if operator != 'count' and key is None:
            raise ERROR_DB_QUERY(reason=f"'aggregate.group.fields' condition requires a key: {condition}")

        if name is None:
            raise ERROR_DB_QUERY(reason=f"'aggregate.group.fields' condition requires a name: {condition}")

        return key, name, operator

    @classmethod
    def _get_group_keys(cls, condition):
        key = condition.get('key', condition.get('k'))
        name = condition.get('name', condition.get('n'))

        if key is None:
            raise ERROR_DB_QUERY(reason=f"'aggregate.group.keys' condition requires a key: {condition}")

        if name is None:
            raise ERROR_DB_QUERY(reason=f"'aggregate.group.keys' condition requires a name: {condition}")

        return key, name

    @classmethod
    def _make_group_rule(cls, options):
        _group_keys = []
        _all_keys = []
        _include_project = False
        _project_fields = {}
        _rules = []
        _group_rule = {
            '$group': {
                '_id': {}
            }
        }
        _keys = options.get('keys', [])
        _fields = options.get('fields', [])

        if len(_keys) == 0:
            raise ERROR_REQUIRED_PARAMETER(key='aggregate.group.keys')

        for condition in _keys:
            key, name = cls._get_group_keys(condition)
            _all_keys.append(key)
            _group_keys.append(name)
            _group_rule['$group']['_id'][name] = f'${key}'

        for condition in _fields:
            key, name, operator = cls._get_group_fields(condition)

            if key:
                _all_keys.append(key)
            _group_rule['$group'].update(STAT_OPERATORS[operator](key, operator, name))
            _project_fields[name] = 1

            if operator == 'size':
                _include_project = True
                _project_fields[name] = {
                    '$size': f'${name}'
                }

        _rules.append(_group_rule)

        if _include_project:
            _rules.append({
                '$project': _project_fields
            })

        return _rules, _group_keys, _all_keys

    @classmethod
    def _make_unwind_rule(cls, options):
        if 'path' not in options:
            raise ERROR_REQUIRED_PARAMETER(key='aggregate.unwind.path')

        return {
            '$unwind': f"${options['path']}"
        }, options['path']

    @classmethod
    def _make_count_rule(cls, options):
        if 'name' not in options:
            raise ERROR_REQUIRED_PARAMETER(key='aggregate.count.name')

        return {
            '$count': options['name']
        }

    @classmethod
    def _check_lookup_field(cls, ref_key, all_keys):
        for key in all_keys:
            if key.startswith(ref_key) and len(key) > len(ref_key) and key[len(ref_key)] == '.':
                return True

        return False

    @classmethod
    def _make_lookup_rules(cls, options, all_keys):
        all_keys = list(set(all_keys))
        rules = []
        for ref_key, lookup_conf in options.items():
            if cls._check_lookup_field(ref_key, all_keys):
                lookup_from = lookup_conf['from']
                lookup_local = lookup_conf.get('localField', ref_key)
                lookup_foreign = lookup_conf.get('foreignField', '_id')
                rules.append({
                    '$lookup': {
                        'from': lookup_from,
                        'localField': lookup_local,
                        'foreignField': lookup_foreign,
                        'as': ref_key
                    }
                })
                rules.append({
                    '$unwind': {
                        'path': f'${ref_key}',
                        'preserveNullAndEmptyArrays': True
                    }
                })

        return rules

    @classmethod
    def _make_aggregation_rules(cls, aggregate):
        _aggregation_rules = []
        _group_keys = []
        _all_keys = []
        _aggregate_meta = cls._meta.get('aggregate', {})

        if 'group' not in aggregate and 'count' not in aggregate:
            raise ERROR_REQUIRED_PARAMETER(key='aggregate.group or aggregate.count')

        for unwind_options in aggregate.get('unwind', []):
            rule, unwind_path = cls._make_unwind_rule(unwind_options)
            _aggregation_rules.append(rule)
            _all_keys.append(unwind_path)

        if 'group' in aggregate:
            rules, group_keys, all_keys = cls._make_group_rule(aggregate['group'])
            _aggregation_rules += rules
            _group_keys += group_keys
            _all_keys += all_keys

        if 'count' in aggregate:
            rule = cls._make_count_rule(aggregate['count'])
            _aggregation_rules.append(rule)

        if 'lookup' in _aggregate_meta:
            rules = cls._make_lookup_rules(_aggregate_meta['lookup'], _all_keys)
            _aggregation_rules = rules + _aggregation_rules

        return _aggregation_rules

    @classmethod
    def stat(cls, *args, aggregate=None, filter=[], filter_or=[], sort={}, page={}, limit=None, **kwargs):
        if aggregate is None:
            raise ERROR_REQUIRED_PARAMETER(key='aggregate')

        pipeline = []
        _filter = None
        _filter_or = None

        if len(filter) > 0:
            _filter = reduce(lambda x, y: x & y, map(cls._make_condition, filter))

        if len(filter_or) > 0:
            _filter_or = reduce(lambda x, y: x | y, map(cls._make_condition, filter_or))

        if _filter and _filter_or:
            _filter = _filter & _filter_or
        else:
            _filter = _filter or _filter_or

        _aggregation_rules = cls._make_aggregation_rules(aggregate)

        for rule in _aggregation_rules:
            pipeline.append(rule)

        if 'name' in sort:
            if sort.get('desc', False):
                pipeline.append({
                    '$sort': {sort['name']: -1}
                })
            else:
                pipeline.append({
                    '$sort': {sort['name']: 1}
                })

        if 'limit' in page and page['limit'] > 0:
            limit = page['limit']
            start = page.get('start', 1)
            start = 1 if start < 1 else start

            if start > 1:
                pipeline.append({
                    '$skip': start - 1
                })

            pipeline.append({
                '$limit': limit
            })
        else:
            if limit:
                pipeline.append({
                    '$limit': limit
                })

        try:
            vos = cls.objects.filter(_filter)
            cursor = vos.aggregate(pipeline)
            return cls._make_stat_values(cursor)

        except Exception as e:
            raise ERROR_DB_QUERY(reason=e)
