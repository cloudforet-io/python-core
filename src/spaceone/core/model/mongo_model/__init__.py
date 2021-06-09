import bson
import re
import logging
from datetime import datetime
from functools import reduce
from mongoengine import EmbeddedDocumentField, EmbeddedDocument, Document, QuerySet, register_connection
from pymongo import ReadPreference
from mongoengine.errors import *
from spaceone.core import config
from spaceone.core import utils
from spaceone.core.error import *
from spaceone.core.model import BaseModel
from spaceone.core.model.mongo_model.filter_operator import FILTER_OPERATORS
from spaceone.core.model.mongo_model.stat_operator import STAT_OPERATORS

_REFERENCE_ERROR_FORMAT = r'Could not delete document \((\w+)\.\w+ refers to it\)'
_MONGO_CONNECTIONS = []
_MONGO_INIT_MODELS = []
_LOGGER = logging.getLogger(__name__)


def _raise_reference_error(class_name, message):
    m = re.findall(_REFERENCE_ERROR_FORMAT, message)
    if len(m) > 0:
        raise ERROR_EXIST_RESOURCE(parent=class_name, child=m[0])


class MongoCustomQuerySet(QuerySet):

    def last(self):
        return self.order_by('-id').first()


class MongoModel(Document, BaseModel):

    auto_create_index = True
    case_insensitive_index = False
    meta = {
        'abstract': True,
        'queryset_class': MongoCustomQuerySet,
        'auto_create_index': False
    }

    @classmethod
    def init(cls):
        global_conf = config.get_global()

        if global_conf.get('MOCK_MODE', False) == False:
            cls.connect()

            if cls not in _MONGO_INIT_MODELS:
                cls.auto_create_index = global_conf.get('DATABASE_AUTO_CREATE_INDEX', True)
                cls.case_insensitive_index = global_conf.get('DATABASE_CASE_INSENSITIVE_INDEX', False)
                cls._create_index()

                _MONGO_INIT_MODELS.append(cls)

    @classmethod
    def connect(cls):
        db_alias = cls._meta.get('db_alias', 'default')
        if db_alias not in _MONGO_CONNECTIONS:
            global_conf = config.get_global()
            databases = global_conf.get('DATABASES', {})

            if db_alias not in databases:
                raise ERROR_DB_CONFIGURATION(backend=db_alias)

            db_conf = databases[db_alias].copy()

            if 'read_preference' in db_conf:
                read_preference = getattr(ReadPreference, db_conf['read_preference'], None)
                if read_preference:
                    db_conf['read_preference'] = read_preference
                else:
                    del db_conf['read_preference']

            register_connection(db_alias, **db_conf)

            _MONGO_CONNECTIONS.append(db_alias)

    @classmethod
    def _create_index(cls):
        if cls.auto_create_index:
            indexes = cls._meta.get('indexes', [])

            if len(indexes) > 0:
                _LOGGER.debug(f'Create MongoDB Indexes ({cls.__name__} Model: {len(indexes)} Indexes)')

                for index in indexes:
                    try:
                        if cls.case_insensitive_index:
                            cls.create_index(index, collation={"locale": "en", "strength": 2})
                        else:
                            cls.create_index(index)

                    except Exception as e:
                        _LOGGER.error(f'Index Creation Failure: {e}')

    @classmethod
    def create(cls, data):
        create_data = {}
        unique_fields = []

        for name, field in cls._fields.items():
            if field.unique:
                if isinstance(field.unique_with, str):
                    unique_fields.append([field.name, field.unique_with])
                elif isinstance(field.unique_with, list):
                    unique_fields.append([field.name] + field.unique_with)
                else:
                    unique_fields.append([field.name])

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

        for unique_field in unique_fields:
            conditions = {}
            for f in unique_field:
                conditions[f] = data.get(f)
            vos = cls.filter(**conditions)
            if vos.count() > 0:
                raise ERROR_SAVE_UNIQUE_VALUES(keys=unique_field)

        try:
            new_vo = cls(**create_data).save()
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

        for name, field in self._fields.items():
            if field.unique:
                if isinstance(field.unique_with, str):
                    unique_fields.append([field.name, field.unique_with])
                elif isinstance(field.unique_with, list):
                    unique_fields.append([field.name] + field.unique_with)
                else:
                    unique_fields.append([field.name])

            if getattr(field, 'auto_now', False):
                if name not in data.keys():
                    data[name] = datetime.utcnow()

        for unique_field in unique_fields:
            conditions = {'pk__ne': self.pk}
            for f in unique_field:
                conditions[f] = data.get(f)

            vos = self.filter(**conditions)
            if vos.count() > 0:
                raise ERROR_SAVE_UNIQUE_VALUES(keys=unique_field)

        for key in list(data.keys()):
            if key not in updatable_fields:
                del data[key]

        if data != {}:
            try:
                super().update(**data)
                self.reload()
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

    def increment(self, key, amount=1):
        key = key.replace('.', '__')
        inc_data = {
            f'inc__{key}': amount
        }

        super().update(**inc_data)
        self.reload()
        return self

    def decrement(self, key, amount=1):
        key = key.replace('.', '__')
        dec_data = {
            f'dec__{key}': amount
        }

        super().update(**dec_data)
        self.reload()
        return self

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
            only = cls._remove_duplicate_only_keys(only)
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
    def _check_reference_field(cls, key):
        ref_keys = cls._meta.get('reference_query_keys', {}).keys()
        if key in ref_keys:
            return False
        else:
            return True

    @classmethod
    def _get_reference_model(cls, key):
        for ref_key, ref_conf in cls._meta.get('reference_query_keys', {}).items():
            if key.startswith(ref_key) and key[len(ref_key)] == '.':
                if isinstance(ref_conf, dict):
                    ref_model = ref_conf.get('model')
                    foreign_key = ref_conf.get('foreign_key')
                else:
                    ref_model = ref_conf
                    foreign_key = None

                ref_query_key = key.replace(f'{ref_key}.', '')
                if ref_model == 'self':
                    ref_model = cls

                return ref_model, ref_key, ref_query_key, foreign_key

        return None, None, None, None

    @classmethod
    def _change_reference_condition(cls, key, value, operator):
        ref_model, ref_key, ref_query_key, foreign_key = cls._get_reference_model(key)
        if ref_model:
            if value is None:
                return ref_key, value, operator
            else:
                ref_vos, total_count = ref_model.query(
                    filter=[{'k': ref_query_key, 'v': value, 'o': operator}])

                if foreign_key:
                    ref_values = []
                    for ref_vo in ref_vos:
                        ref_value = getattr(ref_vo, foreign_key)
                        if ref_value:
                            ref_values.append(ref_value)
                else:
                    ref_values = list(ref_vos)
                return ref_key, ref_values, 'in'

        else:
            return key, value, operator

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

        if key and operator:
            if key in change_query_keys:
                key = change_query_keys[key]

            if operator not in ['regex', 'regex_in']:
                if cls._check_reference_field(key):
                    key, value, operator = cls._change_reference_condition(key, value, operator)

                    resolver, mongo_operator, is_multiple = FILTER_OPERATORS[operator]

                key = key.replace('.', '__')

            return resolver(key, value, mongo_operator, is_multiple)
        else:
            raise ERROR_DB_QUERY(reason='Filter condition should have key, value and operator.')

    @classmethod
    def _make_filter(cls, filter, filter_or):
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

        return _filter

    @classmethod
    def _remove_duplicate_only_keys(cls, only):
        changed_only = []
        duplicated_only = []
        for key in only:
            exists = False

            for changed_key in changed_only:
                if key == changed_key or key.startswith(f'{changed_key}.'):
                    exists = True
                elif changed_key.startswith(f'{key}.'):
                    duplicated_only.append(changed_key)

            if exists is False:
                changed_only.append(key)

        if len(duplicated_only) > 0:
            changed_only = list(set(changed_only) - set(duplicated_only))

        return changed_only

    @classmethod
    def query(cls, *args, only=None, exclude=None, all_fields=False, filter=None, filter_or=None,
              sort=None, page=None, minimal=False, count_only=False, **kwargs):

        if filter is None:
            filter = []

        if filter_or is None:
            filter_or = []

        if sort is None:
            sort = {}

        if page is None:
            page = {}

        _order_by = None
        minimal_fields = cls._meta.get('minimal_fields')

        _filter = cls._make_filter(filter, filter_or)

        if 'key' in sort:
            if sort.get('desc', False):
                _order_by = f'-{sort["key"]}'
            else:
                _order_by = f'{sort["key"]}'

        try:
            if cls.case_insensitive_index:
                vos = cls.objects.filter(_filter).collation({'locale': 'en', 'strength': 2})
            else:
                vos = cls.objects.filter(_filter)

            if _order_by:
                vos = vos.order_by(_order_by)

            if only:
                if 'key' in sort:
                    if sort['key'] not in only:
                        only.append(sort['key'])
                else:
                    ordering = cls._meta.get('ordering')
                    for key in ordering:
                        if key.startswith('+') or key.startswith('-'):
                            key = key[1:]
                        if key not in only:
                            only.append(key)

                only = cls._remove_duplicate_only_keys(only)
                vos = vos.only(*only)

            if exclude:
                vos = vos.exclude(*exclude)

            if minimal and minimal_fields:
                vos = vos.only(*minimal_fields)

            if all_fields:
                vos = vos.all_fields()

            total_count = vos.count()

            if count_only:
                vos = []

            else:
                if 'limit' in page and page['limit'] > 0:
                    start = page.get('start', 1)
                    if start < 1:
                        start = 1

                    vos = vos[start - 1:start + page['limit'] - 1]

            return vos, total_count

        except Exception as e:
            raise ERROR_DB_QUERY(reason=e)

    @classmethod
    def _check_well_known_type(cls, value):
        if isinstance(value, datetime):
            return f'{value.isoformat()}Z'
        elif isinstance(value, bson.objectid.ObjectId):
            return str(value)
        elif isinstance(value, Document):
            return str(value.id)
        elif isinstance(value, EmbeddedDocument):
            return dict(value.to_mongo())
        else:
            return value

    @classmethod
    def _make_aggregate_values(cls, cursor):
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
    def _make_distinct_values(cls, values):
        changed_values = []
        for value in values:
            changed_values.append(cls._check_well_known_type(value))

        return changed_values

    @classmethod
    def _get_group_fields(cls, condition, _before_group_keys):
        key = condition.get('key', condition.get('k'))
        name = condition.get('name', condition.get('n'))
        operator = condition.get('operator', condition.get('o'))
        value = condition.get('value', condition.get('v'))
        date_format = condition.get('date_format')

        if operator not in STAT_OPERATORS:
            raise ERROR_DB_QUERY(reason=f"'aggregate.group.fields' operator is not supported. "
                                        f"(operator = {STAT_OPERATORS.keys()})")

        if operator not in ['count', 'date'] and key is None:
            raise ERROR_DB_QUERY(reason=f"'aggregate.group.fields' condition requires a key: {condition}")

        if name is None:
            raise ERROR_DB_QUERY(reason=f"'aggregate.group.fields' condition requires a name: {condition}")

        if operator == 'date' and value is None:
            raise ERROR_DB_QUERY(reason=f"'aggregate.group.fields' condition requires a value: {condition}")

        if key in _before_group_keys:
            key = f'_id.{key}'

        return key, name, operator, value, date_format

    @classmethod
    def _get_group_keys(cls, condition, _before_group_keys):
        key = condition.get('key', condition.get('k'))
        name = condition.get('name', condition.get('n'))
        date_format = condition.get('date_format')

        if key is None:
            raise ERROR_DB_QUERY(reason=f"'aggregate.group.keys' condition requires a key: {condition}")

        if name is None:
            raise ERROR_DB_QUERY(reason=f"'aggregate.group.keys' condition requires a name: {condition}")

        if key in _before_group_keys:
            key = f'_id.{key}'

        if date_format:
            rule = {
                '$dateToString': {
                    'format': date_format,
                    'date': f'${key}'
                }
            }
        else:
            rule = f'${key}'

        return key, name, rule

    @classmethod
    def _make_group_rule(cls, options, _before_group_keys):
        _group_keys = []
        _include_project = False
        _include_second_project = False
        _project_fields = {}
        _second_project_fields = {}
        _project_rules = []
        _rules = []
        _group_rule = {
            '$group': {
                '_id': {}
            }
        }
        _keys = options.get('keys', [])
        _fields = options.get('fields', [])

        # if len(_keys) == 0:
        #     raise ERROR_REQUIRED_PARAMETER(key='aggregate.group.keys')

        for condition in _keys:
            key, name, rule = cls._get_group_keys(condition, _before_group_keys)
            _group_keys.append(name)
            _group_rule['$group']['_id'][name] = rule

        for condition in _fields:
            key, name, operator, value, date_format = cls._get_group_fields(condition, _before_group_keys)

            rule = STAT_OPERATORS[operator](key, operator, name, value, date_format)

            if rule.get('group') is not None:
                _group_rule['$group'].update(rule['group'])

            if rule.get('project') is not None:
                _include_project = True
                _project_fields.update(rule['project'])
            else:
                _project_fields[name] = 1

            if rule.get('second_project') is not None:
                _include_second_project = True
                _second_project_fields.update(rule['second_project'])
            else:
                _second_project_fields[name] = 1

        _rules.append(_group_rule)

        if _include_project:
            _rules.append({
                '$project': _project_fields
            })

        if _include_second_project:
            _rules.append({
                '$project': _second_project_fields
            })

        return _rules, _group_keys

    @classmethod
    def _make_unwind_rule(cls, options):
        if 'path' not in options:
            raise ERROR_REQUIRED_PARAMETER(key='aggregate.unwind.path')

        return {
            '$unwind': f"${options['path']}"
        }

    @classmethod
    def _make_count_rule(cls, options):
        if 'name' not in options:
            raise ERROR_REQUIRED_PARAMETER(key='aggregate.count.name')

        return {
            '$count': options['name']
        }

    @classmethod
    def _make_sort_rule(cls, options, _group_keys):
        if 'key' not in options:
            raise ERROR_REQUIRED_PARAMETER(key='aggregate.sort.key')

        if options['key'] in _group_keys:
            sort_name = f'_id.{options["key"]}'
        else:
            sort_name = options['key']

        if options.get('desc', False):
            return {
                '$sort': {sort_name: -1}
            }
        else:
            return {
                '$sort': {sort_name: 1}
            }

    @classmethod
    def _make_aggregate_rules(cls, aggregate):
        _aggregate_rules = []
        _group_keys = []

        if not isinstance(aggregate, list):
            raise ERROR_INVALID_PARAMETER_TYPE(key='aggregate', type='list')

        for stage in aggregate:
            if 'unwind' in stage:
                rule = cls._make_unwind_rule(stage['unwind'])
                _aggregate_rules.append(rule)
            elif 'group' in stage:
                rules, group_keys = cls._make_group_rule(stage['group'], _group_keys)
                _aggregate_rules += rules
                _group_keys += group_keys
            elif 'count' in stage:
                rule = cls._make_count_rule(stage['count'])
                _aggregate_rules.append(rule)
            elif 'sort' in stage:
                rule = cls._make_sort_rule(stage['sort'], _group_keys)
                _aggregate_rules.append(rule)
            else:
                raise ERROR_REQUIRED_PARAMETER(key='aggregate.unwind or aggregate.group or '
                                                   'aggregate.count or aggregate.sort')

        return _aggregate_rules, _group_keys

    @classmethod
    def _stat_aggregate(cls, vos, aggregate, page):
        result = {}
        pipeline = []
        _aggregate_rules, _group_keys = cls._make_aggregate_rules(aggregate)

        for rule in _aggregate_rules:
            pipeline.append(rule)

        if 'limit' in page and page['limit'] > 0:
            limit = page['limit']
            start = page.get('start', 1)
            start = 1 if start < 1 else start

            result['total_count'] = 0
            cursor = vos.aggregate(pipeline + [{'$count': 'total_count'}])
            for c in cursor:
                result['total_count'] = c['total_count']
                break

            if start > 1:
                pipeline.append({
                    '$skip': start - 1
                })

            pipeline.append({
                '$limit': limit
            })

        cursor = vos.aggregate(pipeline)
        result['results'] = cls._make_aggregate_values(cursor)
        return result

    @classmethod
    def _stat_distinct(cls, vos, distinct, page):
        result = {}
        values = vos.distinct(distinct)

        try:
            values.sort()
        except Exception:
            pass

        if 'limit' in page and page['limit'] > 0:
            start = page.get('start', 1)
            if start < 1:
                start = 1

            result['total_count'] = len(values)
            values = values[start - 1:start + page['limit'] - 1]

        result['results'] = cls._make_distinct_values(values)
        return result

    @classmethod
    def stat(cls, *args, aggregate=None, distinct=None, filter=None, filter_or=None, page=None, **kwargs):

        if filter is None:
            filter = []

        if filter_or is None:
            filter_or = []

        if page is None:
            page = {}

        if not (aggregate or distinct):
            raise ERROR_REQUIRED_PARAMETER(key='aggregate')

        _filter = cls._make_filter(filter, filter_or)

        try:
            if cls.case_insensitive_index:
                vos = cls.objects.filter(_filter).collation({'locale': 'en', 'strength': 2})
            else:
                vos = cls.objects.filter(_filter)

            if aggregate:
                return cls._stat_aggregate(vos, aggregate, page)

            elif distinct:
                return cls._stat_distinct(vos, distinct, page)

        except Exception as e:
            raise ERROR_DB_QUERY(reason=e)
