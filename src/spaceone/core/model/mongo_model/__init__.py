import bson
import re
import logging
import certifi
import copy
from datetime import datetime, date
from dateutil.relativedelta import relativedelta
from functools import reduce, partial
from mongoengine import (
    EmbeddedDocumentField,
    EmbeddedDocument,
    Document,
    QuerySet,
    register_connection,
)
from mongoengine.fields import DateField, DateTimeField, ComplexDateTimeField
from pymongo import ReadPreference
from mongoengine.errors import *
from spaceone.core import config
from spaceone.core import utils
from spaceone.core.error import *
from spaceone.core.model.base_model import BaseModel
from spaceone.core.model.mongo_model.filter_operator import FILTER_OPERATORS
from spaceone.core.model.mongo_model.stat_operator import (
    STAT_GROUP_OPERATORS,
    STAT_PROJECT_OPERATORS,
)

_REFERENCE_ERROR_FORMAT = r"Could not delete document \((\w+)\.\w+ refers to it\)"
_MONGO_INIT_MODELS = []

_LOGGER = logging.getLogger(__name__)


def _raise_reference_error(class_name, message):
    m = re.findall(_REFERENCE_ERROR_FORMAT, message)
    if len(m) > 0:
        raise ERROR_EXIST_RESOURCE(parent=class_name, child=m[0])


class MongoCustomQuerySet(QuerySet):
    def last(self):
        return self.order_by("-id").first()

    def update(self, *args, **kwargs):
        if len(args) > 0 and isinstance(args[0], dict):
            kwargs.update(args[0])
        super().update(**kwargs)

    def increment(self, key, amount=1):
        key = key.replace(".", "__")
        inc_data = {f"inc__{key}": amount}

        super().update(**inc_data)

    def decrement(self, key, amount=1):
        key = key.replace(".", "__")
        dec_data = {f"dec__{key}": amount}

        super().update(**dec_data)

    def set_data(self, key, data):
        key = key.replace(".", "__")
        set_data = {f"set__{key}": data}

        super().update(**set_data)

    def unset_data(self, *keys):
        unset_data = {}

        for key in keys:
            key = key.replace(".", "__")
            unset_data[f"unset__{key}"] = 1

        super().update(**unset_data)

    def append(self, key, data):
        key = key.replace(".", "__")
        append_data = {f"push__{key}": data}

        super().update(**append_data)

    def remove(self, key, data):
        key = key.replace(".", "__")
        remove_data = {f"pull__{key}": data}
        super().update(**remove_data)


class MongoModel(Document, BaseModel):
    auto_create_index = True
    meta = {
        "abstract": True,
        "queryset_class": MongoCustomQuerySet,
        "auto_create_index": False,
    }

    @classmethod
    def init(cls, create_index: bool = True) -> None:
        global_conf = config.get_global()
        databases = global_conf.get("DATABASES", {})
        db_name_prefix = global_conf.get("DATABASE_NAME_PREFIX", "")

        if not global_conf.get("MOCK_MODE", False):
            for alias, db_conf in databases.items():
                is_connect = cls._connect(alias, db_conf, db_name_prefix)
                if is_connect:
                    package_path = config.get_package()
                    model_path = config.get_global("DATABASE_MODEL_PATH", "model")
                    __import__(f"{package_path}.{model_path}", fromlist=["*"])

                    for model in cls.__subclasses__():
                        if create_index:
                            model._create_index()
                        model._load_default_meta()

    @classmethod
    def _connect(cls, alias: str, db_conf: dict, db_name_prefix: str) -> bool:
        is_connect = False
        db_conf = copy.deepcopy(db_conf)
        engine = db_conf.get("engine")
        host = db_conf.get("host")
        db = db_conf.get("db")
        username = db_conf.get("username")

        if engine is None:
            raise ERROR_DB_ENGINE_UNDEFINE(alias=alias)
        del db_conf["engine"]

        if engine == "MongoModel":
            if host and db and username:
                if "read_preference" in db_conf:
                    read_preference = getattr(
                        ReadPreference, db_conf["read_preference"], None
                    )
                    if read_preference:
                        db_conf["read_preference"] = read_preference
                    else:
                        del db_conf["read_preference"]

                db_name: str = db_conf.get("db", "")
                db_conf["db"] = f"{db_name_prefix}{db_name}"

                host: str = str(db_conf.get("host", "")).strip()
                if host.startswith("mongodb+srv://"):
                    db_conf["tlsCAFile"] = certifi.where()

                register_connection(alias, **db_conf)
                is_connect = True
                _LOGGER.debug(f"Create MongoDB Connection: {alias}")

        return is_connect

    @classmethod
    def _load_default_meta(cls):
        cls._meta["datetime_fields"] = []
        for name, field in cls._fields.items():
            if isinstance(field, DateField):
                cls._meta["datetime_fields"].append(name)
            elif isinstance(field, DateTimeField):
                cls._meta["datetime_fields"].append(name)
            elif isinstance(field, ComplexDateTimeField):
                cls._meta["datetime_fields"].append(name)

    @classmethod
    def _create_index(cls) -> None:
        if cls.auto_create_index:
            indexes = cls._meta.get("indexes", [])
            unique_fields = cls._get_unique_fields()

            if len(indexes) > 0 or len(unique_fields) > 0:
                total_index_count = len(indexes) + len(unique_fields)

                _LOGGER.debug(
                    f"Create MongoDB Indexes ({cls.__name__} Model: {total_index_count} Indexes)"
                )

                for unique_field in unique_fields:
                    try:
                        cls.create_index({"fields": unique_field, "unique": True})

                    except Exception as e:
                        _LOGGER.error(f"Unique Index Creation Failure: {e}")

                for index in indexes:
                    try:
                        cls.create_index(index)

                    except Exception as e:
                        if e.code != 85:  # 85: IndexOptionsConflict
                            _LOGGER.error(f"Index Creation Failure: {e}")

    @classmethod
    def _get_unique_fields(cls):
        unique_fields = []
        for name, field in cls._fields.items():
            if field.unique:
                if isinstance(field.unique_with, str):
                    unique_fields.append([field.name, field.unique_with])
                elif isinstance(field.unique_with, list):
                    unique_fields.append([field.name] + field.unique_with)
                else:
                    unique_fields.append([field.name])

        return unique_fields

    @classmethod
    def _trim_value(cls, value: any) -> any:
        if isinstance(value, str):
            return value.strip()
        elif isinstance(value, dict):
            changed_value = {}
            for k, v in value.items():
                if isinstance(k, str):
                    k = k.strip()
                if isinstance(v, str):
                    v = v.strip()
                changed_value[k] = cls._trim_value(v)

            return changed_value
        else:
            return value

    @classmethod
    def create(cls, data):
        create_data = {}

        for name, field in cls._fields.items():
            if name in data:
                create_data[name] = data[name]
            else:
                generate_id = getattr(field, "generate_id", None)
                if generate_id:
                    create_data[name] = utils.generate_id(generate_id)

                if getattr(field, "auto_now", False):
                    create_data[name] = datetime.utcnow()
                elif getattr(field, "auto_now_add", False):
                    create_data[name] = datetime.utcnow()

        for unique_field in cls._get_unique_fields():
            conditions = {}
            for f in unique_field:
                conditions[f] = data.get(f)
            vos = cls.filter(**conditions)
            if vos.count() > 0:
                raise ERROR_SAVE_UNIQUE_VALUES(keys=unique_field)

        for key, value in create_data.items():
            create_data[key] = cls._trim_value(value)

        try:
            new_vo = cls(**create_data).save()
        except Exception as e:
            raise ERROR_DB_QUERY(reason=e)

        return new_vo

    def update(self, data):
        updatable_fields = self._meta.get(
            "updatable_fields",
            list(
                filter(
                    lambda x: x != self._meta.get("id_field", "id"), self._fields.keys()
                )
            ),
        )

        for name, field in self._fields.items():
            if getattr(field, "auto_now", False):
                if name not in data.keys():
                    data[name] = datetime.utcnow()

        for unique_field in self._get_unique_fields():
            conditions = {"pk__ne": self.pk}
            for f in unique_field:
                conditions[f] = data.get(f)

            vos = self.filter(**conditions)
            if vos.count() > 0:
                raise ERROR_SAVE_UNIQUE_VALUES(keys=unique_field)

        for key in list(data.keys()):
            if key not in updatable_fields:
                del data[key]

        if data != {}:
            for key, value in data.items():
                data[key] = self._trim_value(value)

            try:
                super().update(**data)
                self.reload()
            except Exception as e:
                raise ERROR_DB_QUERY(reason=e)

        return self

    def delete(self, *args):
        try:
            super().delete(*args)
        except OperationError as e:
            _raise_reference_error(self.__class__.__name__, str(e))
            raise ERROR_DB_QUERY(reason=e)
        except Exception as e:
            raise ERROR_DB_QUERY(reason=e)

    def terminate(self):
        super().delete()

    def increment(self, key, amount=1):
        key = key.replace(".", "__")
        inc_data = {f"inc__{key}": amount}

        super().update(**inc_data)
        self.reload()
        return self

    def decrement(self, key, amount=1):
        key = key.replace(".", "__")
        dec_data = {f"dec__{key}": amount}

        super().update(**dec_data)
        self.reload()
        return self

    def set_data(self, key, data):
        key = key.replace(".", "__")
        set_data = {f"set__{key}": data}

        super().update(**set_data)
        self.reload()
        return self

    def unset_data(self, *keys):
        unset_data = {}

        for key in keys:
            key = key.replace(".", "__")
            unset_data[f"unset__{key}"] = 1

        super().update(**unset_data)
        self.reload()
        return self

    def append(self, key, data) -> Document:
        key = key.replace(".", "__")
        append_data = {}

        field = getattr(self._fields.get(key, {}), "field", None)
        if field and isinstance(field, EmbeddedDocumentField):
            reference_model = field.document_type_obj
            append_data[f"push__{key}"] = reference_model(**data)
        else:
            append_data[f"push__{key}"] = data

        super().update(**append_data)
        self.reload()
        return self

    def remove(self, key, data):
        key = key.replace(".", "__")
        remove_data = {f"pull__{key}": data}
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
                change_conditions[f"{key}__in"] = value
            else:
                change_conditions[key] = value

        return cls.objects.filter(**change_conditions)

    def to_dict(self) -> dict:
        return dict(self.to_mongo())

    @classmethod
    def _get_target_objects(cls, target):
        if target:
            read_preference = getattr(ReadPreference, target, None)
            if read_preference:
                return cls.objects.read_preference(read_preference)

        return cls.objects

    @staticmethod
    def _check_operator_value(is_multiple, operator, value, condition):
        if is_multiple:
            if not isinstance(value, list):
                raise ERROR_OPERATOR_LIST_VALUE_TYPE(
                    operator=operator, condition=condition
                )

        else:
            if isinstance(value, list):
                raise ERROR_OPERATOR_VALUE_TYPE(operator=operator, condition=condition)

    @classmethod
    def _check_reference_field(cls, key):
        ref_keys = cls._meta.get("reference_query_keys", {}).keys()
        if key in ref_keys:
            return False
        else:
            return True

    @classmethod
    def _get_reference_model(cls, key):
        for ref_key, ref_conf in cls._meta.get("reference_query_keys", {}).items():
            if key.startswith(ref_key) and key[len(ref_key)] == ".":
                if isinstance(ref_conf, dict):
                    ref_model = ref_conf.get("model")
                    foreign_key = ref_conf.get("foreign_key")
                else:
                    ref_model = ref_conf
                    foreign_key = None

                ref_query_key = key.replace(f"{ref_key}.", "")
                if ref_model == "self":
                    ref_model = cls

                return ref_model, ref_key, ref_query_key, foreign_key

        return None, None, None, None

    @classmethod
    def _change_reference_condition(cls, key, value, operator, reference_filter=None):
        ref_model, ref_key, ref_query_key, foreign_key = cls._get_reference_model(key)
        if ref_model:
            if value is None:
                return ref_key, value, operator
            else:
                if operator == "not":
                    _filter = [{"k": ref_query_key, "v": value, "o": "eq"}]
                elif operator == "not_in":
                    _filter = [{"k": ref_query_key, "v": value, "o": "in"}]
                else:
                    _filter = [{"k": ref_query_key, "v": value, "o": operator}]
                if reference_filter:
                    for key, value in reference_filter.items():
                        if value:
                            _filter.append({"k": key, "v": value, "o": "eq"})

                ref_vos, total_count = ref_model.query(filter=_filter)

                if foreign_key:
                    ref_values = []
                    for ref_vo in ref_vos:
                        ref_value = getattr(ref_vo, foreign_key)
                        if ref_value:
                            ref_values.append(ref_value)
                else:
                    ref_values = list(ref_vos)

                if operator in ["not", "not_in"]:
                    return ref_key, ref_values, "not_in"
                else:
                    return ref_key, ref_values, "in"

        else:
            return key, value, operator

    @classmethod
    def _make_condition(cls, condition, reference_filter=None):
        key = condition.get("key", condition.get("k"))
        value = condition.get("value", condition.get("v"))
        operator = condition.get("operator", condition.get("o"))
        change_query_keys = cls._meta.get("change_query_keys", {})

        if operator not in FILTER_OPERATORS:
            raise ERROR_DB_QUERY(
                reason=f"Filter operator is not supported. (operator = "
                f"{FILTER_OPERATORS.keys()})"
            )

        resolver, mongo_operator, is_multiple = FILTER_OPERATORS.get(operator)

        cls._check_operator_value(is_multiple, operator, value, condition)

        if key and operator:
            if key in change_query_keys:
                key = change_query_keys[key]

            if operator not in ["regex", "regex_in"]:
                if cls._check_reference_field(key):
                    key, value, operator = cls._change_reference_condition(
                        key, value, operator, reference_filter
                    )

                    resolver, mongo_operator, is_multiple = FILTER_OPERATORS[operator]

                key = key.replace(".", "__")

            return resolver(key, value, mongo_operator, is_multiple)
        else:
            raise ERROR_DB_QUERY(
                reason="Filter condition should have key, value and operator."
            )

    @classmethod
    def _make_filter(cls, filter, filter_or, reference_filter):
        _filter = None
        _filter_or = None

        if len(filter) > 0:
            _filter = reduce(
                lambda x, y: x & y,
                map(
                    partial(cls._make_condition, reference_filter=reference_filter),
                    filter,
                ),
            )

        if len(filter_or) > 0:
            _filter_or = reduce(
                lambda x, y: x | y,
                map(
                    partial(cls._make_condition, reference_filter=reference_filter),
                    filter_or,
                ),
            )

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
                if key == changed_key or key.startswith(f"{changed_key}."):
                    exists = True
                elif changed_key.startswith(f"{key}."):
                    duplicated_only.append(changed_key)

            if exists is False:
                changed_only.append(key)

        if len(duplicated_only) > 0:
            changed_only = list(set(changed_only) - set(duplicated_only))

        return changed_only

    @staticmethod
    def _make_unwind_project_stage(only: list):
        project_fields = []
        for key in only:
            project_fields.append(
                {
                    "key": key,
                    "name": key,
                }
            )

        return {
            "project": {
                "exclude_keys": False,
                "only_keys": False,
                "fields": project_fields,
            }
        }

    @classmethod
    def _stat_with_unwind(
        cls,
        unwind: list,
        only: list = None,
        filter: list = None,
        filter_or: list = None,
        sort: list = None,
        page: dict = None,
        target: str = None,
    ):
        if only is None:
            raise ERROR_DB_QUERY(reason="unwind option requires only option.")

        if not isinstance(unwind, dict):
            raise ERROR_DB_QUERY(reason="unwind option should be dict type.")

        if "path" not in unwind:
            raise ERROR_DB_QUERY(reason="unwind option should have path key.")

        unwind_path = unwind["path"]
        unwind_filter = unwind.get("filter")

        aggregate = [{"unwind": {"path": unwind_path}}]

        if unwind_filter:
            aggregate.append({"match": {"filter": unwind_filter}})

        # Add project stage
        project_fields = []
        for key in only:
            project_fields.append(
                {
                    "key": key,
                    "name": key,
                }
            )

        aggregate.append(
            {
                "project": {
                    "exclude_keys": True,
                    "only_keys": True,
                    "fields": project_fields,
                }
            }
        )

        # Add sort stage
        if sort:
            aggregate.append({"sort": sort})

        response = cls.stat(
            aggregate=aggregate,
            filter=filter,
            filter_or=filter_or,
            page=page,
            tageet=target,
            allow_disk_use=True,
        )

        try:
            vos = []
            total_count = response.get("total_count", 0)
            for result in response.get("results", []):
                unwind_data = utils.get_dict_value(result, unwind_path)
                result = utils.change_dict_value(result, unwind_path, [unwind_data])

                vo = cls(**result)
                vos.append(vo)
        except Exception as e:
            raise ERROR_DB_QUERY(reason=f"Failed to convert unwind result: {e}")

        return vos, total_count

    @classmethod
    def query(
        cls,
        *args,
        only=None,
        exclude=None,
        filter=None,
        filter_or=None,
        sort=None,
        page=None,
        minimal=False,
        count_only=False,
        unwind=None,
        reference_filter=None,
        target=None,
        **kwargs,
    ):
        filter = filter or []
        filter_or = filter_or or []
        sort = sort or []
        page = page or {}

        if unwind:
            return cls._stat_with_unwind(
                unwind, only, filter, filter_or, sort, page, target
            )

        else:
            _order_by = []
            minimal_fields = cls._meta.get("minimal_fields")

            _filter = cls._make_filter(filter, filter_or, reference_filter)

            for sort_option in sort:
                if sort_option.get("desc", False):
                    _order_by.append(f'-{sort_option["key"]}')
                else:
                    _order_by.append(f'{sort_option["key"]}')

            try:
                vos = cls._get_target_objects(target).filter(_filter)

                if len(_order_by) > 0:
                    vos = vos.order_by(*_order_by)

                if only:
                    if len(_order_by) > 0:
                        ordering = _order_by
                    else:
                        ordering = cls._meta.get("ordering")

                    for key in ordering:
                        if key.startswith("+") or key.startswith("-"):
                            key = key[1:]
                        if key not in only:
                            only.append(key)

                    only = cls._remove_duplicate_only_keys(only)
                    vos = vos.only(*only)

                if exclude:
                    vos = vos.exclude(*exclude)

                if minimal and minimal_fields:
                    vos = vos.only(*minimal_fields)

                total_count = vos.count()

                if count_only:
                    vos = []

                else:
                    if "limit" in page and page["limit"] > 0:
                        start = page.get("start", 1)
                        if start < 1:
                            start = 1

                        vos = vos[start - 1 : start + page["limit"] - 1]

                return vos, total_count

            except Exception as e:
                raise ERROR_DB_QUERY(reason=e)

    @classmethod
    def _check_well_known_type(cls, value):
        if isinstance(value, bson.objectid.ObjectId):
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
                if key == "_id" and isinstance(row[key], dict):
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
    def _make_sub_conditions(cls, sub_conditions, _before_group_keys):
        and_sub_conditions = []

        for sub_condition in sub_conditions:
            key = sub_condition.get("key", sub_condition.get("k"))
            value = sub_condition.get("value", sub_condition.get("v"))
            operator = sub_condition.get("operator", sub_condition.get("o"))

            if key is None:
                raise ERROR_DB_QUERY(
                    reason=f"'aggregate.group.fields.conditions.key' condition requires a key: {sub_condition}"
                )

            if value is None:
                raise ERROR_DB_QUERY(
                    reason=f"'aggregate.group.fields.conditions.value' condition requires a value: {sub_condition}"
                )

            if operator is None:
                raise ERROR_DB_QUERY(
                    reason=f"'aggregate.group.fields.conditions.operator' condition requires a operator: {sub_condition}"
                )

            _SUPPORTED_OPERATOR = ["eq", "not", "gt", "gte", "lt", "lte"]

            if operator not in _SUPPORTED_OPERATOR:
                raise ERROR_DB_QUERY(
                    reason=f"'aggregate.group.fields.conditions.operator' condition's {operator} operator is not "
                    f"supported. (supported_operator = {_SUPPORTED_OPERATOR})"
                )

            if key in _before_group_keys:
                key = f"_id.{key}"

            and_sub_conditions.append({f"${operator}": [f"${key}", value]})

        return {"$and": and_sub_conditions}

    @classmethod
    def _get_group_fields(cls, condition, _before_group_keys):
        key = condition.get("key", condition.get("k"))
        name = condition.get("name", condition.get("n"))
        operator = condition.get("operator", condition.get("o"))
        sub_conditions = condition.get("conditions")
        sub_fields = condition.get("fields") or []
        data_type = condition.get("data_type")

        if operator not in STAT_GROUP_OPERATORS:
            raise ERROR_DB_QUERY(
                reason=f"'aggregate.group.fields' condition's {operator} operator is not supported. "
                f"(supported_operator = {list(STAT_GROUP_OPERATORS.keys())})"
            )

        if name is None:
            raise ERROR_DB_QUERY(
                reason=f"'aggregate.group.fields' condition requires a name: {condition}"
            )

        if key in _before_group_keys:
            key = f"_id.{key}"

        for sub_field in sub_fields:
            f_key = sub_field.get("key", sub_field.get("k"))
            f_name = sub_field.get("name", sub_field.get("n"))

            if f_key is None:
                raise ERROR_DB_QUERY(
                    reason=f"'aggregate.group.fields.fields' condition requires a key: {condition}"
                )

            if f_name is None:
                raise ERROR_DB_QUERY(
                    reason=f"'aggregate.group.fields.fields' condition requires a name: {condition}"
                )

            if f_key in _before_group_keys:
                sub_field["key"] = f"_id.{f_key}"

        if sub_conditions:
            sub_conditions = cls._make_sub_conditions(
                sub_conditions, _before_group_keys
            )

        return key, name, operator, sub_fields, sub_conditions, data_type

    @classmethod
    def _get_group_keys(cls, condition, _before_group_keys):
        key = condition.get("key", condition.get("k"))
        name = condition.get("name", condition.get("n"))
        date_format = condition.get("date_format")

        if key is None:
            raise ERROR_DB_QUERY(
                reason=f"'aggregate.group.keys' condition requires a key: {condition}"
            )

        if name is None:
            raise ERROR_DB_QUERY(
                reason=f"'aggregate.group.keys' condition requires a name: {condition}"
            )

        if key in _before_group_keys:
            key = f"_id.{key}"

        if date_format:
            if date_format == "year":
                rule = {"$year": f"${key}"}
            elif date_format == "month":
                rule = {"$month": f"${key}"}
            elif date_format == "day":
                rule = {"$dayOfMonth": f"${key}"}
            else:
                rule = {"$dateToString": {"format": date_format, "date": f"${key}"}}
        else:
            rule = f"${key}"

        return name, rule

    @classmethod
    def _make_group_rule(cls, options, _before_group_keys):
        _group_keys = []
        _group_rule = {"$group": {"_id": {}}}
        _keys = options.get("keys", [])
        _fields = options.get("fields", [])

        if len(_keys) == 0 and len(_fields) == 0:
            raise ERROR_REQUIRED_PARAMETER(
                key="aggregate.group.keys || aggregate.group.fields"
            )

        for condition in _keys:
            name, rule = cls._get_group_keys(condition, _before_group_keys)
            _group_rule["$group"]["_id"][name] = rule
            _group_keys.append(name)

        for condition in _fields:
            (
                key,
                name,
                operator,
                sub_fields,
                sub_conditions,
                data_type,
            ) = cls._get_group_fields(condition, _before_group_keys)

            rule = STAT_GROUP_OPERATORS[operator](
                condition,
                key,
                operator,
                name,
                data_type,
                sub_conditions,
                sub_fields,
                cls._meta["datetime_fields"],
            )
            _group_rule["$group"].update(rule)

        return _group_rule, _group_keys

    @classmethod
    def _get_project_fields(cls, condition):
        key = condition.get("key", condition.get("k"))
        name = condition.get("name", condition.get("n"))
        operator = condition.get("operator", condition.get("o"))
        fields = condition.get("fields", condition.get("f"))

        if operator and operator not in STAT_PROJECT_OPERATORS:
            raise ERROR_DB_QUERY(
                reason=f"'aggregate.project.fields' condition's {operator} operator is not supported. "
                f"(supported_operator = {list(STAT_PROJECT_OPERATORS.keys())})"
            )

        if name is None:
            raise ERROR_DB_QUERY(
                reason=f"'aggregate.project.fields' condition requires a name: {condition}"
            )

        return key, name, operator, fields

    @classmethod
    def _make_project_rule(cls, options, _group_keys):
        _rules = []
        _fields = options.get("fields", [])
        _exclude_keys = options.get("exclude_keys", False)
        _only_keys = options.get("only_keys", False)
        _project_rule = {"$project": {}}

        if len(_fields) == 0:
            raise ERROR_REQUIRED_PARAMETER(key="aggregate.project.fields")

        for condition in _fields:
            key, name, operator, fields = cls._get_project_fields(condition)

            if operator:
                rule = STAT_PROJECT_OPERATORS[operator](
                    condition, key, operator, name, fields, _group_keys
                )
                _project_rule["$project"].update(rule)
            else:
                if key in _group_keys:
                    key = f"_id.{key}"

                if _only_keys:
                    _project_rule["$project"][name] = 1
                else:
                    _project_rule["$project"][name] = f"${key}"

        if _exclude_keys:
            _project_rule["$project"]["_id"] = 0
            _group_keys = []

        return _project_rule, _group_keys

    @classmethod
    def _make_unwind_rule(cls, options):
        if "path" not in options:
            raise ERROR_REQUIRED_PARAMETER(key="aggregate.unwind.path")

        return {"$unwind": f"${options['path']}"}

    @classmethod
    def _make_count_rule(cls, options):
        if "name" not in options:
            raise ERROR_REQUIRED_PARAMETER(key="aggregate.count.name")

        return {"$count": options["name"]}

    @classmethod
    def _make_sort_rule(cls, options, _group_keys):
        order_by = {}

        for sort_option in options:
            if sort_option["key"] in _group_keys:
                sort_key = f'_id.{sort_option["key"]}'
            else:
                sort_key = sort_option["key"]

            order_by[sort_key] = -1 if sort_option.get("desc", False) else 1

        return {"$sort": order_by}

    @classmethod
    def _make_limit_rule(cls, options):
        return {"$limit": options}

    @classmethod
    def _make_skip_rule(cls, options):
        return {"$skip": options}

    @classmethod
    def _make_match_rule(cls, options):
        match_options = {"$and": []}

        for condition in options.get("filter", []):
            key = condition.get("key", condition.get("k"))
            value = condition.get("value", condition.get("v"))
            operator = condition.get("operator", condition.get("o"))

            if not (key and value and operator):
                raise ERROR_REQUIRED_PARAMETER(key="aggregate.match.filter")

            if operator == "eq":
                match_options["$and"].append({key: value})
            elif operator == "not":
                match_options["$and"].append({key: {"$ne": value}})
            elif operator == "in":
                match_options["$and"].append({key: {"$in": value}})
            elif operator == "not_in":
                match_options["$and"].append({key: {"$nin": value}})
            elif operator == "contain":
                match_options["$and"].append({key: {"$regex": f".*{value}.*"}})
            elif operator == "not_contain":
                match_options["$and"].append(
                    {key: {"$not": {"$regex": f".*{value}.*"}}}
                )
            elif operator == "contain_in":
                match_options["$and"].append(
                    {"$or": [{key: {"$regex": f".*{v}.*"}} for v in value]}
                )
            elif operator == "not_contain_in":
                match_options["$and"].append(
                    {"$nor": [{key: {"$regex": f".*{v}.*"}} for v in value]}
                )
            else:
                raise ERROR_DB_QUERY(reason=f"Unsupported operator: {operator}")

        return {"$match": match_options}

    @classmethod
    def _make_aggregate_rules(cls, aggregate):
        _aggregate_rules = []
        _group_keys = []

        if not isinstance(aggregate, list):
            raise ERROR_INVALID_PARAMETER_TYPE(key="aggregate", type="list")

        for stage in aggregate:
            if "unwind" in stage:
                rule = cls._make_unwind_rule(stage["unwind"])
                _aggregate_rules.append(rule)
            elif "group" in stage:
                rule, group_keys = cls._make_group_rule(stage["group"], _group_keys)
                _aggregate_rules.append(rule)
                _group_keys += group_keys
            elif "count" in stage:
                rule = cls._make_count_rule(stage["count"])
                _aggregate_rules.append(rule)
            elif "sort" in stage:
                rule = cls._make_sort_rule(stage["sort"], _group_keys)
                _aggregate_rules.append(rule)
            elif "project" in stage:
                rule, _group_keys = cls._make_project_rule(
                    stage["project"], _group_keys
                )
                _aggregate_rules.append(rule)
            elif "limit" in stage:
                rule = cls._make_limit_rule(stage["limit"])
                _aggregate_rules.append(rule)
            elif "skip" in stage:
                rule = cls._make_skip_rule(stage["skip"])
                _aggregate_rules.append(rule)
            elif "match" in stage:
                rule = cls._make_match_rule(stage["match"])
                _aggregate_rules.append(rule)
            else:
                raise ERROR_REQUIRED_PARAMETER(
                    key="aggregate.unwind or aggregate.group or "
                    "aggregate.count or aggregate.sort or "
                    "aggregate.project or aggregate.limit or "
                    "aggregate.skip"
                )

        return _aggregate_rules

    @classmethod
    def _stat_aggregate(cls, vos, aggregate, page, allow_disk_use):
        result = {}
        pipeline = []
        _aggregate_rules = cls._make_aggregate_rules(aggregate)

        for rule in _aggregate_rules:
            pipeline.append(rule)

        if "limit" in page and page["limit"] > 0:
            limit = page["limit"]
            start = page.get("start", 1)
            start = 1 if start < 1 else start

            result["total_count"] = 0
            cursor = vos.aggregate(pipeline + [{"$count": "total_count"}])
            for c in cursor:
                result["total_count"] = c["total_count"]
                break

            if start > 1:
                pipeline.append({"$skip": start - 1})

            pipeline.append({"$limit": limit})

        if allow_disk_use:
            _LOGGER.debug(f"[_stat_aggregate] allow_disk_use: {allow_disk_use}")
            cursor = vos.aggregate(pipeline, allowDiskUse=True)
        else:
            cursor = vos.aggregate(pipeline)
        result["results"] = cls._make_aggregate_values(cursor)
        return result

    @classmethod
    def _stat_distinct(cls, vos, distinct, page):
        result = {}
        values = vos.distinct(distinct)

        try:
            values.sort()
        except Exception:
            pass

        if "limit" in page and page["limit"] > 0:
            start = page.get("start", 1)
            if start < 1:
                start = 1

            result["total_count"] = len(values)
            values = values[start - 1 : start + page["limit"] - 1]

        result["results"] = cls._make_distinct_values(values)
        return result

    @classmethod
    def stat(
        cls,
        *args,
        aggregate=None,
        distinct=None,
        filter=None,
        filter_or=None,
        page=None,
        reference_filter=None,
        target="SECONDARY_PREFERRED",
        allow_disk_use=False,
        **kwargs,
    ):
        filter = filter or []
        filter_or = filter_or or []
        page = page or {}

        if not (aggregate or distinct):
            raise ERROR_REQUIRED_PARAMETER(key="aggregate")

        _filter = cls._make_filter(filter, filter_or, reference_filter)

        try:
            vos = cls._get_target_objects(target).filter(_filter)

            if aggregate:
                return cls._stat_aggregate(vos, aggregate, page, allow_disk_use)

            elif distinct:
                return cls._stat_distinct(vos, distinct, page)

        except Exception as e:
            if not isinstance(e, ERROR_BASE):
                e = ERROR_UNKNOWN(message=str(e))

            raise ERROR_DB_QUERY(reason=e.message)

    @classmethod
    def _check_field_group(cls, field_group):
        for key in field_group:
            if key.startswith("_total_"):
                raise ERROR_INVALID_PARAMETER(
                    key="field_group",
                    reason="Field group keys cannot contain _total_ characters.",
                )

    @classmethod
    def _make_group_keys(cls, group_by, date_field, granularity=None):
        group_keys = []
        for group_option in group_by:
            if isinstance(group_option, dict):
                key = group_option.get("key")
                name = group_option.get("name")

                if not (key and name):
                    raise ERROR_INVALID_PARAMETER(
                        key="group_by",
                        reason="group_by option requires a key and name.",
                    )
            elif isinstance(group_option, str):
                key = group_option
                name = key.rsplit(".", 1)[-1:][0]
            else:
                raise ERROR_INVALID_PARAMETER(
                    key="group_by",
                    reason="group_by option should be dict or str type.",
                )

            group_keys.append({"key": key, "name": name})

        if granularity and granularity in ["DAILY", "MONTHLY", "YEARLY"]:
            group_keys.append({"key": date_field, "name": "date"})

        return group_keys

    @classmethod
    def _make_group_fields(cls, fields):
        group_fields = []
        for name, condition in fields.items():
            cls._check_condition(condition)
            operator = condition["operator"]
            key = condition.get("key")
            fields = condition.get("fields")
            data_type = condition.get("data_type")

            group_field = {"name": name, "operator": operator, "data_type": data_type}

            if operator != "count":
                group_field["key"] = key

            if operator == "push" and fields:
                push_fields = []
                for key, value in fields.items():
                    push_fields.append({"key": value, "name": key})

                group_field["fields"] = push_fields

            group_fields.append(group_field)

        return group_fields

    @classmethod
    def _check_condition(cls, condition):
        key = condition.get("key")
        operator = condition.get("operator")
        fields = condition.get("fields", [])

        if operator is None:
            raise ERROR_REQUIRED_PARAMETER(key="query.fields.operator")

        # Check Operator
        if operator not in ["count", "push"] and key is None:
            raise ERROR_REQUIRED_PARAMETER(key="query.fields.key")

        if operator == "push" and not key and len(fields) == 0:
            raise ERROR_REQUIRED_PARAMETER(key="query.fields.fields")

    @classmethod
    def _make_field_group_keys(cls, group_keys, field_group):
        field_group_keys = []
        for group_option in group_keys:
            key = group_option["name"]
            name = group_option["name"]
            if name not in field_group:
                if name == "date":
                    field_group_keys.append({"key": "date", "name": "date"})
                else:
                    field_group_keys.append({"key": key, "name": name})

        return field_group_keys

    @classmethod
    def _make_field_group_fields(cls, group_fields, field_group):
        field_group_fields = []
        for group_field in group_fields:
            field_name = group_field["name"]
            operator = group_field["operator"]

            if operator in ["sum", "average", "max", "min", "count"]:
                field_group_fields.append(
                    {
                        "key": field_name,
                        "name": f"_total_{field_name}",
                        "operator": "sum" if operator == "count" else operator,
                    }
                )

            field_group_fields.append(
                {
                    "name": field_name,
                    "operator": "push",
                    "fields": cls._make_field_group_push_fields(
                        field_name, field_group
                    ),
                }
            )

        return field_group_fields

    @classmethod
    def _make_field_group_push_fields(cls, field_name, field_group):
        push_fields = [{"name": "value", "key": field_name}]

        for field_group_key in field_group:
            push_fields.append({"name": field_group_key, "key": field_group_key})

        return push_fields

    @classmethod
    def _make_field_group_query(cls, group_keys, group_fields, field_group):
        field_group_query = {
            "group": {
                "keys": cls._make_field_group_keys(group_keys, field_group),
                "fields": cls._make_field_group_fields(group_fields, field_group),
            }
        }

        return [field_group_query]

    @classmethod
    def _make_select_query(cls, select):
        select_query = {"project": {"fields": [], "exclude_keys": True}}

        supported_operator = ["add", "subtract", "multiply", "divide", "size", "sum"]

        for name, condition in select.items():
            if isinstance(condition, str):
                select_query["project"]["fields"].append(
                    {"name": name, "key": condition}
                )
            elif isinstance(condition, dict):
                operator = condition.get("operator")
                key = condition.get("key")
                fields = condition.get("fields")

                if operator not in supported_operator:
                    raise ERROR_INVALID_PARAMETER(
                        key="query.select.operator",
                        reason=f"supported_operator = {supported_operator}",
                    )

                if key:
                    select_query["project"]["fields"].append(
                        {"name": name, "operator": operator, "key": key}
                    )
                elif fields:
                    select_query["project"]["fields"].append(
                        {"name": name, "operator": operator, "fields": fields}
                    )
                else:
                    raise ERROR_REQUIRED_PARAMETER(key="query.select.fields")

        return [select_query]

    @classmethod
    def _make_sort_query(cls, sort, group_fields, has_field_group):
        sort_query = {"sort": []}
        if has_field_group:
            group_field_names = []
            for group_field in group_fields:
                group_field_names.append(group_field["name"])

            for condition in sort:
                key = condition.get("key")
                desc = condition.get("desc", False)

                if key in group_field_names:
                    key = f"_total_{key}"

                sort_query["sort"].append({"key": key, "desc": desc})
        else:
            sort_query["sort"] = sort

        return [sort_query]

    @classmethod
    def _make_page_query(cls, page):
        page_query = []
        start = page.get("start")
        limit = page.get("limit")

        if limit:
            if start:
                page_query.append({"skip": start - 1})

            page_query.append({"limit": limit + 1})

        return page_query

    @classmethod
    def _make_date_filter(cls, date_field, key, operator):
        return [{"k": date_field, "v": key, "o": operator}]

    @classmethod
    def _parse_start_and_end_time(cls, key, value) -> date:
        if isinstance(value, date):
            return value
        elif isinstance(value, datetime):
            return value.date()
        elif isinstance(value, str):
            if len(value) == 4:
                date_format = "%Y"
                date_type = "YYYY"
            elif len(value) == 7:
                date_format = "%Y-%m"
                date_type = "YYYY-MM"
            else:
                date_format = "%Y-%m-%d"
                date_type = "YYYY-MM-DD"

            try:
                dt = datetime.strptime(value, date_format).date()

                if date_type == "YYYY":
                    if key == "start":
                        return datetime(dt.year, 1, 1)
                    else:
                        return datetime(dt.year, 1, 1) + relativedelta(years=1)
                elif date_type == "YYYY-MM":
                    if key == "start":
                        return datetime(dt.year, dt.month, 1)
                    else:
                        return datetime(dt.year, dt.month, 1) + relativedelta(months=1)
                else:
                    if key == "start":
                        return datetime(dt.year, dt.month, dt.day)
                    else:
                        return datetime(dt.year, dt.month, dt.day) + relativedelta(
                            days=1
                        )

            except Exception as e:
                raise ERROR_INVALID_PARAMETER_TYPE(key=key, type=date_type)
        else:
            raise ERROR_INVALID_PARAMETER(
                key=key, reason=f"{key} option should be datetime or str"
            )

    @classmethod
    def _convert_date_value(cls, date_value, date_field_format):
        if isinstance(date_field_format, str):
            return date_value.strftime(date_field_format)
        else:
            return date_value

    @classmethod
    def analyze(
        cls,
        *args,
        granularity=None,
        fields=None,
        select=None,
        group_by=None,
        field_group=None,
        filter=None,
        filter_or=None,
        page=None,
        sort=None,
        start=None,
        end=None,
        date_field="date",
        date_field_format="%Y-%m-%d",
        reference_filter=None,
        target="SECONDARY_PREFERRED",
        allow_disk_use=False,
        **kwargs,
    ):
        if fields is None:
            raise ERROR_REQUIRED_PARAMETER(key="fields")

        filter = filter or []
        filter_or = filter_or or []
        group_by = group_by or []
        sort = sort or []
        page = page or {}
        field_group = field_group or []
        cls._check_field_group(field_group)
        has_field_group = len(field_group) > 0
        page_limit = page.get("limit")

        if start:
            start_time: date = cls._parse_start_and_end_time("start", start)
            start_value = cls._convert_date_value(start_time, date_field_format)
            filter += cls._make_date_filter(date_field, start_value, "gte")

        if end:
            end_time: date = cls._parse_start_and_end_time("end", end)
            end_value = cls._convert_date_value(end_time, date_field_format)
            filter += cls._make_date_filter(date_field, end_value, "lt")

        group_keys = cls._make_group_keys(group_by, date_field, granularity)
        group_fields = cls._make_group_fields(fields)

        query = {
            "filter": filter,
            "filter_or": filter_or,
            "aggregate": [{"group": {"keys": group_keys, "fields": group_fields}}],
            "target": target,
            "allow_disk_use": allow_disk_use,
            "reference_filter": reference_filter,
        }

        if select:
            query["aggregate"] += cls._make_select_query(select)

        if has_field_group:
            query["aggregate"] += cls._make_field_group_query(
                group_keys, group_fields, field_group
            )

        if len(sort) > 0:
            query["aggregate"] += cls._make_sort_query(
                sort, group_fields, has_field_group
            )

        if page:
            query["aggregate"] += cls._make_page_query(page)

        response = cls.stat(**query)

        if page_limit:
            response["more"] = len(response["results"]) > page_limit
            response["results"] = response["results"][:page_limit]

        return response
