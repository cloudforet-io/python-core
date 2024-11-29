import re
import functools
import types
from typing import Union
from dateutil.parser import parse
from datetime import datetime, date
from typing import get_type_hints
from pydantic import ValidationError, BaseModel

from spaceone.core import utils
from spaceone.core.error import *

__all__ = [
    "convert_model",
    "change_value_by_rule",
    "change_only_key",
    "check_required",
    "set_query_page_limit",
    "append_query_filter",
    "change_tag_filter",
    "append_keyword_filter",
    "change_timestamp_value",
    "change_date_value",
    "change_timestamp_filter",
    "check_query_filter",
]


def _raise_pydantic_error(e: ValidationError):
    for error in e.errors():
        if error["type"] == "value_error.missing":
            raise ERROR_REQUIRED_PARAMETER(key=", ".join(error["loc"]))
        elif error["type"].startswith("type_error"):
            raise ERROR_INVALID_PARAMETER_TYPE(
                key=", ".join(error["loc"]), type=error["type"].split(".")[1]
            )
        else:
            raise ERROR_INVALID_PARAMETER(
                key=", ".join(error["loc"]), reason=error["msg"]
            )


def _generate_response(response_iterator: types.GeneratorType) -> types.GeneratorType:
    for response in response_iterator:
        if isinstance(response, BaseModel):
            response = response.dict()

        yield response


def convert_model(func) -> callable:
    @functools.wraps(func)
    def wrapped_func(self, params: dict) -> Union[dict, types.GeneratorType]:
        type_hints = get_type_hints(func)
        params_hint = type_hints.get("params")

        if params_hint and isinstance(params, dict):
            try:
                params = params_hint(**params)
            except ValidationError as e:
                _raise_pydantic_error(e)

        response_or_iterator = func(self, params)

        if isinstance(response_or_iterator, types.GeneratorType):
            return _generate_response(response_or_iterator)
        else:
            if isinstance(response_or_iterator, BaseModel):
                response_or_iterator = response_or_iterator.dict()

        return response_or_iterator

    return wrapped_func


def change_value_by_rule(rule: str, param_key: str, value: any = None) -> any:
    def wrapper(func: callable) -> callable:
        @functools.wraps(func)
        def wrapped_func(cls, params: dict) -> Union[dict, types.GeneratorType]:
            if param_value := params.get(param_key):
                if rule == "APPEND":
                    if isinstance(param_value, list):
                        param_value.append(value)
                    else:
                        param_value = [param_value, value]

                params[param_key] = param_value

            return func(cls, params)

        return wrapped_func

    return wrapper


def change_only_key(change_rule, key_path="only") -> callable:
    def wrapper(func):
        @functools.wraps(func)
        def wrapped_func(cls, params):
            only_keys = change_rule.keys()
            only = utils.get_dict_value(params, key_path, [])
            new_only = []
            for key in only:
                key_match = key.split(".", 1)[0]
                if key_match in only_keys:
                    if change_rule[key_match] not in new_only:
                        new_only.append(change_rule[key_match])
                else:
                    new_only.append(key)

            utils.change_dict_value(params, key_path, new_only)
            return func(cls, params)

        return wrapped_func

    return wrapper


def check_required(required_keys) -> callable:
    def wrapper(func):
        @functools.wraps(func)
        def wrapped_func(cls, params):
            for key in required_keys:
                value = utils.get_dict_value(params, key)
                if value is None:
                    raise ERROR_REQUIRED_PARAMETER(key=key)

            return func(cls, params)

        return wrapped_func

    return wrapper


def set_query_page_limit(default_limit) -> callable:
    def wrapper(func):
        @functools.wraps(func)
        def wrapped_func(cls, params):
            query = params.get("query", {})
            query["page"] = query.get("page", {})
            page_limit = query["page"].get("limit", 1000)

            if page_limit > default_limit:
                page_limit = default_limit

            query["page"]["limit"] = page_limit
            params["query"] = query

            return func(cls, params)

        return wrapped_func

    return wrapper


def append_query_filter(filter_keys) -> callable:
    def wrapper(func):
        @functools.wraps(func)
        def wrapped_func(cls, params):
            query = params.get("query", {})
            query["filter"] = query.get("filter", [])

            for key in filter_keys:
                if key in params:
                    if isinstance(params[key], list):
                        query["filter"].append({"k": key, "v": params[key], "o": "in"})
                    else:
                        query["filter"].append({"k": key, "v": params[key], "o": "eq"})

            params["query"] = query

            return func(cls, params)

        return wrapped_func

    return wrapper


def change_tag_filter(tag_key="tags") -> callable:
    def wrapper(func):
        @functools.wraps(func)
        def wrapped_func(cls, params):
            params["query"] = params.get("query", {})
            change_filter = []
            for condition in params["query"].get("filter", []):
                key = condition.get("key", condition.get("k"))
                if key and key.startswith(f"{tag_key}."):
                    value = condition.get("value", condition.get("v"))
                    operator = condition.get("operator", condition.get("o"))
                    tag_key_split = key.split(".", 1)
                    change_filter.append(
                        {
                            "key": "tags",
                            "value": {
                                "key": tag_key_split[1],
                                "value": _change_match_query(
                                    operator, value, condition
                                ),
                            },
                            "operator": "match",
                        }
                    )
                else:
                    change_filter.append(condition)

            if "only" in params["query"]:
                change_only = []
                for key in params["query"].get("only", []):
                    if key.startswith("tags.") and "tags" not in change_only:
                        change_only.append("tags")
                    else:
                        change_only.append(key)

                params["query"]["only"] = change_only

            params["query"]["filter"] = change_filter
            return func(cls, params)

        return wrapped_func

    return wrapper


def _change_match_query(operator, value, condition) -> callable:
    if operator == "eq":
        return value
    elif operator == "not":
        return {"$ne": value}
    elif operator == "in":
        if not isinstance(value, list):
            raise ERROR_OPERATOR_LIST_VALUE_TYPE(operator=operator, condition=condition)

        return {"$in": value}
    elif operator == "not_in":
        if not isinstance(value, list):
            raise ERROR_OPERATOR_LIST_VALUE_TYPE(operator=operator, condition=condition)

        return {"$nin": value}
    elif operator == "contain":
        return re.compile(value, re.IGNORECASE)
    elif operator == "not_contain":
        return {"$not": re.compile(value, re.IGNORECASE)}
    elif operator == "contain_in":
        if not isinstance(value, list):
            raise ERROR_OPERATOR_LIST_VALUE_TYPE(operator=operator, condition=condition)

        return {"$in": value}
    elif operator == "not_contain_in":
        if not isinstance(value, list):
            raise ERROR_OPERATOR_LIST_VALUE_TYPE(operator=operator, condition=condition)

        return {"$nin": value}
    else:
        raise ERROR_DB_QUERY(reason="Filter operator is not supported.")


def append_keyword_filter(keywords=None) -> callable:
    if keywords is None:
        keywords = []

    def wrapper(func):
        @functools.wraps(func)
        def wrapped_func(cls, params):
            query = params.get("query", {})
            if "keyword" in query:
                query["filter_or"] = query.get("filter_or", [])

                keyword = query["keyword"].strip()
                if len(keyword) > 0:
                    for key in keywords:
                        query["filter_or"].append(
                            {
                                "k": key,
                                "v": list(filter(None, keyword.split(" "))),
                                "o": "contain_in",
                            }
                        )

                del query["keyword"]
                params["query"] = query

            return func(cls, params)

        return wrapped_func

    return wrapper


def change_timestamp_value(
    timestamp_keys=None, timestamp_format="google_timestamp"
) -> callable:
    if timestamp_keys is None:
        timestamp_keys = []

    def wrapper(func):
        @functools.wraps(func)
        def wrapped_func(cls, params):
            change_params = {}

            for key, value in params.items():
                if key in timestamp_keys:
                    if not _is_null(value):
                        value = _convert_datetime_from_timestamp(
                            value, key, timestamp_format
                        )
                        change_params[key] = value
                else:
                    change_params[key] = value

            return func(cls, change_params)

        return wrapped_func

    return wrapper


def change_date_value(date_keys=None, date_format="%Y-%m-%d") -> callable:
    if date_keys is None:
        date_keys = []

    def wrapper(func):
        @functools.wraps(func)
        def wrapped_func(cls, params):
            change_params = {}

            for key, value in params.items():
                if key in date_keys:
                    if not _is_null(value):
                        value = _convert_date_from_string(value, key, date_format)
                        change_params[key] = value
                else:
                    change_params[key] = value

            return func(cls, change_params)

        return wrapped_func

    return wrapper


def change_timestamp_filter(
    filter_keys=None, timestamp_format="google_timestamp"
) -> callable:
    if filter_keys is None:
        filter_keys = []

    def wrapper(func):
        @functools.wraps(func)
        def wrapped_func(cls, params):
            query = params.get("query", {})
            query_filter = query.get("filter")
            query_filter_or = query.get("filter_or")
            if query_filter:
                query["filter"] = _change_timestamp_condition(
                    query_filter, filter_keys, "filter", timestamp_format
                )

            if query_filter_or:
                query["filter_or"] = _change_timestamp_condition(
                    query_filter_or, filter_keys, "filter_or", timestamp_format
                )

            params["query"] = query

            return func(cls, params)

        return wrapped_func

    return wrapper


def _is_null(value) -> bool:
    if value is None or str(value).strip() == "":
        return True

    return False


def _change_timestamp_condition(
    query_filter, filter_keys, filter_type, timestamp_format
) -> list:
    change_filter = []

    for condition in query_filter:
        key = condition.get("k") or condition.get("key")
        value = condition.get("v") or condition.get("value")
        operator = condition.get("o") or condition.get("operator")

        if key in filter_keys:
            value = _convert_datetime_from_timestamp(
                value, f"query.{filter_type}.{key}", timestamp_format
            )

        change_filter.append({"key": key, "value": value, "operator": operator})

    return change_filter


def _convert_datetime_from_timestamp(timestamp, key, timestamp_format) -> datetime:
    type_message = (
        "google.protobuf.Timestamp({seconds: <second>, nanos: <nano second>})"
    )

    try:
        if timestamp_format == "iso8601":
            type_message = "ISO 8601(YYYY-MM-DDThh:mm:ssTZD)"
            return parse(timestamp)
        else:
            seconds = timestamp["seconds"]
            nanos = timestamp.get("nanos")

            # TODO: change nanoseconds to timestamp

            return datetime.utcfromtimestamp((int(seconds)))
    except Exception as e:
        raise ERROR_INVALID_PARAMETER_TYPE(key=key, type=type_message)


def _convert_date_from_string(date_str, key, date_format) -> date:
    try:
        return datetime.strptime(date_str, date_format).date()
    except Exception as e:
        raise ERROR_INVALID_PARAMETER_TYPE(key=key, type=date_format)


def check_query_filter(keywords=None) -> callable:
    if keywords is None:
        keywords = []

    def wrapper(func):
        @functools.wraps(func)
        def wrapped_func(cls, params):
            query = params.get("query", {})
            if "filter" in query:
                for filters in query["filter"]:
                    key = filters.get("key", filters.get("k"))
                    if key in keywords:
                        raise ERROR_INVALID_PARAMETER(
                            key=key, reason="Include secure parameter"
                        )

            if "group_by" in query:
                for group_bys in query["group_by"]:
                    key = group_bys.get("key", group_bys.get("k"))
                    if key in keywords:
                        raise ERROR_INVALID_PARAMETER(
                            key=key, reason="Include secure parameter"
                        )

            if "fields" in query:
                value = query["fields"].get("value", query["fields"].get("v"))
                key = value.get("key", value.get("k"))
                if key in keywords:
                    raise ERROR_INVALID_PARAMETER(
                        key=key, reason="Include secure parameter"
                    )

            if "distinct" in query:
                key = query["distinct"]
                if key in keywords:
                    raise ERROR_INVALID_PARAMETER(
                        key=key, reason="Include secure parameter"
                    )

            return func(cls, params)

        return wrapped_func

    return wrapper
