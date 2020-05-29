import re
import secrets
import uuid
import datetime
from urllib.parse import urlparse
import yaml
import urllib
from typing import Tuple
from functools import reduce


def generate_id(prefix: str = 'id', nbytes: int = 6) -> str:
    random_id = secrets.token_hex(nbytes)
    return f'{prefix}-{random_id}'


def random_string() -> str:
    return uuid.uuid4().hex


def load_yaml(yaml_str: str) -> dict:
    try:
        return yaml.load(yaml_str, Loader=yaml.Loader)
    except Exception:
        raise ValueError(f'YAML Load Error: {yaml_str}')


def load_yaml_from_file(yaml_file: str) -> dict:
    try:
        with open(yaml_file, 'r') as f:
            return load_yaml(f)
    except Exception:
        raise Exception(f'YAML Load Error: {yaml_file}')


def load_yaml_from_url(url: str):
    try:
        f = urllib.urlopen(url)
        return load_yaml(f.read())
    except Exception as e:
        raise Exception(f'Http call error: {url}. e={e}')


def parse_endpoint(endpoint: str) -> dict:
    try:
        o = urlparse(endpoint)
    except Exception:
        raise ValueError(f'Endpoint is invalid. ({endpoint})')

    return {
        'scheme': o.scheme,
        'hostname': o.hostname,
        'port': o.port,
        'path': o.path
    }


def parse_grpc_uri(uri: str) -> dict:
    try:
        endpoint_info = parse_endpoint(uri)

        if endpoint_info['scheme'] != 'grpc':
            raise ValueError(f'gRPC endpoint type is invalid. ({uri})')

        version, api_class, method = \
            filter(lambda x: x.strip() != '', endpoint_info['path'].split('/'))
    except Exception:
        raise ValueError(f'gRPC URI is invalid. ({uri})')

    return {
        'endpoint': f'{endpoint_info["hostname"]}:{endpoint_info["port"]}',
        'version': version,
        'api_class': api_class,
        'method': method
    }


def deep_merge(from_dict: dict, into_dict: dict) -> dict:
    for key, value in from_dict.items():
        if isinstance(value, dict):
            node = into_dict.setdefault(key, {})
            deep_merge(value, node)
        else:
            into_dict[key] = value

    return into_dict


def get_dict_value(data: dict, dotted_key: str, default_value=None):
    try:
        return reduce(dict.__getitem__, dotted_key.split('.'), data)
    except Exception as e:
        return default_value


def change_dict_value(data: dict, dotted_key: str, change_value, change_type='value') -> dict:
    # change_value = func or value(any type)
    if '.' in dotted_key:
        key, rest = dotted_key.split('.', 1)
        if key in data:
            if rest.startswith('[]') and isinstance(data[key], list):
                list_data = []
                for sub_data in data[key]:
                    if rest.strip() == '[]':
                        list_data.append(_change_value_by_type(change_type, sub_data, change_value))
                    else:
                        sub_rest = rest.split('.', 1)[1]
                        list_data.append(change_dict_value(sub_data, sub_rest, change_value, change_type))
                data[key] = list_data
            elif isinstance(data[key], dict):
                data[key] = change_dict_value(data[key], rest, change_value, change_type)
    else:
        if dotted_key in data:
            data[dotted_key] = _change_value_by_type(change_type, data[dotted_key], change_value)

    return data


def _change_value_by_type(change_type, original_value, change_value):
    if change_type == 'value':
        return change_value
    elif change_type == 'func':
        return change_value(original_value)
    else:
        return original_value


def parse_timediff_query(query: str) -> datetime:
    try:
        time_info, include_operator = _parse_timediff_from_regex(query)
        base_dt = _convert_base_time(time_info['base_time'])
        if include_operator:
            time_delta = _convert_time_delta(time_info['time_delta_number'], time_info['time_delta_unit'])
            if time_info['operator'] == '+':
                return base_dt + time_delta
            else:
                return base_dt - time_delta
        else:
            return base_dt
    except Exception as e:
        raise ValueError(f'Timediff format is invalid. (value={query})')


def _convert_time_delta(time_delta_number: str, time_delta_unit: str) -> datetime.timedelta:
    _time_delta_map = {
        's': 'seconds',
        'm': 'minutes',
        'h': 'hours',
        'd': 'days',
        'w': 'weeks'
    }

    time_delta_params = {
        _time_delta_map[time_delta_unit]: int(time_delta_number)
    }

    return datetime.timedelta(**time_delta_params)


def _convert_base_time(time_str: str) -> datetime:
    now = datetime.datetime.utcnow()
    if time_str == 'now':
        return now
    elif time_str == 'now/d':
        return datetime.datetime.combine(now.date(), datetime.time(0))
    elif time_str == 'now/w':
        today = datetime.datetime.combine(now.date(), datetime.time(0))
        return today - datetime.timedelta(days=now.date().weekday())


def _parse_timediff_from_regex(query: str) -> Tuple[dict, bool]:
    p = r'^\s?(?P<base_time>now(\/[dw])?)\s?' \
        r'(?P<operator>[+|-])\s?' \
        r'(?P<time_delta_number>\d+)' \
        r'(?P<time_delta_unit>[s|m|h|d|w])\s?$'
    rule = re.compile(p)
    match = rule.match(query)
    if match:
        return match.groupdict(), True
    else:
        if query.strip() not in ['now', 'now/d', 'now/w']:
            raise ValueError(f'Timediff format is invalid. (value={query})')

        return {'base_time': query.strip()}, False


if __name__ == '__main__':
    pass
