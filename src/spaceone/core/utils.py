import re
import random
import string
import secrets
import datetime
from urllib.parse import urlparse
import yaml
import json
import urllib
from typing import Tuple
from functools import reduce
from pathlib import Path


def generate_id(prefix: str = 'id', nbytes: int = 6) -> str:
    random_id = secrets.token_hex(nbytes)
    return f'{prefix}-{random_id}'


def generate_secret(nbytes: int = 32) -> str:
    return secrets.token_urlsafe(nbytes)


def generate_password(length: int = 8) -> str:
    # create alphanumerical from string constants
    printable = f'{string.ascii_letters}{string.digits}'

    # convert printable from string to list and shuffle
    printable = list(printable)
    random.shuffle(printable)

    # generate random password and convert to string
    random_password = random.choices(printable, k=length)

    # Append one lowercase and one uppercase and one number characters
    random_password.append(random.choices(list(string.ascii_lowercase))[0])
    random_password.append(random.choices(list(string.ascii_uppercase))[0])
    random_password.append(random.choices(list(string.digits))[0])

    random_password = ''.join(random_password)
    return random_password


def random_string(nbytes: int = 6) -> str:
    return secrets.token_hex(nbytes)


def create_dir(path: str):
    p = Path(path)
    p.mkdir(parents=True, exist_ok=True)


def dump_json(data: dict, indent=None, sort_keys=False) -> str:
    try:
        return json.dumps(data, indent=indent, sort_keys=sort_keys)
    except Exception as e:
        raise ValueError(f'JSON Dump Error: {str(e)}')


def load_json(json_str: str) -> dict:
    try:
        return json.loads(json_str)
    except Exception:
        raise ValueError(f'JSON Load Error: {json_str}')


def dump_yaml(data: dict) -> str:
    try:
        return yaml.dump(data)
    except Exception as e:
        raise ValueError(f'YAML Dump Error: {str(e)}')


def save_yaml_to_file(data: dict, yaml_file: str):
    try:
        yaml_str = dump_yaml(data)
        with open(yaml_file, 'w') as f:
            f.write(yaml_str)
            f.close()
    except Exception as e:
        raise ValueError(f'YAML Save Error: {str(e)}')


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


def load_yaml_from_url(url: str) -> dict:
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

        version, service, method = \
            filter(lambda x: x.strip() != '', endpoint_info['path'].split('/'))
    except Exception:
        raise ValueError(f'gRPC URI is invalid. ({uri})')

    return {
        'endpoint': f'{endpoint_info["hostname"]}:{endpoint_info["port"]}',
        'version': version,
        'service': service,
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


def get_dict_value(data: dict, dotted_key: str, default_value=None):
    if '.' in dotted_key:
        key, rest = dotted_key.split(".", 1)

        if isinstance(data, dict) and key in data:
            if isinstance(data[key], list):
                return get_list_values(data[key], rest, default_value)
            else:
                return get_dict_value(data[key], rest, default_value)

        else:
            return default_value
    else:
        if isinstance(data, dict):
            return data.get(dotted_key, default_value)
        else:
            return default_value


def get_list_values(values: list, dotted_key: str, default_value=None):
    list_values = []
    try:
        # Get value by index
        if '.' in dotted_key:
            index, rest = dotted_key.split('.', 1)
            index = int(index)
            if index >= len(values):
                return default_value
            else:
                list_values.append(values[index])
        else:
            return values[int(dotted_key)]

    except Exception:
        list_values = values
        rest = dotted_key

    # Check condition (cond_key:cond_value=>get_key)
    match_option = 'contain'
    if len(rest) > 1 and rest[0] == "$":
        condition = True
        try:
            cond_option, rest = rest[1:].split('=>', 1)
            cond_key, cond_value = cond_option.split(':')
        except Exception as e:
            # Syntax Error
            return default_value

        if cond_value[:1] == '=':
            match_option = 'eq'
            cond_value = cond_value[1:]

        elif cond_value[:1] == '!':
            match_option = 'not'
            cond_value = cond_value[1:]

    else:
        condition = False
        cond_key = None
        cond_value = None

    results = []
    for value in list_values:
        # Get value from condition
        if condition and not _check_condition(match_option, value[cond_key], cond_value):
            continue

        # Get value from dict key
        result = get_dict_value(value, rest)

        if result:
            if isinstance(result, list):
                results += result
            else:
                results.append(result)

    try:
        return list(set(results))
    except Exception:
        return results


def _check_condition(match_option: str, val1, val2):
    val1 = str(val1).lower()
    val2 = str(val2).lower()

    if match_option == 'eq':
        if val1 == val2:
            return True

    elif match_option == 'not':
        if val1.find(val2) < 0:
            return True

    else:
        if val1.find(val2) >= 0:
            return True

    return False


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


if __name__ == '__main__':
    pass
