import json
import re
from datetime import datetime
from django.utils.safestring import mark_safe


time_frequency = {
    '1': {'minute': '*', 'hour': '*', 'day_of_month': '*', 'month_of_year': '*', 'day_of_week': '*'},
    '3': {'minute': '*/3', 'hour': '*', 'day_of_month': '*', 'month_of_year': '*', 'day_of_week': '*'},
    '5': {'minute': '*/5', 'hour': '*', 'day_of_month': '*', 'month_of_year': '*', 'day_of_week': '*'},
    '15': {'minute': '*/15', 'hour': '*', 'day_of_month': '*', 'month_of_year': '*', 'day_of_week': '*'},
    '30': {'minute': '*/30', 'hour': '*', 'day_of_month': '*', 'month_of_year': '*', 'day_of_week': '*'},
    '60': {'minute': '0', 'hour': '*', 'day_of_month': '*', 'month_of_year': '*', 'day_of_week': '*'},
    '120': {'minute': '0', 'hour': '*/2', 'day_of_month': '*', 'month_of_year': '*', 'day_of_week': '*'},
    '180': {'minute': '0', 'hour': '*/3', 'day_of_month': '*', 'month_of_year': '*', 'day_of_week': '*'},
    '360': {'minute': '0', 'hour': '*/6', 'day_of_month': '*', 'month_of_year': '*', 'day_of_week': '*'},
    '720': {'minute': '0', 'hour': '*/12', 'day_of_month': '*', 'month_of_year': '*', 'day_of_week': '*'},
    'D': {'minute': '0', 'hour': '0', 'day_of_month': '*', 'month_of_year': '*', 'day_of_week': '*'},
    'M': {'minute': '0', 'hour': '0', 'day_of_month': '1', 'month_of_year': '*', 'day_of_week': '*'},
    'W': {'minute': '0', 'hour': '0', 'day_of_month': '*', 'month_of_year': '*', 'day_of_week': '0'}
}


def dumps(data: dict | str) -> str:
    if isinstance(data, dict):
        data = json.dumps(data, indent=2)
    regex = re.compile(r'\[\n(?:\[.+?\]|[^\[\]]*)*?\]', re.DOTALL)
    if not regex.search(data):
        return data
    for i in regex.findall(data):
        l = json.dumps(json.loads(i))
        return dumps(data.replace(i, l))


def schedule(time_frame) -> bool:
    call_time = datetime.now()
    second = call_time.second > 0 and call_time.second <= 3
    if time_frame == '1':
        return second
    elif time_frame == '3':
        return call_time.minute % 3 == 0 and second
    elif time_frame == '5':
        return call_time.minute % 5 == 0 and second
    elif time_frame == '15':
        return call_time.minute % 15 == 0 and second
    elif time_frame == '30':
        return call_time.minute % 30 == 0 and second
    elif time_frame == '60':
        return call_time.minute == 0 and second
    elif time_frame == '120':
        return call_time.minute == 0 and call_time.hour % 2 == 0 and second
    elif time_frame == '180':
        return call_time.minute == 0 and call_time.hour % 3 == 0 and second
    elif time_frame == '360':
        return call_time.minute == 0 and call_time.hour % 6 == 0 and second
    elif time_frame == '720':
        return call_time.minute == 0 and call_time.hour % 12 == 0 and second
    elif time_frame == 'D':
        return call_time.minute == 0 and call_time.hour == 0 and second
    elif time_frame == 'M':
        return call_time.minute == 0 and call_time.hour == 0 and call_time.day == 1 and second
    elif time_frame == 'W':
        return call_time.minute == 0 and call_time.hour == 0 and call_time.weekday() == 0 and second
    return False


def get_pretty_dict(data) -> str:
    data = dict(sorted(data.items()))
    data = json.dumps(data, indent=2)
    data = re.sub('"', "'", data)
    return mark_safe(
        f'<pre style="font-size: 1.05em; font-family: monospace;">{data}</pre>'
    )


def get_pretty_text(obj) -> str:
    text = json.dumps(obj, indent=2)
    l = []
    for k, v in obj.items():
        if isinstance(v, float):
            l.append(f'{k}: {v:.5f}')
        else:
            l.append(f'{k}: {v}')
    text = '<br>'.join(l)
    return mark_safe(
        '<span style="font-size: 1.05em; font-family: monospace;'
        f'white-space: nowrap;">{text}</span>'
    )


def sort_data(parameters: dict, template: dict) -> dict:
    sorted_data = {
        i: parameters.get(i)
        for i in template.keys()
    }
    return sorted_data


def get_client_ip(request) -> str:
    x_forwarded_for = request.headers.get("x-forwarded-for")
    if x_forwarded_for:
        ip = x_forwarded_for.split(",")[0]
    else:
        ip = request.META.get("REMOTE_ADDR")
    return ip


def convert_dict_values(data: dict) -> dict[str, str | int | float]:
    for k, v in data.items():
        if isinstance(v, str):
            if re.search(r'^(?!.*:)[+-]?\d+\.\d+$', v):
                data[k] = round(float(v), 10)
            elif re.search(r'^[-+]?\d+$', v):
                data[k] = int(v)
            elif re.search(r'\w', v):
                data[k] = v
            elif v == '':
                data[k] = None
            if k in ['uTime', 'cTime', 'pTime', 'fillTime', 'ts']:
                try:
                    data[k] = datetime.fromtimestamp(int(v) / 1000).strftime('%d-%m-%Y %H:%M:%S.%f')
                except ValueError:
                    data[k] = v
    return data
