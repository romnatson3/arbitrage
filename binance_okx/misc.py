import logging
import json
import re
from datetime import datetime
from django.utils.safestring import mark_safe
from binance_okx.models import Position, Bill, Strategy


logger = logging.getLogger(__name__)


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
        if not v:
            v = ''
        if isinstance(v, float):
            l.append(f'{k}: {v:.10f}'.rstrip('0').rstrip('.'))
        elif isinstance(v, list):
            l.append(
                f'{k}: <pre style="font-size: 1.05em; font-family: monospace;">'
                f'{json.dumps(v, indent=2)}</pre>'
            )
        else:
            l.append(f'{k}: {v}')
    text = '<br>'.join(l)
    return mark_safe(
        '<span style="font-size: 1.05em; font-family: monospace;'
        f'white-space: wrap;">{text}</span>'
    )


def sort_data(parameters: dict, template: dict) -> dict:
    sorted_data = {
        i: parameters.get(i)
        for i in template.keys()
    }
    return sorted_data


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
                    data[k] = datetime.fromtimestamp(int(v) / 1000).strftime('%d-%m-%Y %H:%M:%S.%f')[:-3]
                except ValueError:
                    data[k] = v
    return data
