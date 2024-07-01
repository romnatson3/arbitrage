import csv
import json
import os
import logging
import pathlib
from datetime import datetime
from .helper import SavedOkxOrderId
from django_redis import get_redis_connection


logger = logging.getLogger(__name__)


def save_filled_limit_order_id(account_id: int, data: dict) -> None:
    saved_orders_ids = SavedOkxOrderId(account_id, data['instId'])
    if not data['ordType'] == 'limit':
        logger.info(f'Order {data["ordId"]} is not limit')
        return
    if not data['state'] == 'filled':
        logger.info(f'Limit order {data["ordId"]} is not filled')
        return
    if saved_orders_ids.add(data['ordId']):
        logger.info(f'Limit order {data["ordId"]} is filled and saved')
    else:
        logger.error(f'Failed to save order {data["ordId"]}')


def write_ask_bid_to_csv_by_symbol(data: dict) -> None:
    symbol = data['s']
    binance_ask = str(data['a']).replace('.', ',')
    binance_bid = str(data['b']).replace('.', ',')
    timestamp = datetime.fromtimestamp(int(data['E']) / 1000).strftime('%d-%m-%Y %H:%M:%S.%f')
    file_path = pathlib.Path('/opt/ask_bid') / f'{symbol}.csv'
    if not file_path.parent.exists():
        os.mkdir(file_path.parent)
    header = ['symbol', 'timestamp', 'binance_ask', 'binance_bid', 'okx_ask', 'okx_bid']
    key = f'okx_ask_bid_{symbol}'
    connection = get_redis_connection('default')
    okx_last_data = connection.zrange(key, -1, -1)
    if okx_last_data:
        okx_last_data = json.loads(okx_last_data[0])
        okx_ask = str(okx_last_data['ask']).replace('.', ',')
        okx_bid = str(okx_last_data['bid']).replace('.', ',')
    else:
        okx_ask = 0
        okx_bid = 0
    with open(file_path, 'a', newline='') as file:
        writer = csv.writer(file, delimiter=';')
        if file.tell() == 0:
            writer.writerow(header)
        writer.writerow([symbol, timestamp, binance_ask, binance_bid, okx_ask, okx_bid])


def save_ask_bid_to_cache(data: dict) -> None:
    connection = get_redis_connection('default')
    pipeline = connection.pipeline()
    if 'instId' in data:
        exchange = 'okx'
        symbol = ''.join(data['instId'].split('-')[:2])
        ask = float(data['askPx'])
        bid = float(data['bidPx'])
        timestamp = int(data['ts'])
    else:
        exchange = 'binance'
        symbol = data['s']
        ask = float(data['a'])
        bid = float(data['b'])
        timestamp = int(data['E'])
    key = f'{exchange}_ask_bid_{symbol}'
    data = json.dumps(dict(symbol=symbol, ask=ask, bid=bid, timestamp=timestamp))
    # current_time = int(datetime.now().timestamp() * 1000)
    current_time = int(timestamp)
    one_minute_ago = current_time - 60000
    pipeline.execute_command('zadd', key, current_time, data)
    pipeline.execute_command('zremrangebyscore', key, 0, one_minute_ago)
    pipeline.execute()
