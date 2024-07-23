import csv
import json
import os
import logging
import pathlib
from datetime import datetime
from .helper import SavedOkxOrderId
from django_redis import get_redis_connection
from django.core.cache import cache


logger = logging.getLogger(__name__)


def save_okx_market_price_to_cache(data: dict) -> None:
    symbol = ''.join(data['instId'].split('-')[:-1])
    market_price = float(data['markPx'])
    cache.set(f'okx_market_price_{symbol}', market_price)


def save_filled_limit_order_id(data: dict) -> None:
    saved_orders_ids = SavedOkxOrderId(data['account_id'], data['instId'])
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


def write_ask_bid_to_csv_and_cache_by_symbol(data: dict) -> None:
    symbol = data['s']
    binance_ask = float(data['a'])
    binance_bid = float(data['b'])
    binance_ask_str = str(data['a']).replace('.', ',')
    binance_bid_str = str(data['b']).replace('.', ',')
    date_time = datetime.fromtimestamp(int(data['E']) / 1000).strftime('%d-%m-%Y %H:%M:%S.%f')[:-3]
    timestamp = int(data['E'])
    file_path = pathlib.Path('/opt/ask_bid') / f'{symbol}.csv'
    if not file_path.parent.exists():
        os.mkdir(file_path.parent)
    header = ['symbol', 'date', 'time', 'binance_ask', 'binance_bid', 'okx_ask', 'okx_bid']
    connection = get_redis_connection('default')
    pipeline = connection.pipeline()
    okx_last_data = connection.zrange(f'okx_ask_bid_{symbol}', -1, -1)
    if okx_last_data:
        okx_last_data = json.loads(okx_last_data[0])
        okx_ask = okx_last_data['ask']
        okx_bid = okx_last_data['bid']
        okx_ask_str = str(okx_last_data['ask']).replace('.', ',')
        okx_bid_str = str(okx_last_data['bid']).replace('.', ',')
    else:
        okx_ask = 0
        okx_bid = 0
        okx_ask_str = 0
        okx_bid_str = 0
    data = json.dumps(dict(
        symbol=symbol, binance_ask=binance_ask, binance_bid=binance_bid,
        okx_ask=okx_ask, okx_bid=okx_bid, timestamp=timestamp, date_time=date_time
    ))
    key = f'binance_okx_ask_bid_{symbol}'
    one_minute_ago = timestamp - 60000
    pipeline.execute_command('zadd', key, timestamp, data)
    pipeline.execute_command('zremrangebyscore', key, 0, one_minute_ago)
    pipeline.execute()
    with open(file_path, 'a', newline='') as file:
        writer = csv.writer(file, delimiter=';')
        if file.tell() == 0:
            writer.writerow(header)
        date = date_time.split(' ')[0]
        time = date_time.split(' ')[1]
        writer.writerow([symbol, date, time, binance_ask_str, binance_bid_str, okx_ask_str, okx_bid_str])


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
    # current_time = int(datetime.now().timestamp() * 1000)
    current_time = int(timestamp)
    date_time = datetime.fromtimestamp(timestamp / 1000).strftime('%d-%m-%Y %H:%M:%S.%f')[:-3]
    key = f'{exchange}_ask_bid_{symbol}'
    data = json.dumps(dict(symbol=symbol, ask=ask, bid=bid, timestamp=timestamp, date_time=date_time))
    one_minute_ago = current_time - 10000
    pipeline.execute_command('zadd', key, current_time, data)
    pipeline.execute_command('zremrangebyscore', key, 0, one_minute_ago)
    pipeline.execute()
