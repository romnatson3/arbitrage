import logging
from math import floor
import json
from datetime import datetime
from django_redis import get_redis_connection
from django.core.cache import cache
from .exceptions import AcquireLockException
from .models import OkxSymbol, Strategy


logger = logging.getLogger(__name__)


class Calculator():
    def get_sz(self, quote_coin: float, symbol: OkxSymbol) -> float:
        contract_count = (quote_coin / symbol.market_price) / symbol.ct_val
        if contract_count < symbol.lot_sz:
            return symbol.lot_sz
        sz = floor(contract_count / symbol.lot_sz) * symbol.lot_sz
        return round(sz, 1)

    def get_base_coin_from_sz(self, sz: float, contract_value: float) -> float:
        base_coin = sz * contract_value
        return base_coin

    def get_stop_loss_price(self, price: float, percentage: float, position_side: str) -> float:
        if percentage <= 0:
            return 0
        if position_side == 'long':
            stop_loss_price = price - (price / 100 * percentage)
        if position_side == 'short':
            stop_loss_price = price + (price / 100 * percentage)
        return round(stop_loss_price, 5)

    def get_take_profit_price(
        self,
        price: float,
        percentage: float,
        fee_percent: float,
        spread_percent: float,
        position_side: str
    ) -> float:
        if percentage <= 0:
            return 0
        if position_side == 'long':
            take_profit_price = price * (1 + (percentage + fee_percent + spread_percent) / 100)
        if position_side == 'short':
            take_profit_price = price * (1 - (percentage + fee_percent + spread_percent) / 100)
        return round(take_profit_price, 5)


calc = Calculator()


class TaskLock():
    def __init__(self, key) -> None:
        self.key = key

    def acquire(self) -> bool:
        return cache.add(self.key, 1, timeout=60)

    def release(self) -> None:
        cache.delete(self.key)

    def __enter__(self):
        if not self.acquire():
            raise AcquireLockException('Failed to acquire lock')
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.release()


def get_ask_bid_prices_from_cache_by_symbol(strategy: Strategy, symbol: str) -> dict[str, float]:
    prices = dict(
        binance_previous_ask=0, binance_previous_bid=0,
        binance_last_ask=0, binance_last_bid=0,
        okx_previous_ask=0, okx_previous_bid=0,
        okx_last_ask=0, okx_last_bid=0, position_side=None
    )
    conection = get_redis_connection('default')
    records = conection.zrange(f'binance_okx_ask_bid_{symbol}', 0, -1, 'REV')
    records = [json.loads(i) for i in records]
    if not records:
        logger.debug('No records found in cache', extra=strategy.extra_log)
        return prices
    data = records.pop(0)
    prices.update(
        binance_last_ask=data['binance_ask'],
        binance_last_bid=data['binance_bid'],
        okx_last_ask=data['okx_ask'],
        okx_last_bid=data['okx_bid']
    )
    if not records:
        logger.debug('Only one record found in cache', extra=strategy.extra_log)
        return prices
    edge_timestamp = data['timestamp'] - strategy.search_duration
    records = [i for i in records if i['timestamp'] >= edge_timestamp]
    if not records:
        logger.debug(
            f'No records found in cache for the last {strategy.search_duration} ms',
            extra=strategy.extra_log
        )
        return prices
    for item in records:
        if prices['binance_last_bid'] > item['binance_bid']:
            prices['position_side'] = 'long'
            break
        elif prices['binance_last_ask'] < item['binance_ask']:
            prices['position_side'] = 'short'
            break
    else:
        logger.debug(
            f'First condition not met for last {strategy.search_duration} ms',
            extra=strategy.extra_log
        )
        return prices
    prices.update(
        binance_previous_ask=item['binance_ask'],
        binance_previous_bid=item['binance_bid'],
        okx_previous_ask=item['okx_ask'],
        okx_previous_bid=item['okx_bid']
    )
    return prices


class SavedOkxOrderId():
    def __init__(self, account_id: int, inst_id: str) -> None:
        self.key = f'okx_orders_{inst_id}_{account_id}'
        self.conection = get_redis_connection('default')
        self.pipeline = self.conection.pipeline()

    def add(self, order_id: str) -> bool:
        self.pipeline.execute_command('lpush', self.key, order_id)
        self.pipeline.execute_command('ltrim', self.key, 0, 100)
        result = self.pipeline.execute()
        return all(result)

    def get_orders(self) -> set[str]:
        return {i.decode() for i in self.conection.lrange(self.key, 0, -1)}
