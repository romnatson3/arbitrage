from math import floor
from django_redis import get_redis_connection
from django.core.cache import cache
from .exceptions import AcquireLockException
from .models import OkxSymbol


class Calculator():
    def get_sz(self, quote_coin: float, symbol: OkxSymbol) -> float:
        qty = floor(quote_coin / symbol.market_price / symbol.ct_val / symbol.lot_sz) * symbol.lot_sz
        return round(qty, 1)

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
            take_profit_price = price * (1 + (percentage + 2 * fee_percent + spread_percent) / 100)
        if position_side == 'short':
            take_profit_price = price * (1 - (percentage + 2 * fee_percent + spread_percent) / 100)
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


class CachePrice():
    def __init__(self, exchange: str) -> None:
        self.conection = get_redis_connection('default')
        self.pipeline = self.conection.pipeline()
        self.exchange = exchange

    def push_ask(self, symbol: str, value: float) -> bool:
        key = f'{self.exchange}_price_ask_{symbol}'
        self.pipeline.execute_command('lpush', key, value)
        self.pipeline.execute_command('ltrim', key, 0, 1)
        result = self.pipeline.execute()
        return all(result)

    def push_bid(self, symbol: str, value: float) -> bool:
        key = f'{self.exchange}_price_bid_{symbol}'
        self.pipeline.execute_command('lpush', key, value)
        self.pipeline.execute_command('ltrim', key, 0, 1)
        result = self.pipeline.execute()
        return all(result)

    def _get_ask(self, symbol: str) -> list[float]:
        key = f'{self.exchange}_price_ask_{symbol}'
        prices = self.conection.lrange(key, 0, -1)
        prices = [float(i) for i in prices][::-1]
        if len(prices) == 0:
            return [0.0, 0.0]
        elif len(prices) == 1:
            return [0.0, prices[0]]
        return prices

    def _get_bid(self, symbol: str) -> list[float]:
        key = f'{self.exchange}_price_bid_{symbol}'
        prices = self.conection.lrange(key, 0, -1)
        prices = [float(i) for i in prices][::-1]
        if len(prices) == 0:
            return [0.0, 0.0]
        elif len(prices) == 1:
            return [0.0, prices[0]]
        return prices

    def get_ask_previous_price(self, symbol):
        return self._get_ask(symbol)[0]

    def get_bid_previous_price(self, symbol):
        return self._get_bid(symbol)[0]

    def get_ask_last_price(self, symbol):
        return self._get_ask(symbol)[1]

    def get_bid_last_price(self, symbol):
        return self._get_bid(symbol)[1]


class CacheOkxOrderId():
    def __init__(self, account_id: int, inst_id: str) -> None:
        self.conection = get_redis_connection('default')
        self.pipeline = self.conection.pipeline()
        self.key = f'orders_{inst_id}_{account_id}'

    def add(self, order_id: str) -> bool:
        self.pipeline.execute_command('lpush', self.key, order_id)
        self.pipeline.execute_command('ltrim', self.key, 0, 100)
        result = self.pipeline.execute()
        return all(result)

    def remove(self, order_id: str) -> bool:
        return self.conection.lrem(self.key, 0, order_id) == 1

    def get_orders(self) -> list[str]:
        l = self.conection.lrange(self.key, 0, -1)
        return [i.decode() for i in l][::-1]
