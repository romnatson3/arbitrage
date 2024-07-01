from math import floor
import json
from django_redis import get_redis_connection
from django.core.cache import cache
from .exceptions import AcquireLockException
from .models import OkxSymbol


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


class AskBidPrices():
    def __init__(self, exchange: str, symbol: str) -> None:
        self.conection = get_redis_connection('default')
        self.key = f'{exchange}_ask_bid_{symbol}'
        self.data = self._get_last_two_records()

    def _get_last_two_records(self) -> dict[str, float]:
        records = self.conection.zrange(self.key, -2, -1)
        if not records:
            return dict(previous_ask=0, previous_bid=0, last_ask=0, last_bid=0)
        if len(records) == 1:
            last = json.loads(records[0])
            return dict(previous_ask=0, previous_bid=0, last_ask=last['ask'], last_bid=last['bid'])
        elif len(records) == 2:
            previous, last = json.loads(records[0]), json.loads(records[1])
            return dict(previous_ask=previous['ask'], previous_bid=previous['bid'],
                        last_ask=last['ask'], last_bid=last['bid'])

    def get_previous_ask_price(self):
        return self.data['previous_ask']

    def get_previous_bid_price(self):
        return self.data['previous_bid']

    def get_last_ask_price(self):
        return self.data['last_ask']

    def get_last_bid_price(self):
        return self.data['last_bid']


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
