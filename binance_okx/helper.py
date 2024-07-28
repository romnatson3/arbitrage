import logging
from math import floor
import json
from django_redis import get_redis_connection
from django.core.cache import cache
from .exceptions import AcquireLockException
from .models import OkxSymbol, Strategy, Symbol


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


def check_all_conditions(strategy: Strategy, symbol: Symbol) -> tuple[bool, str, dict]:
    prices = {}
    nothing = False, '', {}
    conection = get_redis_connection('default')
    records = conection.zrange(f'binance_okx_ask_bid_{symbol.symbol}', 0, -1)
    records = [json.loads(i) for i in records]
    if not records:
        logger.debug('No records found in cache', extra=strategy.extra_log)
        return nothing
    data = records.pop(-1)
    binance_last_ask = data['binance_ask']
    binance_last_bid = data['binance_bid']
    okx_last_ask = data['okx_ask']
    okx_last_bid = data['okx_bid']
    date_time_last_prices = data['date_time']
    prices.update(
        binance_last_ask=binance_last_ask, binance_last_bid=binance_last_bid,
        okx_last_ask=okx_last_ask, okx_last_bid=okx_last_bid,
        date_time_last_prices=date_time_last_prices
    )
    logger.debug(
        f'{date_time_last_prices=}, {binance_last_ask=}, {binance_last_bid=}, '
        f'{okx_last_ask=}, {okx_last_bid=}',
        extra=strategy.extra_log
    )
    edge_timestamp = data['timestamp'] - strategy.search_duration
    records = [i for i in records if i['timestamp'] >= edge_timestamp]
    if not records:
        logger.debug(
            f'Only one record found in cache during the last {strategy.search_duration} ms',
            extra=strategy.extra_log
        )
        return nothing
    for i in records:
        binance_previous_ask = i['binance_ask']
        binance_previous_bid = i['binance_bid']
        okx_previous_ask = i['okx_ask']
        okx_previous_bid = i['okx_bid']
        date_time_previous_prices = i['date_time']
        prices.update(
            binance_previous_ask=binance_previous_ask, binance_previous_bid=binance_previous_bid,
            okx_previous_ask=okx_previous_ask, okx_previous_bid=okx_previous_bid
        )
        if binance_last_bid > binance_previous_bid:
            position_side = 'long'
            logger.debug(
                f'First condition for long position is met {binance_last_bid=} > {binance_previous_bid=}',
                extra=strategy.extra_log
            )
            logger.debug(
                f'{date_time_previous_prices=}, {binance_previous_ask=}, {binance_previous_bid=}, '
                f'{okx_previous_ask=}, {okx_previous_bid=}',
                extra=strategy.extra_log
            )
            second_condition_met, ask_bid_data = check_second_condition(strategy, symbol, position_side, prices)
            if second_condition_met:
                return True, position_side, ask_bid_data
        if binance_last_ask < binance_previous_ask:
            position_side = 'short'
            logger.debug(
                f'First condition for short position is met {binance_last_ask=} < {binance_previous_ask=}',
                extra=strategy.extra_log
            )
            logger.debug(
                f'{date_time_previous_prices=}, {binance_previous_ask=}, {binance_previous_bid=}, '
                f'{okx_previous_ask=}, {okx_previous_bid=}',
                extra=strategy.extra_log
            )
            second_condition_met, ask_bid_data = check_second_condition(strategy, symbol, position_side, prices)
            if second_condition_met:
                return True, position_side, ask_bid_data
    else:
        logger.debug(
            f'Conditions is NOT met during the last {strategy.search_duration} ms',
            extra=strategy.extra_log
        )
    return nothing


def check_second_condition(
    strategy: Strategy, symbol: Symbol, position_side: str, prices: dict
) -> tuple[bool, dict]:
    binance_previous_ask = prices['binance_previous_ask']
    binance_previous_bid = prices['binance_previous_bid']
    binance_last_ask = prices['binance_last_ask']
    binance_last_bid = prices['binance_last_bid']
    okx_previous_ask = prices['okx_previous_ask']
    okx_previous_bid = prices['okx_previous_bid']
    okx_last_ask = prices['okx_last_ask']
    okx_last_bid = prices['okx_last_bid']
    if position_side == 'long':
        binance_delta_percent = (
            (binance_last_bid - binance_previous_bid) / binance_previous_bid * 100
        )
        okx_delta_percent = (
            (okx_last_ask - okx_previous_ask) / okx_previous_ask * 100
        )
    elif position_side == 'short':
        binance_delta_percent = (
            (binance_previous_ask - binance_last_ask) / binance_previous_ask * 100
        )
        okx_delta_percent = (
            (okx_previous_bid - okx_last_bid) / okx_previous_bid * 100
        )
    if okx_delta_percent >= 0:
        logger.debug(
            f'{position_side=}, {binance_delta_percent=:.5f}, {okx_delta_percent=:.5f}',
            extra=strategy.extra_log
        )
        spread_percent = (okx_last_ask - okx_last_bid) / okx_last_bid * 100
        target_profit = strategy.tp_first_price_percent if strategy.close_position_parts else strategy.target_profit
        min_delta_percent = strategy.open_plus_close_fee + spread_percent + target_profit
        delta_percent = binance_delta_percent - okx_delta_percent
        if delta_percent >= min_delta_percent:
            logger.info(
                f'Second condition for {position_side} position is met '
                f'{delta_percent=:.5f} >= {min_delta_percent=:.5f}',
                extra=strategy.extra_log
            )
            if position_side == 'long':
                delta_points_binance = (binance_last_bid - binance_previous_bid) / symbol.okx.tick_size
                delta_points_okx = (okx_last_ask - okx_previous_ask) / symbol.okx.tick_size
                delta_points = delta_points_binance - delta_points_okx
            elif position_side == 'short':
                delta_points_binance = (binance_previous_ask - binance_last_ask) / symbol.okx.tick_size
                delta_points_okx = (okx_previous_bid - okx_last_bid) / symbol.okx.tick_size
                delta_points = delta_points_binance - delta_points_okx
            spread_points = (okx_last_ask - okx_last_bid) / symbol.okx.tick_size
            prices.update(
                spread_points=spread_points, spread_percent=spread_percent,
                delta_points=delta_points, delta_percent=delta_percent,
                target_delta=min_delta_percent,
            )
            return True, prices
        else:
            logger.debug(
                f'Second condition for {position_side} position is NOT met '
                f'{delta_percent=:.5f} < {min_delta_percent=:.5f}',
                extra=strategy.extra_log
            )
            return False, {}
    else:
        logger.debug(
            f'Second condition for {position_side} position is NOT met, {okx_delta_percent=:.5f} < 0',
            extra=strategy.extra_log
        )
        return False, {}


def calculation_delta_and_points_for_entry(
    symbol: Symbol, position_side: str, previous_prices: dict
) -> dict:
    conection = get_redis_connection('default')
    last_prices = conection.zrange(f'binance_okx_ask_bid_{symbol.symbol}', -1, -1)[0]
    last_prices = json.loads(last_prices)
    binance_last_ask = last_prices['binance_ask']
    binance_last_bid = last_prices['binance_bid']
    okx_last_ask = last_prices['okx_ask']
    okx_last_bid = last_prices['okx_bid']
    binance_previous_ask = previous_prices['binance_previous_ask']
    binance_previous_bid = previous_prices['binance_previous_bid']
    okx_previous_ask = previous_prices['okx_previous_ask']
    okx_previous_bid = previous_prices['okx_previous_bid']
    if position_side == 'long':
        binance_delta_percent = (
            (binance_last_bid - binance_previous_bid) / binance_previous_bid * 100
        )
        okx_delta_percent = (
            (okx_last_ask - okx_previous_ask) / okx_previous_ask * 100
        )
        delta_points_binance = (binance_last_bid - binance_previous_bid) / symbol.okx.tick_size
        delta_points_okx = (okx_last_ask - okx_previous_ask) / symbol.okx.tick_size
        delta_points = delta_points_binance - delta_points_okx
    elif position_side == 'short':
        binance_delta_percent = (
            (binance_previous_ask - binance_last_ask) / binance_previous_ask * 100
        )
        okx_delta_percent = (
            (okx_previous_bid - okx_last_bid) / okx_previous_bid * 100
        )
        delta_points_binance = (binance_previous_ask - binance_last_ask) / symbol.okx.tick_size
        delta_points_okx = (okx_previous_bid - okx_last_bid) / symbol.okx.tick_size
        delta_points = delta_points_binance - delta_points_okx
    spread_points = (okx_last_ask - okx_last_bid) / symbol.okx.tick_size
    spread_percent = (okx_last_ask - okx_last_bid) / okx_last_bid * 100
    delta_percent = binance_delta_percent - okx_delta_percent
    return dict(
        binance_last_ask=binance_last_ask, binance_last_bid=binance_last_bid,
        okx_last_ask=okx_last_ask, okx_last_bid=okx_last_bid,
        spread_points=spread_points, spread_percent=spread_percent,
        delta_points=delta_points, delta_percent=delta_percent,
    )
