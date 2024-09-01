import logging
import time
from math import floor
import json
from decimal import Decimal, ROUND_DOWN
from typing import Optional, Any
import okx.Account
from django_redis import get_redis_connection
from django.core.cache import cache
from .exceptions import AcquireLockException
from .models import OkxSymbol, Strategy, Symbol, Account, Execution
from .misc import convert_dict_values
from .exceptions import GetBillsException


logger = logging.getLogger(__name__)
conection = get_redis_connection('default')


def round_by_lot_sz(value: float, lot_sz: str) -> float:
    # value = Decimal(value).quantize(Decimal(lot_sz), rounding=ROUND_DOWN)
    value = Decimal(value).quantize(Decimal(lot_sz))
    if len(lot_sz) == 1:
        return int(value)
    else:
        return float(value)


def price_to_string(price: float) -> str:
    return f'{price:.10f}'.rstrip('0').rstrip('.')


class Calculator():
    def get_sz(self, symbol: OkxSymbol, quote_coin: float, price: float = None) -> float | int:
        if not price:
            price = symbol.market_price
        size_contract = (quote_coin / price) / symbol.ct_val
        if size_contract < float(symbol.lot_sz):
            if len(symbol.lot_sz) == 1:
                return int(symbol.lot_sz)
            else:
                return float(symbol.lot_sz)
        sz = round_by_lot_sz(size_contract, symbol.lot_sz)
        return sz

    def get_base_coin_from_sz(self, sz: float, contract_value: float) -> float:
        base_coin = sz * contract_value
        return base_coin

    def get_usdt_from_sz(self, symbol: OkxSymbol, sz: float, price: float = None) -> float:
        if not price:
            price = symbol.market_price
        usdt = sz * symbol.market_price * symbol.ct_val
        return round(usdt, 2)

    def get_stop_loss_price(
        self, symbol: OkxSymbol, price: float, percentage: float,
        position_side: str
    ) -> float:
        if percentage <= 0:
            return 0
        if position_side == 'long':
            stop_loss_price = price - (price / 100 * percentage)
        if position_side == 'short':
            stop_loss_price = price + (price / 100 * percentage)
        # return round(stop_loss_price, 4)
        stop_loss_price = float(
            Decimal(stop_loss_price)
            .quantize(Decimal(str(symbol.tick_size)), rounding=ROUND_DOWN)
        )
        return stop_loss_price

    def get_take_profit_price(
        self,
        symbol: OkxSymbol,
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
        # return round(take_profit_price, 4)
        take_profit_price = float(
            Decimal(take_profit_price)
            .quantize(Decimal(str(symbol.tick_size)), rounding=ROUND_DOWN)
        )
        return take_profit_price


calc = Calculator()


class TaskLock():
    def __init__(self, key) -> None:
        self.key = key

    def acquire(self) -> bool:
        return cache.add(self.key, 1, timeout=15)

    def release(self) -> None:
        cache.delete(self.key)

    def __enter__(self):
        if not self.acquire():
            raise AcquireLockException('Failed to acquire lock')
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.release()


def check_all_conditions(strategy: Strategy, symbol: Symbol, max_score: int) -> tuple[bool, str, dict]:
    start_time = time.time()
    prices = {}
    nothing = False, '', {}
    min_score = max_score - strategy.search_duration
    records = conection.zrangebyscore(
        f'binance_okx_ask_bid_{symbol.symbol}', min_score, max_score
    )
    if not records:
        logger.debug(
            f'No cache entry found in the range {min_score=}, {max_score=}',
            extra=strategy.extra_log
        )
        return nothing
    records = [json.loads(i) for i in records]
    data = records.pop(-1)
    binance_last_ask = data['binance_ask_price']
    binance_last_bid = data['binance_bid_price']
    okx_last_ask = data['okx_ask_price']
    okx_last_ask_size = data['okx_ask_size']
    okx_last_bid = data['okx_bid_price']
    okx_last_bid_size = data['okx_bid_size']
    date_time_last_prices = data['date_time']
    prices.update(
        binance_last_ask=binance_last_ask, binance_last_bid=binance_last_bid,
        okx_last_ask=okx_last_ask, okx_last_ask_size=okx_last_ask_size,
        okx_last_bid=okx_last_bid, okx_last_bid_size=okx_last_bid_size,
        date_time_last_prices=date_time_last_prices
    )
    logger.trace(
        'Last record in cache: '
        f'{date_time_last_prices=}, {binance_last_ask=}, {binance_last_bid=}, '
        f'{okx_last_ask=}, {okx_last_bid=}',
        extra=strategy.extra_log
    )
    if not records:
        logger.debug(
            f'Only one cache entry found in the range {min_score=}, {max_score=}',
            extra=strategy.extra_log
        )
        return nothing
    counter = 0
    for i in records:
        counter += 1
        binance_previous_ask = i['binance_ask_price']
        binance_previous_bid = i['binance_bid_price']
        okx_previous_ask = i['okx_ask_price']
        okx_previous_bid = i['okx_bid_price']
        date_time_previous_prices = i['date_time']
        prices.update(
            binance_previous_ask=binance_previous_ask,
            binance_previous_bid=binance_previous_bid,
            okx_previous_ask=okx_previous_ask,
            okx_previous_bid=okx_previous_bid,
            date_time_previous_prices=date_time_previous_prices
        )
        if binance_last_bid > binance_previous_bid:
            position_side = 'long'
            logger.trace(
                f'Current iteration: {counter}. '
                'First condition for long position is met '
                f'{binance_last_bid=} > {binance_previous_bid=}, '
                f'{date_time_last_prices=}, {date_time_previous_prices=}',
                extra=strategy.extra_log
            )
            second_condition_met, ask_bid_data = (
                check_second_condition(
                    strategy, symbol, position_side, prices, counter, len(records)
                )
            )
            if second_condition_met:
                return True, position_side, ask_bid_data
        if binance_last_ask < binance_previous_ask:
            position_side = 'short'
            logger.trace(
                f'Current iteration: {counter}. '
                'First condition for short position is met '
                f'{binance_last_ask=} < {binance_previous_ask=}, '
                f'{date_time_last_prices=}, {date_time_previous_prices=}',
                extra=strategy.extra_log
            )
            second_condition_met, ask_bid_data = (
                check_second_condition(
                    strategy, symbol, position_side, prices, counter, len(records)
                )
            )
            if second_condition_met:
                return True, position_side, ask_bid_data
    execution_time = round((time.time() - start_time) * 1000)
    logger.debug(
        f'Conditions are NOT met, last price time: {date_time_last_prices}, '
        f'iterations: {counter}, execution time: {execution_time} ms',
        extra=strategy.extra_log
    )
    return nothing


def check_second_condition(
    strategy: Strategy, symbol: Symbol, position_side: str, prices: dict,
    counter: int, records_count: int
) -> tuple[bool, dict]:
    binance_previous_ask = prices['binance_previous_ask']
    binance_previous_bid = prices['binance_previous_bid']
    binance_last_ask = prices['binance_last_ask']
    binance_last_bid = prices['binance_last_bid']
    okx_previous_ask = prices['okx_previous_ask']
    okx_previous_bid = prices['okx_previous_bid']
    okx_last_ask = prices['okx_last_ask']
    okx_last_bid = prices['okx_last_bid']
    date_time_last_prices = prices['date_time_last_prices']
    date_time_previous_prices = prices['date_time_previous_prices']
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
        logger.trace(
            f'{okx_delta_percent=:.5f} >= 0, {position_side=}, '
            f'{binance_delta_percent=:.5f}, {okx_delta_percent=:.5f}',
            extra=strategy.extra_log
        )
        spread_percent = (okx_last_ask - okx_last_bid) / okx_last_bid * 100
        if strategy.close_position_parts:
            take_profit = strategy.tp_first_price_percent
        else:
            take_profit = strategy.take_profit
        min_delta_percent = strategy.open_plus_close_fee + spread_percent + take_profit
        delta_percent = binance_delta_percent - okx_delta_percent
        if delta_percent >= min_delta_percent:
            logger.debug(
                f'Second condition for {position_side} position is met, '
                f'iteration: {counter} of {records_count}, '
                f'{delta_percent=:.5f} >= {min_delta_percent=:.5f}',
                extra=strategy.extra_log
            )
            logger.debug(
                f'{date_time_previous_prices=}, {binance_previous_ask=}, {binance_previous_bid=}, '
                f'{okx_previous_ask=}, {okx_previous_bid=}, '
                f'{date_time_last_prices=}, {binance_last_ask=}, {binance_last_bid=}, '
                f'{okx_last_ask=}, {okx_last_bid=}',
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
                target_delta=min_delta_percent, position_side=position_side
            )
            return True, prices
        else:
            logger.trace(
                f'Second condition for {position_side} position is NOT met '
                f'{delta_percent=:.5f} < {min_delta_percent=:.5f}, '
                f'{date_time_last_prices=}, {date_time_previous_prices=}',
                extra=strategy.extra_log
            )
            return False, {}
    else:
        logger.trace(
            f'Second condition for {position_side} position is NOT met, '
            f'{okx_delta_percent=:.5f} < 0',
            extra=strategy.extra_log
        )
        return False, {}


def calculation_delta_and_points_for_entry(
    symbol: Symbol, position_side: str, previous_prices: dict
) -> dict:
    conection = get_redis_connection('default')
    last_prices = conection.zrange(f'binance_okx_ask_bid_{symbol.symbol}', -1, -1)[0]
    last_prices = json.loads(last_prices)
    binance_last_ask = last_prices['binance_ask_price']
    binance_last_bid = last_prices['binance_bid_price']
    okx_last_ask = last_prices['okx_ask_price']
    okx_last_bid = last_prices['okx_bid_price']
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


def get_bills(account: Account, bill_id: Optional[int] = None) -> list[dict[str, Any]]:
    client = okx.Account.AccountAPI(
        account.api_key, account.api_secret, account.api_passphrase,
        flag='1' if account.testnet else '0',
        debug=False
    )
    if not bill_id:
        result = client.get_account_bills(instType='SWAP', mgnMode='isolated', type=2)
    else:
        result = client.get_account_bills(
            instType='SWAP', mgnMode='isolated', type=2,
            before=bill_id
        )
    if result['code'] != '0':
        raise GetBillsException(result)
    bills = list(map(convert_dict_values, result['data']))
    for i in bills:
        i['subType'] = Execution.sub_type.get(i['subType'], i['subType'])
    return bills
