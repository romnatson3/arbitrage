import logging
from django.core.cache import cache
from django.utils import timezone
from types import SimpleNamespace as Namespace
from .models import Strategy, Symbol, Position
from .helper import calc
from .trade import OkxTrade, OkxEmulateTrade, get_take_profit_grid, get_stop_loss_breakeven
from .helper import calculation_delta_and_points_for_entry, TaskLock
from .exceptions import PlaceOrderException


logger = logging.getLogger(__name__)


def check_funding_time(strategy: Strategy, symbol: Symbol, position_side: str) -> bool:
    funding_time = symbol.okx.funding_time
    edge_time = funding_time - timezone.timedelta(minutes=strategy.time_to_funding)
    if timezone.localtime() > edge_time:
        logger.warning(f'Funding time {funding_time} is too close', extra=strategy.extra_log)
        if not strategy.only_profit:
            logger.warning('Only profit mode is disabled. Skip', extra=strategy.extra_log)
            return False
        else:
            funding_rate = symbol.okx.funding_rate
            if (funding_rate > 0 and position_side == 'long') or (funding_rate < 0 and position_side == 'short'):
                logger.warning(
                    f'Funding rate {funding_rate:.5f} is unfavorable for the current position. Position will not open',
                    extra=strategy.extra_log
                )
                return False
            else:
                logger.warning(
                    f'Funding rate {funding_rate:.5f} is favorable for the {position_side} position. Position will open',
                    extra=strategy.extra_log
                )
    return True


def time_close_position(strategy: Strategy, position: Position) -> bool:
    if strategy.time_to_close:
        tz = timezone.get_current_timezone()
        open_time = timezone.datetime.strptime(
            position.position_data['cTime'], '%d-%m-%Y %H:%M:%S.%f').astimezone(tz)
        close_time = open_time + timezone.timedelta(seconds=strategy.time_to_close)
        seconds_to_close = (close_time - timezone.localtime()).total_seconds()
        if seconds_to_close > 0:
            logger.debug(
                f'Time to close {round(seconds_to_close//60)} minutes {round(seconds_to_close%60)} seconds',
                extra=strategy.extra_log
            )
            return False
        else:
            logger.warning(f'Close time {close_time} reached', extra=strategy.extra_log)
            return True


def fill_position_data(strategy: Strategy, position: Position, prices: dict, prices_entry: dict) -> Position:
    position.ask_bid_data.update(
        binance_previous_ask=prices['binance_previous_ask'],
        binance_last_ask=prices['binance_last_ask'],
        binance_previous_bid=prices['binance_previous_bid'],
        binance_last_bid=prices['binance_last_bid'],
        okx_previous_ask=prices['okx_previous_ask'],
        okx_last_ask=prices['okx_last_ask'],
        okx_previous_bid=prices['okx_previous_bid'],
        okx_last_bid=prices['okx_last_bid'],
        spread_points=prices['spread_points'],
        spread_percent=prices['spread_percent'],
        delta_points=prices['delta_points'],
        delta_percent=prices['delta_percent'],
        target_delta=prices['target_delta'],
        date_time_last_prices=prices['date_time_last_prices']
    )
    if strategy.close_position_parts:
        take_profit_grid = get_take_profit_grid(
            position, position.entry_price, prices['spread_percent'], position.side
        )
        position.sl_tp_data.update(take_profit_grid)
    if strategy.target_profit:
        position.sl_tp_data['take_profit_price'] = calc.get_take_profit_price(
            position.entry_price, strategy.target_profit, strategy.open_plus_close_fee,
            prices['spread_percent'], position.side
        )
    if strategy.stop_loss:
        position.sl_tp_data['stop_loss_price'] = calc.get_stop_loss_price(
            position.entry_price, strategy.stop_loss, position.side
        )
    if strategy.stop_loss_breakeven:
        position.sl_tp_data['stop_loss_breakeven'] = get_stop_loss_breakeven(
            position.entry_price, strategy.open_plus_close_fee,
            prices['spread_percent'], position.side
        )
    position.ask_bid_data.update(
        binance_last_ask_entry=prices_entry['binance_last_ask'],
        binance_last_bid_entry=prices_entry['binance_last_bid'],
        okx_last_ask_entry=prices_entry['okx_last_ask'],
        okx_last_bid_entry=prices_entry['okx_last_bid'],
        spread_points_entry=prices_entry['spread_points'],
        spread_percent_entry=prices_entry['spread_percent'],
        delta_points_entry=prices_entry['delta_points'],
        delta_percent_entry=prices_entry['delta_percent']
    )
    logger.info('Filled position data', extra=strategy.extra_log)
    position.save(update_fields=['ask_bid_data', 'sl_tp_data'])
    return position


def get_pre_enter_data(strategy: Strategy, symbol: Symbol, position_side: str, prices: dict) -> tuple:
    if position_side == 'long':
        sz = prices['okx_last_ask_size']
        price = prices['okx_last_ask']
    elif position_side == 'short':
        sz = prices['okx_last_bid_size']
        price = prices['okx_last_bid']
    logger.info(f'Get pre-enter data {sz=} {price=}', extra=strategy.extra_log)
    sz_from_admin = calc.get_sz(symbol.okx, strategy.position_size, price)
    if sz < sz_from_admin:
        size_contract = sz
        size_usdt = calc.get_usdt_from_sz(symbol.okx, sz, price)
        logger.warning(
            f'Not enough liquidity, open position for {size_contract=}, {size_usdt=}',
            extra=strategy.extra_log
        )
    else:
        size_contract = sz_from_admin
        size_usdt = strategy.position_size
        logger.info(
            f'Liquidity is enough, open position for {size_contract=} {size_usdt=}',
            extra=strategy.extra_log
        )
    return price, size_contract, size_usdt


def open_emulate_position(strategy: Strategy, symbol: Symbol, position_side: str, prices: dict) -> None:
    if not check_funding_time(strategy, symbol, position_side):
        return
    trade = OkxEmulateTrade(strategy, symbol)
    open_price, size_contract, size_usdt = get_pre_enter_data(strategy, symbol, position_side, prices)
    position = trade.create_position(
        open_price, size_contract, size_usdt, position_side, prices['date_time_last_prices']
    )
    fill_position_data(strategy, position, prices, prices)


def open_trade_position(strategy: Strategy, symbol: Symbol, position_side: str, prices: dict) -> None:
    if not check_funding_time(strategy, symbol, position_side):
        TaskLock(f'open_or_increase_position_{strategy.id}_{symbol}').release()
        return
    _, size_contract, size_usdt = get_pre_enter_data(strategy, symbol, position_side, prices)
    trade = OkxTrade(strategy, symbol, size_contract, position_side)
    trade.open_position()


def increase_trade_position(strategy: Strategy, position: Position, prices: dict) -> None:
    if not position.sl_tp_data['increased_position'] and position.sl_tp_data['stop_loss_breakeven_order_id']:
        _, size_contract, size_usdt = get_pre_enter_data(strategy, position.symbol, position.side, prices)
        position.sl_tp_data['increased_position'] = True
        position.save(update_fields=['sl_tp_data'])
        trade = OkxTrade(strategy, position.symbol, size_contract, position.side)
        trade.open_position(increase=True)
    else:
        logger.warning('Not all conditions are met to increase the position', extra=strategy.extra_log)
        TaskLock(f'open_or_increase_position_{strategy.id}_{position.symbol}').release()


def place_orders_after_open_trade_position(position: Position) -> None:
    try:
        position.strategy._extra_log.update(symbol=position.symbol.symbol, position=position.id)
        logger.info('Placing orders after opening position', extra=position.strategy.extra_log)
        strategy = position.strategy
        symbol = position.symbol
        prices = cache.get(f'ask_bid_prices_{symbol}')
        if not prices:
            logger.error('Prices not found in cache', extra=strategy.extra_log)
            return
        cache.delete(f'ask_bid_prices_{symbol}')
        prices_entry = calculation_delta_and_points_for_entry(symbol, position.side, prices)
        position = fill_position_data(strategy, position, prices, prices_entry)
        sl_tp_data = Namespace(**position.sl_tp_data)
        trade = OkxTrade(strategy, symbol, position.sz, position.side)
        if strategy.stop_loss:
            order_id = trade.place_stop_loss(price=sl_tp_data.stop_loss_price, sz=position.sz)
            position.sl_tp_data['stop_loss_order_id'] = int(order_id)
            position.save(update_fields=['sl_tp_data'])
        if strategy.close_position_parts:
            if strategy.close_position_type == 'limit':
                order_id = trade.place_limit_order(sl_tp_data.tp_first_price, sl_tp_data.tp_first_part)
                position.sl_tp_data['tp_first_limit_order_id'] = int(order_id)
                position.save(update_fields=['sl_tp_data'])
                logger.debug(f'Save limit {order_id=} for first part', extra=strategy.extra_log)
                order_id = trade.place_limit_order(
                    sl_tp_data.tp_second_price, position.sz - sl_tp_data.tp_first_part
                )
                position.sl_tp_data['tp_second_limit_order_id'] = int(order_id)
                position.save(update_fields=['sl_tp_data'])
                logger.debug(f'Save limit {order_id=} for second part', extra=strategy.extra_log)
        else:
            if strategy.target_profit:
                if strategy.close_position_type == 'market':
                    trade.update_take_profit(sl_tp_data.take_profit_price)
                if strategy.close_position_type == 'limit':
                    trade.place_limit_order(sl_tp_data.take_profit_price, position.sz)
    except PlaceOrderException as e:
        logger.error(e, extra=strategy.extra_log)
        trade.close_entire_position()
    finally:
        TaskLock(f'open_or_increase_position_{strategy.id}_{symbol}').release()


def calc_tp_and_place_orders_after_increase_trade_position(position: Position) -> None:
    try:
        strategy = position.strategy
        symbol = position.symbol
        position.strategy._extra_log.update(symbol=symbol, position=position.id)
        logger.info(
            'Calculating take profit grid after increasing position',
            extra=strategy.extra_log
        )
        entry_price = position.entry_price
        prices = cache.get(f'ask_bid_prices_{symbol}')
        if not prices:
            logger.error('Prices not found in cache', extra=strategy.extra_log)
            return
        cache.delete(f'ask_bid_prices_{symbol}')
        take_profit_grid = get_take_profit_grid(
            position, entry_price, prices['spread_percent'], position.side
        )
        trade = OkxTrade(strategy, symbol, position.sz, position.side)
        position.sl_tp_data['tp_third_price'] = take_profit_grid['tp_first_price']
        position.sl_tp_data['tp_third_part'] = take_profit_grid['tp_first_part']
        position.sl_tp_data['tp_fourth_price'] = take_profit_grid['tp_second_price']
        position.sl_tp_data['tp_fourth_part'] = take_profit_grid['tp_second_part']
        position.save(update_fields=['sl_tp_data'])
        if strategy.close_position_type == 'limit':
            logger.info(
                'Placing limits orders after increasing position',
                extra=position.strategy.extra_log
            )
            order_id = trade.place_limit_order(
                take_profit_grid['tp_first_price'], take_profit_grid['tp_first_part']
            )
            position.sl_tp_data['tp_third_limit_order_id'] = int(order_id)
            logger.debug(f'Save limit {order_id=} for third part', extra=strategy.extra_log)
            order_id = trade.place_limit_order(
                take_profit_grid['tp_second_price'],
                position.sz - take_profit_grid['tp_first_part']
            )
            position.sl_tp_data['tp_fourth_limit_order_id'] = int(order_id)
            logger.debug(f'Save limit {order_id=} for fourth part', extra=strategy.extra_log)
            position.save(update_fields=['sl_tp_data'])
    except PlaceOrderException as e:
        logger.error(e, extra=strategy.extra_log)
        trade.close_entire_position()
    finally:
        TaskLock(f'open_or_increase_position_{strategy.id}_{symbol}').release()
