import logging
from django.utils import timezone
from types import SimpleNamespace as Namespace
from .models import Strategy, Symbol, Position
from .helper import calc, SavedOkxOrderId
from .trade import (
    OkxTrade, OkxEmulateTrade, get_ask_bid_prices_and_condition,
    get_take_profit_grid, get_stop_loss_breakeven
)


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
                    f'Funding rate {funding_rate:.5f} is unfavorable for the current position. Skip',
                    extra=strategy.extra_log
                )
                return False
            else:
                logger.warning(
                    f'Funding rate {funding_rate:.5f} is favorable for the {position_side} position. Open',
                    extra=strategy.extra_log
                )
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
            strategy, position.entry_price, prices['spread_percent'], position.side
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


def time_close_position(strategy: Strategy, position: Position) -> bool:
    if strategy.time_to_close:
        tz = timezone.get_current_timezone()
        open_time = timezone.datetime.strptime(
            position.position_data['cTime'], '%d-%m-%Y %H:%M:%S.%f').astimezone(tz)
        close_time = open_time + timezone.timedelta(seconds=strategy.time_to_close)
        seconds_to_close = (close_time - timezone.localtime()).total_seconds()
        logger.debug(
            f'Time to close {round(seconds_to_close//60)} minutes {round(seconds_to_close%60)} seconds',
            extra=strategy.extra_log
        )
        if seconds_to_close <= 0:
            logger.warning(f'Close time {close_time} reached', extra=strategy.extra_log)
            return True
    return False


def open_trade_position(strategy: Strategy, symbol: Symbol, position_side: str, prices: dict) -> None:
    if not check_funding_time(strategy, symbol, position_side):
        return
    logger.info(
        f'Opening {position_side} position, size {strategy.position_size} usdt',
        extra=strategy.extra_log
    )
    _, _, prices_entry = get_ask_bid_prices_and_condition(strategy, symbol, prices)
    trade = OkxTrade(strategy, symbol, position_side)
    position = trade.open_position()
    position = fill_position_data(strategy, position, prices, prices_entry)
    sl_tp_data = Namespace(**position.sl_tp_data)
    if strategy.close_position_parts:
        if strategy.close_position_type == 'limit':
            order_id = trade.place_limit_order(sl_tp_data.tp_first_price, sl_tp_data.tp_first_part)
            position.sl_tp_data['tp_first_limit_order_id'] = order_id
            logger.debug(f'Save limit {order_id=} for first part', extra=strategy.extra_log)
            order_id = trade.place_limit_order(sl_tp_data.tp_second_price, sl_tp_data.tp_second_part)
            position.sl_tp_data['tp_second_limit_order_id'] = order_id
            logger.debug(f'Save limit {order_id=} for second part', extra=strategy.extra_log)
            position.save(update_fields=['sl_tp_data'])
    else:
        if strategy.target_profit:
            if strategy.close_position_type == 'market':
                trade.update_take_profit(sl_tp_data.take_profit_price)
            if strategy.close_position_type == 'limit':
                trade.place_limit_order(sl_tp_data.take_profit_price, strategy.position_size)
    if strategy.stop_loss:
        trade.update_stop_loss(price=sl_tp_data.stop_loss_price, sz=position.position_data['pos'])


def watch_increase_position(strategy: Strategy, position: Position, condition_met: bool, prices: dict) -> None:
    if not position.sl_tp_data['increased_position'] and condition_met and position.sl_tp_data['stop_loss_breakeven_set']:
        trade = OkxTrade(strategy, position.symbol, position.side)
        trade.increase_position()
        position.sl_tp_data['increased_position'] = True
        position_data = trade.get_position()
        entry_price = position_data['avgPx']
        take_profit_grid = get_take_profit_grid(
            strategy, entry_price, prices['spread_percent'], position.side
        )
        position.sl_tp_data['tp_third_price'] = take_profit_grid['tp_first_price']
        position.sl_tp_data['tp_third_part'] = take_profit_grid['tp_first_part']
        position.sl_tp_data['tp_fourth_price'] = take_profit_grid['tp_second_price']
        position.sl_tp_data['tp_fourth_part'] = take_profit_grid['tp_second_part']
        order_id = trade.place_limit_order(take_profit_grid['tp_first_price'], take_profit_grid['tp_first_part'])
        position.sl_tp_data['tp_third_limit_order_id'] = order_id
        logger.debug(f'Save limit {order_id=} for third part', extra=strategy.extra_log)
        order_id = trade.place_limit_order(take_profit_grid['tp_second_price'], take_profit_grid['tp_second_part'])
        position.sl_tp_data['tp_fourth_limit_order_id'] = order_id
        logger.debug(f'Save limit {order_id=} for fourth part', extra=strategy.extra_log)
        position.save(update_fields=['sl_tp_data'])


def watch_trade_position(strategy: Strategy, position: Position) -> None:
    logger.debug('Watching position state', extra=strategy.extra_log)
    sl_tp_data = Namespace(**position.sl_tp_data)
    market_price = position.symbol.okx.market_price
    trade = OkxTrade(strategy, position.symbol, position.side)
    if time_close_position(strategy, position):
        trade.close_entire_position()
        return
    if not strategy.close_position_parts:
        return
    if strategy.close_position_type == 'limit':
        saved_orders_ids = SavedOkxOrderId(strategy.second_account.id, position.symbol.okx.inst_id)
        if not sl_tp_data.first_part_closed:
            if sl_tp_data.tp_first_limit_order_id in saved_orders_ids.get_orders():
                logger.info(
                    f'First take profit limit order {sl_tp_data.tp_first_limit_order_id} is filled',
                    extra=strategy.extra_log
                )
                position.sl_tp_data['first_part_closed'] = True
                if strategy.stop_loss_breakeven and not sl_tp_data.stop_loss_breakeven_set:
                    trade.update_stop_loss(
                        price=sl_tp_data.stop_loss_breakeven, sz=position.position_data['pos']
                    )
                    logger.info(
                        f'Updated stop loss to breakeven {sl_tp_data.stop_loss_breakeven}',
                        extra=strategy.extra_log
                    )
                    position.sl_tp_data['stop_loss_breakeven_set'] = True
                position.save(update_fields=['sl_tp_data'])
        elif not sl_tp_data.second_part_closed:
            if sl_tp_data.tp_second_limit_order_id in saved_orders_ids.get_orders():
                logger.info(
                    f'Second take profit limit order {sl_tp_data.tp_second_limit_order_id} is filled',
                    extra=strategy.extra_log
                )
                position.sl_tp_data['second_part_closed'] = True
                position.save(update_fields=['sl_tp_data'])
        elif not sl_tp_data.third_part_closed:
            if sl_tp_data.tp_third_limit_order_id in saved_orders_ids.get_orders():
                logger.info(
                    f'Third take profit limit order {sl_tp_data.tp_third_limit_order_id} is filled',
                    extra=strategy.extra_log
                )
                position.sl_tp_data['third_part_closed'] = True
                position.save(update_fields=['sl_tp_data'])
        elif not sl_tp_data.fourth_part_closed:
            if sl_tp_data.tp_fourth_limit_order_id in saved_orders_ids.get_orders():
                logger.info(
                    f'Fourth take profit limit order {sl_tp_data.tp_fourth_limit_order_id} is filled',
                    extra=strategy.extra_log
                )
                position.sl_tp_data['fourth_part_closed'] = True
                position.save(update_fields=['sl_tp_data'])
    elif strategy.close_position_type == 'market':
        if not sl_tp_data.first_part_closed:
            if ((position.side == 'long' and market_price >= sl_tp_data.tp_first_price) or
                (position.side == 'short' and market_price <= sl_tp_data.tp_first_price)):
                logger.info(
                    f'First take profit price {sl_tp_data.tp_first_price} reached {market_price=}',
                    extra=strategy.extra_log
                )
                trade.close_position(sl_tp_data.tp_first_part, position.symbol.okx, position.side)
                position.sl_tp_data['first_part_closed'] = True
                position.save(update_fields=['sl_tp_data'])
                logger.info(
                    f'First part {sl_tp_data.tp_first_part} of position is closed',
                    extra=strategy.extra_log
                )
                if strategy.stop_loss_breakeven and not sl_tp_data.stop_loss_breakeven_set:
                    trade.update_stop_loss(
                        price=sl_tp_data.stop_loss_breakeven, sz=position.position_data['pos']
                    )
                    logger.info(
                        f'Updated stop loss to breakeven {sl_tp_data.stop_loss_breakeven}',
                        extra=strategy.extra_log
                    )
                    position.sl_tp_data['stop_loss_breakeven_set'] = True
                    position.save(update_fields=['sl_tp_data'])
        elif not sl_tp_data.second_part_closed:
            if ((position.side == 'long' and market_price >= sl_tp_data.tp_second_price) or
                (position.side == 'short' and market_price <= sl_tp_data.tp_second_price)):
                logger.info(
                    f'Second take profit price {sl_tp_data.tp_second_price} reached {market_price=}',
                    extra=strategy.extra_log
                )
                trade.close_position(sl_tp_data.tp_second_part, position.symbol.okx, position.side)
                logger.info('Second part of position is closed', extra=strategy.extra_log)
                position.sl_tp_data['second_part_closed'] = True
                position.save(update_fields=['sl_tp_data'])
        elif not sl_tp_data.third_part_closed:
            if ((position.side == 'long' and market_price >= sl_tp_data.tp_third_price) or
                (position.side == 'short' and market_price <= sl_tp_data.tp_third_price)):
                logger.info(
                    f'Third take profit price {sl_tp_data.tp_third_price} reached {market_price=}',
                    extra=strategy.extra_log
                )
                trade.close_position(sl_tp_data.tp_third_part, position.symbol.okx, position.side)
                logger.info('Third part of position is closed', extra=strategy.extra_log)
                position.sl_tp_data['third_part_closed'] = True
                position.save(update_fields=['sl_tp_data'])
        elif not sl_tp_data.fourth_part_closed:
            if ((position.side == 'long' and market_price >= sl_tp_data.tp_fourth_price) or
                (position.side == 'short' and market_price <= sl_tp_data.tp_fourth_price)):
                logger.info(
                    f'Fourth take profit price {sl_tp_data.tp_fourth_price} reached {market_price=}',
                    extra=strategy.extra_log
                )
                trade.close_position(sl_tp_data.tp_fourth_part, position.symbol.okx, position.side)
                logger.info('Fourth part of position is closed', extra=strategy.extra_log)
                position.sl_tp_data['fourth_part_closed'] = True
                position.save(update_fields=['sl_tp_data'])


def open_emulate_position(strategy: Strategy, symbol: Symbol, position_side: str, prices: dict) -> None:
    if not check_funding_time(strategy, symbol, position_side):
        return
    trade = OkxEmulateTrade(strategy, symbol)
    position = trade.create_position(position_side)
    fill_position_data(strategy, position, prices, prices)


def watch_emulate_position(strategy: Strategy, position: Position) -> None:
    logger.debug('Virtual position is open', extra=strategy.extra_log)
    position_data = Namespace(**position.position_data)
    sl_tp_data = Namespace(**position.sl_tp_data)
    trade = OkxEmulateTrade(strategy, position.symbol)
    if time_close_position(strategy, position_data):
        trade.close_position(position, strategy.position_size)
        return
    market_price = position.symbol.okx.market_price
    if strategy.stop_loss:
        if ((position.side == 'long' and market_price <= sl_tp_data.stop_loss_price) or
            (position.side == 'short' and market_price >= sl_tp_data.stop_loss_price)):
            logger.info(
                f'Stop loss price {sl_tp_data.stop_loss_price} reached {market_price=}',
                extra=strategy.extra_log
            )
            trade.close_position(position, strategy.position_size)
            return
    if strategy.close_position_parts:
        if not sl_tp_data.first_part_closed:
            if ((position.side == 'long' and market_price >= sl_tp_data.tp_first_price) or
                (position.side == 'short' and market_price <= sl_tp_data.tp_first_price)):
                logger.info(
                    f'First take profit price {sl_tp_data.tp_first_price} reached {market_price=}',
                    extra=strategy.extra_log
                )
                trade.close_position(position, sl_tp_data.tp_first_part)
                position.sl_tp_data['first_part_closed'] = True
                position.save(update_fields=['sl_tp_data'])
                logger.info('First part of position is closed', extra=strategy.extra_log)
                return
        else:
            if ((position.side == 'long' and market_price >= sl_tp_data.tp_second_price) or
                (position.side == 'short' and market_price <= sl_tp_data.tp_second_price)):
                logger.info(
                    f'Second take profit price {sl_tp_data.tp_second_price} reached {market_price=}',
                    extra=strategy.extra_log
                )
                trade.close_position(position, sl_tp_data.tp_second_part)
                position.sl_tp_data['second_part_closed'] = True
                position.save(update_fields=['sl_tp_data'])
                logger.info('Second part of position is closed', extra=strategy.extra_log)
                return
    else:
        if strategy.target_profit:
            if ((position.side == 'long' and market_price >= sl_tp_data.take_profit_price) or
                (position.side == 'short' and market_price <= sl_tp_data.take_profit_price)):
                logger.info(
                    f'Take profit price {sl_tp_data.take_profit_price} reached {market_price=}',
                    extra=strategy.extra_log
                )
                trade.close_position(position, strategy.position_size)
                return
