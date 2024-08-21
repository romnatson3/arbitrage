import csv
import json
import os
import logging
import pathlib
from datetime import datetime
from types import SimpleNamespace as Namespace
from django_redis import get_redis_connection
from django.core.cache import cache
from exchange.celery import app
from .models import Strategy
from .misc import convert_dict_values
from .trade import OkxTrade, OkxEmulateTrade
from .exceptions import AcquireLockException
from .helper import TaskLock


logger = logging.getLogger(__name__)


def save_okx_market_price_to_cache(data: dict) -> None:
    symbol = ''.join(data['instId'].split('-')[:-1])
    market_price = float(data['markPx'])
    cache.set(f'okx_market_price_{symbol}', market_price)


def write_last_price_to_csv(data: dict) -> None:
    symbol = ''.join(data['instId'].split('-')[:2])
    ask_price = float(data['askPx'])
    ask_size = float(data['askSz'])
    bid_price = float(data['bidPx'])
    bid_size = float(data['bidSz'])
    last_price = float(data['last'])
    last_size = float(data['lastSz'])
    previous_last_size = cache.get(f'okx_last_size_{symbol}')
    if previous_last_size:
        if last_size == previous_last_size:
            return
    cache.set(f'okx_last_size_{symbol}', last_size)
    timestamp = int(data['ts'])
    date_time = datetime.fromtimestamp(timestamp / 1000).strftime('%d-%m-%Y %H:%M:%S.%f')[:-3]
    ask_price_str = str(ask_price).replace('.', ',')
    ask_size_str = str(ask_size).replace('.', ',')
    bid_price_str = str(bid_price).replace('.', ',')
    bid_size_str = str(bid_size).replace('.', ',')
    last_price_str = str(last_price).replace('.', ',')
    last_size_str = str(last_size).replace('.', ',')
    file_path = pathlib.Path('/opt/ask_bid') / f'LAST_SZ_{symbol}.csv'
    if not file_path.parent.exists():
        os.mkdir(file_path.parent)
    header = [
        'symbol', 'date', 'time', 'okx_ask_price', 'okx_ask_size', 'okx_bid_price',
        'okx_bid_size', 'okx_last_price', 'okx_last_size'
    ]
    with open(file_path, 'a', newline='') as file:
        writer = csv.writer(file, delimiter=';')
        if file.tell() == 0:
            writer.writerow(header)
        date = date_time.split(' ')[0]
        time = date_time.split(' ')[1]
        writer.writerow([
            symbol, date, time, ask_price_str, ask_size_str, bid_price_str,
            bid_size_str, last_price_str, last_size_str
        ])


def write_ask_bid_to_csv_and_cache_by_symbol(data: dict) -> None:
    symbol = data['s']
    binance_ask_price = float(data['a'])
    binance_ask_size = float(data['A'])
    binance_bid_price = float(data['b'])
    binance_bid_size = float(data['B'])
    binance_ask_price_str = str(binance_ask_price).replace('.', ',')
    binance_ask_size_str = str(binance_ask_size).replace('.', ',')
    binance_bid_price_str = str(binance_bid_price).replace('.', ',')
    binance_bid_size_str = str(binance_bid_size).replace('.', ',')
    timestamp = int(data['E'])
    date_time = datetime.fromtimestamp(timestamp / 1000).strftime('%d-%m-%Y %H:%M:%S.%f')[:-3]
    file_path = pathlib.Path('/opt/ask_bid') / f'{symbol}.csv'
    if not file_path.parent.exists():
        os.mkdir(file_path.parent)
    header = ['symbol', 'date', 'time', 'binance_ask_price', 'binance_bid_price', 'okx_ask_price',
              'okx_ask_size', 'okx_bid_price', 'okx_bid_size']
    connection = get_redis_connection('default')
    pipeline = connection.pipeline()
    okx_last_data = connection.zrange(f'okx_ask_bid_{symbol}', -1, -1)
    if okx_last_data:
        okx_last_data = json.loads(okx_last_data[0])
        okx_ask_price = okx_last_data['ask_price']
        okx_ask_size = okx_last_data['ask_size']
        okx_bid_price = okx_last_data['bid_price']
        okx_bid_size = okx_last_data['bid_size']
        okx_ask_price_str = str(okx_ask_price).replace('.', ',')
        okx_ask_size_str = str(okx_ask_size).replace('.', ',')
        okx_bid_price_str = str(okx_bid_price).replace('.', ',')
        okx_bid_size_str = str(okx_bid_size).replace('.', ',')
    else:
        okx_ask_price = 0
        okx_ask_size = 0
        okx_bid_price = 0
        okx_bid_size = 0
        okx_ask_price_str = '0'
        okx_ask_size_str = '0'
        okx_bid_price_str = '0'
        okx_bid_size_str = '0'
    data = json.dumps(dict(
        symbol=symbol, binance_ask_price=binance_ask_price, binance_bid_price=binance_bid_price,
        okx_ask_price=okx_ask_price, okx_ask_size=okx_ask_size, okx_bid_price=okx_bid_price,
        okx_bid_size=okx_bid_size, timestamp=timestamp, date_time=date_time
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
        writer.writerow([
            symbol, date, time, binance_ask_price_str, binance_bid_price_str, okx_ask_price_str,
            okx_ask_size_str, okx_bid_price_str, okx_bid_size_str
        ])


def save_okx_ask_bid_to_cache(data: dict) -> None:
    connection = get_redis_connection('default')
    pipeline = connection.pipeline()
    exchange = 'okx'
    symbol = ''.join(data['instId'].split('-')[:2])
    ask_price = float(data['askPx'])
    ask_size = float(data['askSz'])
    bid_price = float(data['bidPx'])
    bid_size = float(data['bidSz'])
    timestamp = int(data['ts'])
    key = f'{exchange}_ask_bid_{symbol}'
    data = json.dumps(dict(
        symbol=symbol,
        ask_price=ask_price,
        ask_size=ask_size,
        bid_price=bid_price,
        bid_size=bid_size,
        timestamp=timestamp,
    ))
    one_minute_ago = timestamp - 10000
    pipeline.execute_command('zadd', key, timestamp, data)
    pipeline.execute_command('zremrangebyscore', key, 0, one_minute_ago)
    pipeline.execute()


@app.task
def orders_handler(data: dict) -> None:
    try:
        data = convert_dict_values(data)
        data = Namespace(**data)
        symbol: str = ''.join(data.instId.split('-')[:-1])
        try:
            strategy = Strategy.objects.get(enabled=True, mode='trade', second_account_id=data.account_id, symbols__symbol=symbol)
        except Strategy.DoesNotExist:
            logger.error(f'Not found enabled strategy for {symbol=} and {data.account_id=}', extra=dict(symbol=symbol))
            return
        strategy._extra_log.update(symbol=symbol)
        logger.debug(
            f'orderId={data.ordId}, algoId={data.algoId}, ordType={data.ordType}, '
            f'state={data.state}, side={data.side}, posSide={data.posSide}, '
            f'avgPx={data.avgPx}, sz={data.sz}, notionalUsd={data.notionalUsd:.5f}, '
            f'tradeId={data.tradeId}, '
            f'fillSz={data.fillSz}, fillPx={data.fillPx}, '
            f'fillPnl={data.fillPnl}, fillTime={data.fillTime}',
            extra=strategy.extra_log
        )
        positions = sorted(
            filter(lambda x: x.symbol.symbol == symbol and x.is_open is True, strategy.positions.all()),
            key=lambda x: x.id, reverse=True
        )
        if not positions:
            logger.debug('No open position found. Stop processing order', extra=strategy.extra_log)
            return
        position = positions[0]
        strategy._extra_log.update(position=position.id)
        sl_tp_data = Namespace(**position.sl_tp_data)
        if data.state != 'filled':
            logger.debug(f'Order {data.ordId} is not filled. Stop processing order', extra=strategy.extra_log)
            return
        if data.ordType == 'limit':
            if strategy.close_position_type == 'limit':
                if sl_tp_data.tp_first_limit_order_id == data.ordId:
                    logger.info(
                        f'First take profit limit order {sl_tp_data.tp_first_limit_order_id} is filled',
                        extra=strategy.extra_log
                    )
                    position.sl_tp_data['first_part_closed'] = True
                    position.save(update_fields=['sl_tp_data'])
                    logger.info(f'First part {sl_tp_data.tp_first_part} of position is closed', extra=strategy.extra_log)
                    if strategy.stop_loss_breakeven and not sl_tp_data.stop_loss_breakeven_order_id:
                        trade = OkxTrade(strategy, position.symbol, position.size_usdt, position.side)
                        order_id = trade.update_stop_loss(
                            price=sl_tp_data.stop_loss_breakeven, sz=position.sz
                        )
                        logger.info(
                            f'Updated stop loss to breakeven {sl_tp_data.stop_loss_breakeven}, {order_id=}',
                            extra=strategy.extra_log
                        )
                        position.sl_tp_data['stop_loss_breakeven_order_id'] = order_id
                    position.save(update_fields=['sl_tp_data'])
                elif sl_tp_data.tp_second_limit_order_id == data.ordId:
                    logger.info(
                        f'Second take profit limit order {sl_tp_data.tp_second_limit_order_id} is filled',
                        extra=strategy.extra_log
                    )
                    position.sl_tp_data['second_part_closed'] = True
                    position.save(update_fields=['sl_tp_data'])
                    logger.info(f'Second part {sl_tp_data.tp_second_part} of position is closed', extra=strategy.extra_log)
                elif sl_tp_data.tp_third_limit_order_id == data.ordId:
                    logger.info(
                        f'Third take profit limit order {sl_tp_data.tp_third_limit_order_id} is filled',
                        extra=strategy.extra_log
                    )
                    position.sl_tp_data['third_part_closed'] = True
                    position.save(update_fields=['sl_tp_data'])
                    logger.info(f'Third part {sl_tp_data.tp_third_part} of position is closed', extra=strategy.extra_log)
                elif sl_tp_data.tp_fourth_limit_order_id == data.ordId:
                    logger.info(
                        f'Fourth take profit limit order {sl_tp_data.tp_fourth_limit_order_id} is filled',
                        extra=strategy.extra_log
                    )
                    position.sl_tp_data['fourth_part_closed'] = True
                    position.save(update_fields=['sl_tp_data'])
                    logger.info(f'Fourth part {sl_tp_data.tp_fourth_part} of position is closed', extra=strategy.extra_log)
                else:
                    logger.error(f'Order {data.ordId} is not found in sl_tp_data', extra=strategy.extra_log)
        if data.ordType == 'market' and data.algoId:
            if sl_tp_data.stop_loss_order_id == data.algoId:
                logger.info(f'Stop loss market order {data.algoId} is filled', extra=strategy.extra_log)
            if sl_tp_data.stop_loss_breakeven_order_id == data.algoId:
                logger.info(f'Stop loss breakeven market order {data.algoId} is filled', extra=strategy.extra_log)
    except Exception as e:
        logger.exception(e)
        raise e


def check_at_market_price(data: dict) -> None:
    try:
        symbol = ''.join(data['instId'].split('-')[:-1])
        market_price = float(data['markPx'])
        date_time = data['date_time']
        strategies = Strategy.objects.cache(symbols__symbol=symbol, enabled=True)
        for strategy in strategies:
            if strategy.mode == Strategy.Mode.trade:
                if strategy.close_position_type == 'market' and strategy.close_position_parts:
                    check_trade_position_at_market_price.delay(strategy.id, symbol, market_price)
            elif strategy.mode == Strategy.Mode.emulate:
                check_emulate_position_at_market_price.delay(strategy.id, symbol, market_price, date_time)
    except Exception as e:
        logger.exception(e)


@app.task
def check_trade_position_at_market_price(strategy_id: int, symbol: str, market_price: float) -> None:
    try:
        strategy = Strategy.objects.cache(id=strategy_id)[0]
        strategy._extra_log.update(symbol=symbol)
        with TaskLock(f'check_trade_position_at_market_price_{strategy_id}_{symbol}'):
            position = strategy.get_last_trade_open_position(symbol)
            if position and any(position.sl_tp_data.values()):
                sl_tp_data = Namespace(**position.sl_tp_data)
                strategy = position.strategy
                strategy._extra_log.update(position=position.id)
                trade = OkxTrade(strategy, position.symbol, position.size_usdt, position.side)
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
                        logger.info(f'First part {sl_tp_data.tp_first_part} of position is closed', extra=strategy.extra_log)
                        if strategy.stop_loss_breakeven and not sl_tp_data.stop_loss_breakeven_order_id:
                            order_id = trade.update_stop_loss(
                                price=sl_tp_data.stop_loss_breakeven, sz=position.sz
                            )
                            logger.info(
                                f'Updated stop loss to breakeven {sl_tp_data.stop_loss_breakeven}, {order_id=}',
                                extra=strategy.extra_log
                            )
                            position.sl_tp_data['stop_loss_breakeven_order_id'] = order_id
                            position.save(update_fields=['sl_tp_data'])
                elif not sl_tp_data.second_part_closed:
                    if ((position.side == 'long' and market_price >= sl_tp_data.tp_second_price) or
                        (position.side == 'short' and market_price <= sl_tp_data.tp_second_price)):
                        logger.info(
                            f'Second take profit price {sl_tp_data.tp_second_price} reached {market_price=}',
                            extra=strategy.extra_log
                        )
                        trade.close_position(sl_tp_data.tp_second_part, position.symbol.okx, position.side)
                        logger.info(f'Second part {sl_tp_data.tp_second_part} of position is closed', extra=strategy.extra_log)
                        position.sl_tp_data['second_part_closed'] = True
                        position.save(update_fields=['sl_tp_data'])
                if sl_tp_data.increased_position:
                    if not sl_tp_data.third_part_closed:
                        if ((position.side == 'long' and market_price >= sl_tp_data.tp_third_price) or
                            (position.side == 'short' and market_price <= sl_tp_data.tp_third_price)):
                            logger.info(
                                f'Third take profit price {sl_tp_data.tp_third_price} reached {market_price=}',
                                extra=strategy.extra_log
                            )
                            trade.close_position(sl_tp_data.tp_third_part, position.symbol.okx, position.side)
                            logger.info(f'Third part {sl_tp_data.tp_third_part} of position is closed', extra=strategy.extra_log)
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
                            logger.info(f'Fourth part {sl_tp_data.tp_fourth_part} of position is closed', extra=strategy.extra_log)
                            position.sl_tp_data['fourth_part_closed'] = True
                            position.save(update_fields=['sl_tp_data'])
    except AcquireLockException:
        logger.debug('Task check_trade_position_at_market_price is still running', extra=strategy.extra_log)
    except Exception as e:
        logger.exception(e, extra=strategy.extra_log)
        raise e


@app.task
def check_emulate_position_at_market_price(strategy_id: int, symbol: str, market_price: float, date_time: str) -> None:
    try:
        strategy = Strategy.objects.cache(id=strategy_id)[0]
        strategy._extra_log.update(symbol=symbol)
        position = strategy.get_last_emulate_open_position(symbol)
        if position and any(position.sl_tp_data.values()):
            strategy = position.strategy
            strategy._extra_log.update(position=position.id)
            trade = OkxEmulateTrade(strategy, position.symbol)
            sl_tp_data = Namespace(**position.sl_tp_data)
            if strategy.stop_loss:
                if ((position.side == 'long' and market_price <= sl_tp_data.stop_loss_price) or
                    (position.side == 'short' and market_price >= sl_tp_data.stop_loss_price)):
                    logger.info(
                        f'Stop loss price {sl_tp_data.stop_loss_price} reached {market_price=}',
                        extra=strategy.extra_log
                    )
                    trade.close_position(position, date_time=date_time, completely=True)
                    return
            if strategy.close_position_parts:
                if not sl_tp_data.first_part_closed:
                    if ((position.side == 'long' and market_price >= sl_tp_data.tp_first_price) or
                        (position.side == 'short' and market_price <= sl_tp_data.tp_first_price)):
                        logger.info(
                            f'First take profit price {sl_tp_data.tp_first_price} reached {market_price=}',
                            extra=strategy.extra_log
                        )
                        trade.close_position(position, sl_tp_data.tp_first_part, date_time=date_time)
                        position.sl_tp_data['first_part_closed'] = True
                        position.save(update_fields=['sl_tp_data'])
                        logger.info('First part of position is closed', extra=strategy.extra_log)
                        return
                elif not sl_tp_data.second_part_closed:
                    if ((position.side == 'long' and market_price >= sl_tp_data.tp_second_price) or
                        (position.side == 'short' and market_price <= sl_tp_data.tp_second_price)):
                        logger.info(
                            f'Second take profit price {sl_tp_data.tp_second_price} reached {market_price=}',
                            extra=strategy.extra_log
                        )
                        trade.close_position(position, date_time=date_time, completely=True)
                        position.sl_tp_data['second_part_closed'] = True
                        position.save(update_fields=['sl_tp_data'])
                        logger.info('Second part of position is closed', extra=strategy.extra_log)
                        return
                else:
                    logger.warning('All parts of position are closed', extra=strategy.extra_log)
            else:
                if strategy.target_profit:
                    if ((position.side == 'long' and market_price >= sl_tp_data.take_profit_price) or
                        (position.side == 'short' and market_price <= sl_tp_data.take_profit_price)):
                        logger.info(
                            f'Take profit price {sl_tp_data.take_profit_price} reached {market_price=}',
                            extra=strategy.extra_log
                        )
                        trade.close_position(position, date_time=date_time, completely=True)
                        return
    except Exception as e:
        logger.exception(e, extra=strategy.extra_log)
        raise e
