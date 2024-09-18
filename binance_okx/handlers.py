import csv
import json
import os
import logging
import pathlib
from datetime import datetime
from types import SimpleNamespace as Namespace
from django_redis import get_redis_connection
from django.core.cache import cache
from django.conf import settings
from exchange.celery import app
from .models import Strategy
from .misc import convert_dict_values
from .trade import OkxTrade, OkxEmulateTrade
from .exceptions import AcquireLockException
from .helper import TaskLock


logger = logging.getLogger(__name__)
connection = get_redis_connection('default')


def save_okx_market_price_to_cache(data: dict) -> None:
    symbol = ''.join(data['instId'].split('-')[:-1])
    market_price = float(data['markPx'])
    cache.set(f'okx_market_price_{symbol}', market_price)


def write_ask_bid_to_csv_and_cache_by_symbol(data: dict) -> None:
    # connection = get_redis_connection('default')
    pipeline = connection.pipeline()
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
    if cache.get('write_ask_bid_to_csv', False):
        file_path = pathlib.Path(settings.CSV_PATH) / f'{symbol}.csv'
        if not file_path.parent.exists():
            os.mkdir(file_path.parent)
        header = ['symbol', 'date', 'time', 'binance_ask_price', 'binance_bid_price', 'okx_ask_price',
                  'okx_ask_size', 'okx_bid_price', 'okx_bid_size']
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
    # connection = get_redis_connection('default')
    pipeline = connection.pipeline()
    symbol = ''.join(data['instId'].split('-')[:2])
    ask_price = float(data['askPx'])
    ask_size = float(data['askSz'])
    bid_price = float(data['bidPx'])
    bid_size = float(data['bidSz'])
    timestamp = int(data['ts'])
    key = f'okx_ask_bid_{symbol}'
    data = json.dumps(dict(
        symbol=symbol,
        ask_price=ask_price,
        ask_size=ask_size,
        bid_price=bid_price,
        bid_size=bid_size,
        timestamp=timestamp,
    ))
    one_minute_ago = timestamp - 60000
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
            strategy = Strategy.objects.get(
                enabled=True, mode='trade', second_account_id=data.account_id,
                symbols__symbol=symbol
            )
        except Strategy.DoesNotExist:
            logger.error(
                f'Not found enabled strategy for {symbol=} and {data.account_id=}',
                extra=dict(symbol=symbol)
            )
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
            filter(
                lambda x: x.symbol.symbol == symbol and x.is_open is True,
                strategy.positions.all()
            ),
            key=lambda x: x.id, reverse=True
        )
        if not positions:
            logger.debug('No open position found. Stop processing order',
                         extra=strategy.extra_log)
            return
        position = positions[0]
        strategy._extra_log.update(position=position.id)
        sl_tp_data = Namespace(**position.sl_tp_data)
        if data.state != 'filled':
            logger.debug(f'Order {data.ordId} is not filled. Stop processing order',
                         extra=strategy.extra_log)
            return
        if data.ordType == 'limit':
            if strategy.close_position_type == 'limit':
                if sl_tp_data.tp_first_limit_order_id == data.ordId:
                    logger.info(
                        'First take profit limit order '
                        f'{sl_tp_data.tp_first_limit_order_id} is filled, '
                        f'tradeId={data.tradeId}',
                        extra=strategy.extra_log
                    )
                    position.sl_tp_data['first_part_closed'] = True
                    position.trade_ids.append(data.tradeId)
                    position.save(update_fields=['sl_tp_data', 'trade_ids'])
                    logger.info(
                        f'First part sz={sl_tp_data.tp_first_part} of position is closed',
                        extra=strategy.extra_log
                    )
                    if strategy.stop_loss_breakeven and not sl_tp_data.stop_loss_breakeven_order_id:
                        trade = OkxTrade(strategy, position.symbol, position.sz, position.side)
                        order_id = trade.update_stop_loss(
                            price=sl_tp_data.stop_loss_breakeven, sz=position.sz
                        )
                        logger.info(
                            'Updated stop loss to breakeven '
                            f'{sl_tp_data.stop_loss_breakeven}, {order_id=}',
                            extra=strategy.extra_log
                        )
                        position.sl_tp_data['stop_loss_breakeven_order_id'] = order_id
                    position.save(update_fields=['sl_tp_data'])
                elif sl_tp_data.tp_second_limit_order_id == data.ordId:
                    logger.info(
                        'Second take profit limit order '
                        f'{sl_tp_data.tp_second_limit_order_id} is filled, '
                        f'tradeId={data.tradeId}',
                        extra=strategy.extra_log
                    )
                    position.sl_tp_data['second_part_closed'] = True
                    position.trade_ids.append(data.tradeId)
                    position.save(update_fields=['sl_tp_data', 'trade_ids'])
                    logger.info(
                        f'Second part sz={sl_tp_data.tp_second_part} of position is closed',
                        extra=strategy.extra_log
                    )
                elif sl_tp_data.tp_third_limit_order_id == data.ordId:
                    logger.info(
                        'Third take profit limit order '
                        f'{sl_tp_data.tp_third_limit_order_id} is filled, '
                        f'tradeId={data.tradeId}',
                        extra=strategy.extra_log
                    )
                    position.sl_tp_data['third_part_closed'] = True
                    position.trade_ids.append(data.tradeId)
                    position.save(update_fields=['sl_tp_data', 'trade_ids'])
                    logger.info(
                        f'Third part sz={sl_tp_data.tp_third_part} of position is closed',
                        extra=strategy.extra_log
                    )
                elif sl_tp_data.tp_fourth_limit_order_id == data.ordId:
                    logger.info(
                        f'Fourth take profit limit order '
                        f'{sl_tp_data.tp_fourth_limit_order_id} is filled, '
                        f'tradeId={data.tradeId}',
                        extra=strategy.extra_log
                    )
                    position.sl_tp_data['fourth_part_closed'] = True
                    position.trade_ids.append(data.tradeId)
                    position.save(update_fields=['sl_tp_data', 'trade_ids'])
                    logger.info(
                        f'Fourth part sz={sl_tp_data.tp_fourth_part} of position is closed',
                        extra=strategy.extra_log
                    )
                else:
                    logger.error(
                        f'Order {data.ordId} is not found in sl_tp_data',
                        extra=strategy.extra_log
                    )
        if data.ordType == 'market' and data.algoId:
            if sl_tp_data.stop_loss_order_id == data.algoId:
                logger.warning(
                    f'Stop loss market order {data.algoId} is filled, tradeId={data.tradeId}',
                    extra=strategy.extra_log
                )
                position.trade_ids.append(data.tradeId)
                position.save(update_fields=['trade_ids'])
            if sl_tp_data.stop_loss_breakeven_order_id == data.algoId:
                logger.warning(
                    f'Stop loss breakeven market order {data.algoId} is filled, '
                    f'tradeId={data.tradeId}',
                    extra=strategy.extra_log
                )
                position.trade_ids.append(data.tradeId)
                position.save(update_fields=['trade_ids'])
    except Exception as e:
        logger.exception(e)
        raise e


def closing_position_by_market(data: dict) -> None:
    try:
        symbol = ''.join(data['instId'].split('-')[:-1])
        ask_price = float(data['askPx'])
        bid_price = float(data['bidPx'])
        timestamp = int(data['ts'])
        date_time = datetime.fromtimestamp(timestamp / 1000).strftime('%d-%m-%Y %H:%M:%S.%f')[:-3]
        strategies = Strategy.objects.cache(symbols__symbol=symbol, enabled=True)
        for strategy in strategies:
            if strategy.mode == Strategy.Mode.trade:
                if strategy.close_position_type == 'market':
                    if strategy.close_position_parts:
                        closing_trade_position_market_parts.delay(
                            strategy.id, symbol, ask_price, bid_price
                        )
            elif strategy.mode == Strategy.Mode.emulate:
                if strategy.stop_loss or strategy.stop_loss_breakeven:
                    closing_emulate_position_market_stop_loss.delay(
                        strategy.id, symbol, ask_price, bid_price, date_time
                    )
                if strategy.close_position_type == 'market':
                    if strategy.close_position_parts:
                        closing_emulate_position_market_parts.delay(
                            strategy.id, symbol, ask_price, bid_price, date_time
                        )
                    else:
                        if strategy.take_profit:
                            closing_emulate_position_market_take_profit.delay(
                                strategy.id, symbol, ask_price, bid_price, date_time
                            )
    except Exception as e:
        logger.exception(e)
        raise e


@app.task
def closing_position_by_limit(data: dict) -> None:
    try:
        symbol = ''.join(data['instId'].split('-')[:-1])
        timestamp = int(data['ts'])
        date_time = datetime.fromtimestamp(timestamp / 1000).strftime('%d-%m-%Y %H:%M:%S.%f')[:-3]
        last_price = float(data['last'])
        last_size = float(data['lastSz'])
        strategies = Strategy.objects.cache(symbols__symbol=symbol, enabled=True)
        for strategy in strategies:
            if strategy.mode == Strategy.Mode.emulate and strategy.close_position_type == 'limit':
                closing_emulate_position_by_limit.delay(
                    strategy.id, symbol, last_price, last_size, date_time
                )
    except Exception as e:
        logger.exception(e)
        raise e


@app.task
def closing_emulate_position_by_limit(
        strategy_id: int, symbol: str, last_price: float, last_size: float, date_time: str
) -> None:
    try:
        strategy = Strategy.objects.cache(id=strategy_id)[0]
        strategy._extra_log.update(symbol=symbol)
        with TaskLock(f'closing_emulate_position_by_limit_{strategy_id}_{symbol}'):
            position = strategy.get_last_emulate_open_position(symbol)
            if position and any(position.sl_tp_data.values()):
                strategy = position.strategy
                strategy._extra_log.update(position=position.id)
                trade = OkxEmulateTrade(strategy, position.symbol)
                sl_tp_data = Namespace(**position.sl_tp_data)
                bid_price = position.symbol.okx.bid_price
                ask_price = position.symbol.okx.ask_price
                if strategy.close_position_parts:
                    if not sl_tp_data.first_part_closed:
                        if last_price == sl_tp_data.tp_first_price:
                            logger.debug(
                                f'Last price {last_price} is equal to take '
                                f'profit first price {sl_tp_data.tp_first_price}',
                                extra=strategy.extra_log
                            )
                            key = f'emulate_position_total_last_size_{position.id}_{symbol}'
                            total_last_size = cache.get(key, 0)
                            total_last_size = round(total_last_size + last_size, 2)
                            if total_last_size >= sl_tp_data.tp_first_part:
                                cache.delete(key)
                                logger.info(
                                    f'Total last size {total_last_size} >= '
                                    f'first part size {sl_tp_data.tp_first_part}',
                                    extra=strategy.extra_log
                                )
                                position.sl_tp_data['first_part_closed'] = True
                                trade.close_position(
                                    position, sl_tp_data.tp_first_price,
                                    sl_tp_data.tp_first_part, date_time=date_time
                                )
                                return
                            else:
                                cache.set(key, total_last_size)
                                logger.info(
                                    f'Total last size {total_last_size} < first '
                                    f'part size {sl_tp_data.tp_first_part}',
                                    extra=strategy.extra_log
                                )
                        logger.debug(
                            f'Last price {last_price} is not equal to '
                            f'take profit first price {sl_tp_data.tp_first_price}',
                            extra=strategy.extra_log
                        )
                        if position.side == 'long' and bid_price >= sl_tp_data.tp_first_price:
                            logger.info(
                                f'Bid price {bid_price} >= limit order first '
                                f'take profit price {sl_tp_data.tp_first_price}',
                                extra=strategy.extra_log
                            )
                            position.sl_tp_data['first_part_closed'] = True
                            trade.close_position(
                                position, sl_tp_data.tp_first_price,
                                sl_tp_data.tp_first_part, date_time=date_time
                            )
                        elif position.side == 'short' and ask_price <= sl_tp_data.tp_first_price:
                            logger.info(
                                f'Ask price {ask_price} <= limit order first '
                                f'take profit price {sl_tp_data.tp_first_price}',
                                extra=strategy.extra_log
                            )
                            position.sl_tp_data['first_part_closed'] = True
                            trade.close_position(
                                position, sl_tp_data.tp_first_price,
                                sl_tp_data.tp_first_part, date_time=date_time
                            )
                        else:
                            logger.debug(
                                'Limit order first take profit price '
                                f'{sl_tp_data.tp_first_price} not reached '
                                f'{bid_price=} {ask_price=}',
                                extra=strategy.extra_log
                            )
                        if position.sl_tp_data['first_part_closed']:
                            if strategy.stop_loss_breakeven:
                                logger.info(
                                    'Update stop loss to breakeven '
                                    f'{sl_tp_data.stop_loss_breakeven}',
                                    extra=strategy.extra_log
                                )
                                position.sl_tp_data['stop_loss_breakeven_order_id'] = True
                                position.save(update_fields=['sl_tp_data'])
                    elif not sl_tp_data.second_part_closed:
                        if last_price == sl_tp_data.tp_second_price:
                            logger.debug(
                                f'Last price {last_price} is equal to take '
                                f'profit second price {sl_tp_data.tp_second_price}',
                                extra=strategy.extra_log
                            )
                            key = f'emulate_position_total_last_size_{position.id}_{symbol}'
                            total_last_size = cache.get(key, 0)
                            total_last_size += last_size
                            if total_last_size >= sl_tp_data.tp_second_part:
                                cache.delete(key)
                                logger.info(
                                    f'Total last size {total_last_size} >='
                                    f'second part size {sl_tp_data.tp_second_part}',
                                    extra=strategy.extra_log
                                )
                                position.sl_tp_data['second_part_closed'] = True
                                trade.close_position(
                                    position, sl_tp_data.tp_second_price,
                                    position.sz, date_time=date_time
                                )
                                return
                            else:
                                cache.set(key, total_last_size)
                                logger.info(
                                    f'Total last size {total_last_size} < second '
                                    f'part size {sl_tp_data.tp_second_part}',
                                    extra=strategy.extra_log
                                )
                        logger.debug(
                            f'Last price {last_price} is not equal to take profit '
                            f'second price {sl_tp_data.tp_second_price}',
                            extra=strategy.extra_log
                        )
                        if position.side == 'long' and bid_price >= sl_tp_data.tp_second_price:
                            logger.info(
                                f'Bid price {bid_price} >= limit order second '
                                f'take profit price {sl_tp_data.tp_second_price}',
                                extra=strategy.extra_log
                            )
                            position.sl_tp_data['second_part_closed'] = True
                            trade.close_position(
                                position, sl_tp_data.tp_second_price,
                                position.sz, date_time=date_time
                            )
                        elif position.side == 'short' and ask_price <= sl_tp_data.tp_second_price:
                            logger.info(
                                f'Ask price {ask_price} <= limit order second '
                                f'take profit price {sl_tp_data.tp_second_price}',
                                extra=strategy.extra_log
                            )
                            position.sl_tp_data['second_part_closed'] = True
                            trade.close_position(
                                position, sl_tp_data.tp_second_price,
                                position.sz, date_time=date_time
                            )
                        else:
                            logger.debug(
                                f'Second take profit price {sl_tp_data.tp_second_price} '
                                f'not reached {bid_price=} {ask_price=}',
                                extra=strategy.extra_log
                            )
                elif strategy.take_profit:
                    if last_price == sl_tp_data.take_profit_price:
                        logger.debug(
                            f'Last price {last_price} is equal to take profit price '
                            f'{sl_tp_data.take_profit_price}',
                            extra=strategy.extra_log
                        )
                        key = f'emulate_position_total_last_size_{position.id}_{symbol}'
                        total_last_size = cache.get(key, 0)
                        total_last_size += last_size
                        if total_last_size >= position.sz:
                            cache.delete(key)
                            logger.info(
                                f'Total last size {total_last_size} >= '
                                f'position size {position.sz}',
                                extra=strategy.extra_log
                            )
                            trade.close_position(
                                position, sl_tp_data.take_profit_price,
                                position.sz, date_time=date_time
                            )
                            return
                        else:
                            cache.set(key, total_last_size)
                            logger.info(
                                f'Total last size {total_last_size} < position size ',
                                f'{position.sz}',
                                extra=strategy.extra_log
                            )
                    logger.debug(
                        f'Last price {last_price} is not equal to take profit '
                        f'price {sl_tp_data.take_profit_price}',
                        extra=strategy.extra_log
                    )
                    if position.side == 'long' and bid_price >= sl_tp_data.take_profit_price:
                        logger.info(
                            f'Bid price {bid_price} >= take profit price '
                            f'{sl_tp_data.take_profit_price}',
                            extra=strategy.extra_log
                        )
                        trade.close_position(
                            position, sl_tp_data.take_profit_price,
                            position.sz, date_time=date_time
                        )
                    elif position.side == 'short' and ask_price <= sl_tp_data.take_profit_price:
                        logger.info(
                            f'Ask price {ask_price} <= take profit price '
                            f'{sl_tp_data.take_profit_price}',
                            extra=strategy.extra_log
                        )
                        trade.close_position(
                            position, sl_tp_data.take_profit_price,
                            position.sz, date_time=date_time
                        )
                    else:
                        logger.debug(
                            f'Take profit price {sl_tp_data.take_profit_price} '
                            f'not reached {bid_price=} {ask_price=}',
                            extra=strategy.extra_log
                        )
    except AcquireLockException:
        logger.debug(
            'Task closing_emulate_position_by_limit is currently running',
            extra=strategy.extra_log
        )
    except Exception as e:
        logger.exception(e, extra=strategy.extra_log)
        raise e


@app.task
def closing_emulate_position_market_stop_loss(
    strategy_id: int, symbol: str, ask_price: float, bid_price: float, date_time: str
) -> None:
    try:
        strategy = Strategy.objects.cache(id=strategy_id)[0]
        strategy._extra_log.update(symbol=symbol)
        with TaskLock(f'closing_emulate_position_market_stop_loss_{strategy_id}_{symbol}'):
            position = strategy.get_last_emulate_open_position(symbol)
            if position and any(position.sl_tp_data.values()):
                strategy = position.strategy
                strategy._extra_log.update(position=position.id)
                trade = OkxEmulateTrade(strategy, position.symbol)
                sl_tp_data = Namespace(**position.sl_tp_data)
                if sl_tp_data.stop_loss_breakeven_order_id:
                    if position.side == 'long' and bid_price <= sl_tp_data.stop_loss_breakeven:
                        logger.info(
                            f'Bid price {bid_price} <= stop loss breakeven price '
                            f'{sl_tp_data.stop_loss_breakeven}',
                            extra=strategy.extra_log
                        )
                        trade.close_position(
                            position, bid_price, position.sz, date_time=date_time
                        )
                    elif position.side == 'short' and ask_price >= sl_tp_data.stop_loss_breakeven:
                        logger.info(
                            f'Ask price {ask_price} >= stop loss breakeven price '
                            f'{sl_tp_data.stop_loss_breakeven}',
                            extra=strategy.extra_log
                        )
                        trade.close_position(
                            position, ask_price, position.sz, date_time=date_time
                        )
                else:
                    if position.side == 'long' and bid_price <= sl_tp_data.stop_loss_price:
                        logger.info(
                            f'Bid price {bid_price} <= stop loss price '
                            f'{sl_tp_data.stop_loss_price}',
                            extra=strategy.extra_log
                        )
                        trade.close_position(position, bid_price, position.sz, date_time=date_time)
                    if position.side == 'short' and ask_price >= sl_tp_data.stop_loss_price:
                        logger.info(
                            f'Ask price {ask_price} >= stop loss price '
                            f'{sl_tp_data.stop_loss_price}',
                            extra=strategy.extra_log
                        )
                        trade.close_position(position, ask_price, position.sz, date_time=date_time)
    except AcquireLockException:
        logger.debug(
            'Task closing_emulate_position_market_stop_loss is currently running',
            extra=strategy.extra_log
        )
    except Exception as e:
        logger.exception(e, extra=strategy.extra_log)
        raise e


@app.task
def closing_emulate_position_market_take_profit(
    strategy_id: int, symbol: str, ask_price: float, bid_price: float, date_time: str
) -> None:
    try:
        strategy = Strategy.objects.cache(id=strategy_id)[0]
        strategy._extra_log.update(symbol=symbol)
        with TaskLock(f'closing_emulate_position_market_take_profit_{strategy_id}_{symbol}'):
            position = strategy.get_last_emulate_open_position(symbol)
            if position and any(position.sl_tp_data.values()):
                strategy = position.strategy
                strategy._extra_log.update(position=position.id)
                trade = OkxEmulateTrade(strategy, position.symbol)
                sl_tp_data = Namespace(**position.sl_tp_data)
                if position.side == 'long' and bid_price >= sl_tp_data.take_profit_price:
                    logger.info(
                        f'Bid price {bid_price} >= take profit price '
                        f'{sl_tp_data.take_profit_price}',
                        extra=strategy.extra_log
                    )
                    trade.close_position(
                        position, bid_price, position.sz, date_time=date_time
                    )
                if position.side == 'short' and ask_price <= sl_tp_data.take_profit_price:
                    logger.info(
                        f'Ask price {ask_price} <= take profit price '
                        f'{sl_tp_data.take_profit_price}',
                        extra=strategy.extra_log
                    )
                    trade.close_position(
                        position, ask_price, position.sz, date_time=date_time
                    )
    except AcquireLockException:
        logger.debug(
            'Task closing_emulate_position_market_take_profit is currently running',
            extra=strategy.extra_log
        )
    except Exception as e:
        logger.exception(e, extra=strategy.extra_log)
        raise e


@app.task
def closing_emulate_position_market_parts(
    strategy_id: int, symbol: str, ask_price: float, bid_price: float, date_time: str
) -> None:
    try:
        strategy = Strategy.objects.cache(id=strategy_id)[0]
        strategy._extra_log.update(symbol=symbol)
        with TaskLock(f'closing_emulate_position_market_parts_{strategy_id}_{symbol}'):
            position = strategy.get_last_emulate_open_position(symbol)
            if position and any(position.sl_tp_data.values()):
                strategy = position.strategy
                strategy._extra_log.update(position=position.id)
                trade = OkxEmulateTrade(strategy, position.symbol)
                sl_tp_data = Namespace(**position.sl_tp_data)
                if not sl_tp_data.first_part_closed:
                    if position.side == 'long' and bid_price >= sl_tp_data.tp_first_price:
                        logger.info(
                            f'Bid price {bid_price} >= take profit first price '
                            f'{sl_tp_data.tp_first_price}',
                            extra=strategy.extra_log
                        )
                        position.sl_tp_data['first_part_closed'] = True
                        trade.close_position(
                            position, bid_price, sl_tp_data.tp_first_part,
                            date_time=date_time
                        )
                    if position.side == 'short' and ask_price <= sl_tp_data.tp_first_price:
                        logger.info(
                            f'Ask price {ask_price} <= take profit first price '
                            f'{sl_tp_data.tp_first_price}',
                            extra=strategy.extra_log
                        )
                        position.sl_tp_data['first_part_closed'] = True
                        trade.close_position(
                            position, ask_price, sl_tp_data.tp_first_part,
                            date_time=date_time
                        )
                    if position.sl_tp_data['first_part_closed']:
                        if strategy.stop_loss_breakeven:
                            logger.info(
                                f'Update stop loss to breakeven {sl_tp_data.stop_loss_breakeven}',
                                extra=strategy.extra_log
                            )
                            position.sl_tp_data['stop_loss_breakeven_order_id'] = True
                            position.save(update_fields=['sl_tp_data'])
                elif not sl_tp_data.second_part_closed:
                    if position.side == 'long' and bid_price >= sl_tp_data.tp_second_price:
                        logger.info(
                            f'Bid price {bid_price} >= take profit second price '
                            f'{sl_tp_data.tp_second_price}',
                            extra=strategy.extra_log
                        )
                        position.sl_tp_data['second_part_closed'] = True
                        trade.close_position(
                            position, bid_price, sl_tp_data.tp_second_part,
                            date_time=date_time
                        )
                    if position.side == 'short' and ask_price <= sl_tp_data.tp_second_price:
                        logger.info(
                            f'Ask price {ask_price} <= take profit second price '
                            f'{sl_tp_data.tp_second_price}',
                            extra=strategy.extra_log
                        )
                        position.sl_tp_data['second_part_closed'] = True
                        trade.close_position(
                            position, ask_price, sl_tp_data.tp_second_part,
                            date_time=date_time
                        )
                else:
                    logger.warning('All parts of position are closed', extra=strategy.extra_log)
    except AcquireLockException:
        logger.debug(
            'Task closing_emulate_position_market_parts is currently running',
            extra=strategy.extra_log
        )
    except Exception as e:
        logger.exception(e, extra=strategy.extra_log)
        raise e


@app.task
def closing_trade_position_market_parts(
    strategy_id: int, symbol: str, ask_price: float, bid_price: float
) -> None:
    try:
        strategy = Strategy.objects.cache(id=strategy_id)[0]
        strategy._extra_log.update(symbol=symbol)
        with TaskLock(f'closing_trade_position_market_parts_{strategy_id}_{symbol}'):
            position = strategy.get_last_trade_open_position(symbol)
            if position and any(position.sl_tp_data.values()):
                sl_tp_data = Namespace(**position.sl_tp_data)
                strategy = position.strategy
                strategy._extra_log.update(position=position.id)
                trade = OkxTrade(strategy, position.symbol, position.sz, position.side)
                if not sl_tp_data.first_part_closed:
                    if position.side == 'long' and bid_price >= sl_tp_data.tp_first_price:
                        logger.info(
                            f'Bid price {bid_price} >= take profit first price '
                            f'{sl_tp_data.tp_first_price}',
                            extra=strategy.extra_log
                        )
                        position.sl_tp_data['first_part_closed'] = True
                    if position.side == 'short' and ask_price <= sl_tp_data.tp_first_price:
                        logger.info(
                            f'Ask price {ask_price} <= take profit first price '
                            f'{sl_tp_data.tp_first_price}',
                            extra=strategy.extra_log
                        )
                        position.sl_tp_data['first_part_closed'] = True
                    if position.sl_tp_data['first_part_closed']:
                        position.save(update_fields=['sl_tp_data'])
                        trade.close_position(sl_tp_data.tp_first_part)
                        logger.info(
                            'First part of position is closed, '
                            f'size_contract={sl_tp_data.tp_first_part}',
                            extra=strategy.extra_log
                        )
                        if strategy.stop_loss_breakeven and not sl_tp_data.stop_loss_breakeven_order_id:
                            order_id = trade.update_stop_loss(
                                price=sl_tp_data.stop_loss_breakeven,
                                sz=position.sz
                            )
                            logger.info(
                                'Updated stop loss to breakeven '
                                f'{sl_tp_data.stop_loss_breakeven}, {order_id=}',
                                extra=strategy.extra_log
                            )
                            position.sl_tp_data['stop_loss_breakeven_order_id'] = order_id
                            position.save(update_fields=['sl_tp_data'])
                elif not sl_tp_data.second_part_closed:
                    if position.side == 'long' and bid_price >= sl_tp_data.tp_second_price:
                        logger.info(
                            f'Bid price {bid_price} >= take profit second price '
                            f'{sl_tp_data.tp_second_price}',
                            extra=strategy.extra_log
                        )
                        position.sl_tp_data['second_part_closed'] = True
                    if position.side == 'short' and ask_price <= sl_tp_data.tp_second_price:
                        logger.info(
                            f'Ask price {ask_price} <= take profit second price '
                            f'{sl_tp_data.tp_second_price}',
                            extra=strategy.extra_log
                        )
                        position.sl_tp_data['second_part_closed'] = True
                    if position.sl_tp_data['second_part_closed']:
                        if sl_tp_data.increased_position:
                            trade.close_position(sl_tp_data.tp_second_part)
                        else:
                            trade.close_position(position.sz)
                        logger.info(
                            'Second part of position is closed, '
                            f'size_contract={sl_tp_data.tp_second_part}',
                            extra=strategy.extra_log
                        )
                        position.save(update_fields=['sl_tp_data'])
                elif sl_tp_data.increased_position and not sl_tp_data.third_part_closed:
                    if position.side == 'long' and bid_price >= sl_tp_data.tp_third_price:
                        logger.info(
                            f'Bid price {bid_price} >= take profit third price '
                            f'{sl_tp_data.tp_third_price}',
                            extra=strategy.extra_log
                        )
                        position.sl_tp_data['third_part_closed'] = True
                    if position.side == 'short' and ask_price <= sl_tp_data.tp_third_price:
                        logger.info(
                            f'Ask price {ask_price} <= take profit third price '
                            f'{sl_tp_data.tp_third_price}',
                            extra=strategy.extra_log
                        )
                        position.sl_tp_data['third_part_closed'] = True
                    if position.sl_tp_data['third_part_closed']:
                        trade.close_position(sl_tp_data.tp_third_part)
                        logger.info(
                            'Third part of position is closed, '
                            f'size_contract={sl_tp_data.tp_third_part}',
                            extra=strategy.extra_log
                        )
                        position.save(update_fields=['sl_tp_data'])
                elif sl_tp_data.increased_position and not sl_tp_data.fourth_part_closed:
                    if position.side == 'long' and bid_price >= sl_tp_data.tp_fourth_price:
                        logger.info(
                            f'Bid price {bid_price} >= take profit fourth price '
                            f'{sl_tp_data.tp_fourth_price}',
                            extra=strategy.extra_log
                        )
                        position.sl_tp_data['fourth_part_closed'] = True
                    if position.side == 'short' and ask_price <= sl_tp_data.tp_fourth_price:
                        logger.info(
                            f'Ask price {ask_price} <= take profit fourth price '
                            f'{sl_tp_data.tp_fourth_price}',
                            extra=strategy.extra_log
                        )
                        position.sl_tp_data['fourth_part_closed'] = True
                    if position.sl_tp_data['fourth_part_closed']:
                        trade.close_position(position.sz)
                        logger.info(
                            'Fourth part of position is closed, '
                            f'size_contract={sl_tp_data.tp_fourth_part}',
                            extra=strategy.extra_log
                        )
                        position.save(update_fields=['sl_tp_data'])
    except AcquireLockException:
        logger.debug(
            'Task closing_trade_position_market_parts is currently running',
            extra=strategy.extra_log
        )
    except Exception as e:
        logger.exception(e, extra=strategy.extra_log)
        raise e
