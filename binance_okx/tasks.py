import logging
import threading
import time
import re
from django.core.cache import cache
from django.utils import timezone
from django.contrib.auth import get_user_model
from django.conf import settings
# from celery.utils.log import get_task_logger
from binance.um_futures import UMFutures
import okx.MarketData
import okx.PublicData
import okx.Account
from exchange.celery import app
from .helper import TaskLock, check_all_conditions, convert_dict_values, get_bills
from .models import Strategy, Symbol, Account, Position, StatusLog, OkxSymbol, BinanceSymbol, Bill, Execution
from .exceptions import AcquireLockException
from .strategy import (
    open_trade_position, open_emulate_position, increase_trade_position,
    place_orders_after_open_trade_position, place_orders_after_increase_trade_position,
    time_close_position
)
from .ws import (
    WebSocketOkxAskBid, WebSocketBinaceAskBid, WebSocketOkxOrders,
    WebSocketOkxMarketPrice, WebSocketOkxPositions
)
from .handlers import (
    write_ask_bid_to_csv_and_cache_by_symbol, save_okx_ask_bid_to_cache,
    save_okx_market_price_to_cache, orders_handler, check_at_market_price
)
from .trade import OkxTrade, OkxEmulateTrade


# logger = get_task_logger(__name__)
logger = logging.getLogger(__name__)
User = get_user_model()


@app.task
def clean_db_log(days: int = 5) -> None:
    logger.info('Cleaning database log')
    date = timezone.now() - timezone.timedelta(days=days)
    records, _ = StatusLog.objects.filter(created_at__lte=date).delete()
    logger.info(f'Deleted {records} database log records')


def update_okx_symbols() -> None:
    public_api = okx.PublicData.PublicAPI(flag=settings.OKX_FLAG)
    result = public_api.get_instruments(instType='SWAP')
    symbols = result['data']
    for i in symbols:
        inst_id = ''.join(i['instId'].split('-')[:-1])
        symbol, created = OkxSymbol.objects.update_or_create(symbol=inst_id, defaults={'data': i})
        if created:
            logger.info(f'Created okx symbol {symbol}')
        else:
            logger.info(f'Updated okx symbol {symbol}')


def update_binance_symbols() -> None:
    client = UMFutures(show_limit_usage=True)
    result = client.exchange_info()
    symbols = result['data']['symbols']
    for i in symbols:
        symbol = i['symbol']
        symbol, created = BinanceSymbol.objects.update_or_create(symbol=symbol, defaults={'data': i})
        if created:
            logger.info(f'Created binance symbol {symbol}')
        else:
            logger.info(f'Updated binance symbol {symbol}')


@app.task
def update_symbols() -> None:
    update_okx_symbols()
    update_binance_symbols()
    okx_symbols = list(OkxSymbol.objects.order_by('symbol'))
    binance_symbols = list(BinanceSymbol.objects.order_by('symbol'))
    for i in okx_symbols:
        for j in binance_symbols:
            okx_symbol = re.sub(r'\d', '', i.symbol)
            binance_symbol = re.sub(r'\d', '', j.symbol)
            if i.symbol == j.symbol:
                Symbol.objects.get_or_create(symbol=i.symbol, okx=i, binance=j)
                break
            elif okx_symbol == binance_symbol:
                logger.warning(f'Found similar symbols, okx: {i.symbol}, binance: {j.symbol}')
                Symbol.objects.get_or_create(symbol=i.symbol, okx=i, binance=j)
                break


@app.task
def update_bills():
    try:
        with TaskLock('okx_task_update_bills'):
            accounts = Account.objects.filter(exchange='okx').all()
            if not accounts:
                logger.debug('Update bills, no okx accounts found')
                return
            for account in accounts:
                try:
                    bills_ids: set[int] = set(
                        Bill.objects
                        .filter(account=account)
                        .values_list('bill_id', flat=True)
                    )
                    bill_id = None
                    bills = get_bills(account)
                    if bill_id:
                        logger.info(
                            f'Got {len(bills)} new bills from exchange, before {bill_id=}, '
                            f'for account: {account.name}'
                        )
                    else:
                        logger.debug(f'{account.name}. Got {len(bills)} last bills from exchange')
                    if not bills:
                        continue
                    new_bills: list[Bill] = [
                        Bill(bill_id=b['billId'], account=account, data=b)
                        for b in bills if b['billId'] not in bills_ids
                    ]
                    if new_bills:
                        Bill.objects.bulk_create(new_bills, ignore_conflicts=True)
                        logger.info(f'{account.name}. Saved {len(new_bills)} bills to db')
                        logger.info(f'Bill ids: {", ".join([str(b.bill_id) for b in new_bills])}')
                    else:
                        logger.debug(f'{account.name}. All bills are already exist in db')
                except Exception as e:
                    logger.exception(e)
                    continue
    except AcquireLockException:
        logger.debug('Task update_bills is already running')
    except Exception as e:
        logger.exception(e)
        raise e


@app.task
def create_execution(position_id: int) -> None:
    try:
        position = Position.objects.get(id=position_id)
        position.strategy._extra_log.update(symbol=position.symbol.symbol, position=position.id)
        order_ids = Bill.objects.filter(data__tradeId__in=position.trade_ids).values_list('data__ordId', flat=True)
        bills = Bill.objects.filter(data__ordId__in=order_ids).all()
        executions = []
        for bill in bills:
            if Execution.objects.filter(bill_id=bill.bill_id, trade_id=bill.data['tradeId']).exists():
                continue
            executions.append(
                Execution(
                    bill_id=bill.bill_id, trade_id=bill.data['tradeId'],
                    data=bill.data, position=position
                )
            )
        executions = Execution.objects.bulk_create(executions, ignore_conflicts=True)
        for execution in executions:
            logger.info(
                f'Created execution bill_id={execution.data["billId"]}, trade_id={execution.data["tradeId"]}, '
                f'subType={execution.data["subType"]}, sz={execution.data["sz"]}, px={execution.data["px"]}, '
                f'ordId={execution.data["ordId"]}',
                extra=position.strategy.extra_log
            )
    except Exception as e:
        logger.exception(e)
        raise e


@app.task
def create_or_update_position(data: dict) -> None:
    try:
        data = convert_dict_values(data)
        symbol: str = ''.join(data['instId'].split('-')[:-1])
        symbol: Symbol = Symbol.objects.get(symbol=symbol)
        account = Account.objects.get(id=data['account_id'])
        try:
            strategy = Strategy.objects.get(enabled=True, mode='trade', second_account=account, symbols=symbol)
        except Strategy.DoesNotExist:
            logger.error(f'Not found enabled strategy for symbol={symbol.symbol} and account={account.name}')
            return
        strategy._extra_log.update(symbol=symbol.symbol)
        positions = sorted(
            filter(lambda x: x.symbol == symbol and x.is_open is True, strategy.positions.all()),
            key=lambda x: x.id, reverse=True
        )
        position = positions[0] if positions else None
        strategy._extra_log.update(position=position.id if position else None)
        if not position and data['pos'] != 0:
            position = Position.objects.create(
                position_data=data, strategy=strategy, symbol=symbol, account=account,
                trade_ids=[data['tradeId']]
            )
            strategy._extra_log.update(position=position.id)
            logger.info(
                f'Created new position in database, avgPx={data["avgPx"]}, sz={data["pos"]}, '
                f'usdt={data["notionalUsd"]}, side={data["posSide"]}',
                extra=strategy.extra_log
            )
            place_orders_after_open_trade_position(position)
        else:
            if data['pos'] == 0:
                position.is_open = False
                if data['tradeId']:
                    position.trade_ids.append(data['tradeId'])
                position.save(update_fields=['is_open', 'trade_ids'])
                logger.info('Position was closed', extra=strategy.extra_log)
            else:
                increased = True if position.position_data['pos'] < data['pos'] else False
                position.position_data = data
                position.trade_ids.append(data['tradeId'])
                position.save(update_fields=['position_data', 'trade_ids'])
                logger.info(
                    f'Updated position data in database, avgPx={data["avgPx"]}, sz={data["pos"]}, '
                    f'usdt={data["notionalUsd"]}, side={data["posSide"]}',
                    extra=strategy.extra_log
                )
                if increased:
                    place_orders_after_increase_trade_position(position)
        create_execution.apply_async(args=(position.id,), countdown=5)
    except Exception as e:
        logger.exception(e)
        raise e


@app.task
def run_strategy(strategy_id: int) -> None:
    try:
        strategy = Strategy.objects.cache(id=strategy_id, enabled=True)[0]
        for symbol in strategy.symbols.all():
            check_position_close_time.delay(strategy.id, symbol.symbol)
    except Exception as e:
        logger.exception(e, extra=strategy.extra_log)


@app.task
def check_position_close_time(strategy_id: int, symbol: str) -> None:
    try:
        strategy = Strategy.objects.cache(id=strategy_id, enabled=True)[0]
        symbol = Symbol.objects.cache(symbol=symbol)[0]
        strategy._extra_log.update(symbol=symbol)
        with TaskLock(f'check_position_close_time_{strategy_id}_{symbol.symbol}'):
            if strategy.mode == Strategy.Mode.trade:
                position = strategy.get_last_trade_open_position(symbol.symbol)
                if position:
                    strategy._extra_log.update(symbol=symbol.symbol, position=position.id)
                    if time_close_position(strategy, position):
                        trade = OkxTrade(strategy, position.symbol, position.side)
                        trade.close_entire_position()
            elif strategy.mode == Strategy.Mode.emulate:
                position = strategy.get_last_emulate_open_position(symbol.symbol)
                if position:
                    strategy._extra_log.update(symbol=symbol.symbol, position=position.id)
                    if time_close_position(strategy, position):
                        trade = OkxEmulateTrade(strategy, position.symbol)
                        trade.close_position(position, strategy.position_size, completely=True)
    except AcquireLockException:
        logger.debug('Task check_position_close_time is already running', extra=strategy.extra_log)
    except Exception as e:
        logger.exception(e, extra=strategy.extra_log)
        raise e


@app.task
def open_or_increase_position(strategy_id: int, symbol: str, position_side: str, prices: dict) -> None:
    symbol = Symbol.objects.cache(symbol=symbol)[0]
    strategy = Strategy.objects.cache(id=strategy_id, enabled=True)[0]
    strategy._extra_log.update(symbol=symbol)
    try:
        lock = TaskLock(f'open_or_increase_position_{strategy.id}_{symbol}')
        if lock.acquire():
            cache.set(f'ask_bid_prices_{symbol}', prices)
            logger.info(f'Caching ask_bid_prices_{symbol} {prices}', extra=strategy.extra_log)
            position = Position.objects.filter(strategy=strategy, symbol=symbol, is_open=True).last()
            strategy._extra_log.update(symbol=symbol, position=position.id if position else None)
            if strategy.mode == Strategy.Mode.trade:
                if position:
                    increase_trade_position(strategy, position)
                else:
                    open_trade_position(strategy, symbol, position_side)
            elif strategy.mode == Strategy.Mode.emulate:
                open_emulate_position(strategy, symbol, position_side, prices)
                lock.release()
        else:
            logger.debug('Task open_or_increase_position is still running', extra=strategy.extra_log)
    except Exception as e:
        logger.exception(e, extra=strategy.extra_log)
        raise e


@app.task
def check_condition(symbol: str) -> None:
    try:
        symbol = Symbol.objects.cache(symbol=symbol)[0]
        strategies = Strategy.objects.cache(enabled=True, symbols=symbol)
        for strategy in strategies:
            strategy._extra_log.update(symbol=symbol)
            condition_met, position_side, prices = check_all_conditions(strategy, symbol)
            if condition_met:
                open_or_increase_position.delay(strategy.id, symbol.symbol, position_side, prices)
    except Exception as e:
        logger.exception(e)
        raise e


@app.task
def run_websocket_okx_positions() -> None:
    try:
        with TaskLock('task_run_websocket_okx_positions'):
            accounts = Account.objects.filter(exchange='okx').all()
            if not accounts:
                logger.debug('WebSocketOkxPositions no okx accounts found')
                return
            threads = {i.name: i for i in threading.enumerate()}
            for account in accounts:
                name = f'run_forever_okx_positions_{account.id}'
                if name in threads:
                    thread = threads[name]
                    if thread.is_alive():
                        logger.debug(f'{account.name}. WebSocketOkxPositions is already running')
                        continue
                    else:
                        logger.debug(f'{account.name}. WebSocketOkxPositions is exist but not running')
                        thread._kill()
                ws_okx_positions = WebSocketOkxPositions(account)
                ws_okx_positions.start()
                ws_okx_positions.add_handler(lambda data: create_or_update_position.delay(data))
                time.sleep(3)
    except AcquireLockException:
        logger.debug('Task run_websocket_okx_positions is already running')
    except Exception as e:
        logger.exception(e)
        raise e


@app.task
def run_websocket_okx_orders() -> None:
    try:
        with TaskLock('task_run_websocket_okx_orders'):
            accounts = Account.objects.filter(exchange='okx').all()
            if not accounts:
                logger.debug('WebSocketOkxOrders no okx accounts found')
                return
            threads = {i.name: i for i in threading.enumerate()}
            for account in accounts:
                name = f'run_forever_okx_orders_{account.id}'
                if name in threads:
                    thread = threads[name]
                    if thread.is_alive():
                        logger.debug(f'{account.name}. WebSocketOkxOrders is already running')
                        continue
                    else:
                        logger.debug(f'{account.name}. WebSocketOkxOrders is exist but not running')
                        thread._kill()
                ws_okx_orders = WebSocketOkxOrders(account)
                ws_okx_orders.start()
                ws_okx_orders.add_handler(lambda data: orders_handler.delay(data))
                time.sleep(3)
    except AcquireLockException:
        logger.debug('Task run_websocket_okx_orders is already running')
    except Exception as e:
        logger.exception(e)
        raise e


ws_okx_ask_bid = WebSocketOkxAskBid()
ws_binance_ask_bid = WebSocketBinaceAskBid()
ws_okx_market_price = WebSocketOkxMarketPrice()


@app.task
def run_websocket_okx_market_price() -> None:
    try:
        with TaskLock('task_run_websocket_okx_market_price'):
            ws_okx_market_price._threads.update(
                (i.name, i) for i in threading.enumerate() if i.name in ws_okx_market_price._threads
            )
            for name, thread in ws_okx_market_price._threads.items():
                if not thread or not thread.is_alive():
                    logger.warning(f'WebSocketOkxMarketPrice thread "{name}" is not running')
                    ws_okx_market_price._kill()
                    break
            else:
                logger.debug('WebSocketOkxMarketPrice all threads are running')
                return
            ws_okx_market_price.start()
            ws_okx_market_price.add_handler(save_okx_market_price_to_cache)
            ws_okx_market_price.add_handler(check_at_market_price)
            time.sleep(3)
    except AcquireLockException:
        logger.debug('Task run_websocket_okx_market_price is already running')
    except Exception as e:
        logger.exception(e)
        raise e


@app.task
def run_websocket_okx_ask_bid() -> None:
    try:
        with TaskLock('task_run_websocket_okx_ask_bid'):
            ws_okx_ask_bid._threads.update(
                (i.name, i) for i in threading.enumerate() if i.name in ws_okx_ask_bid._threads
            )
            for name, thread in ws_okx_ask_bid._threads.items():
                if not thread or not thread.is_alive():
                    logger.warning(f'WebSocketOkxAskBid thread "{name}" is not running')
                    ws_okx_ask_bid._kill()
                    break
            else:
                logger.debug('WebSocketOkxAskBid all threads are running')
                return
            ws_okx_ask_bid.start()
            ws_okx_ask_bid.add_handler(save_okx_ask_bid_to_cache)
            time.sleep(3)
    except AcquireLockException:
        logger.debug('Task run_websocket_okx_ask_bid is already running')
    except Exception as e:
        logger.exception(e)
        raise e


@app.task
def run_websocket_binance_ask_bid() -> None:
    try:
        with TaskLock('task_run_websocket_binance_ask_bid'):
            ws_binance_ask_bid._threads.update(
                (i.name, i) for i in threading.enumerate() if i.name in ws_binance_ask_bid._threads
            )
            for name, thread in ws_binance_ask_bid._threads.items():
                if not thread or not thread.is_alive():
                    logger.warning(f'WebSocketBinaceAskBid thread "{name}" is not running')
                    ws_binance_ask_bid._kill()
                    break
            else:
                logger.debug('WebSocketBinaceAskBid all threads are running')
                return
            ws_binance_ask_bid.start()
            ws_binance_ask_bid.add_handler(write_ask_bid_to_csv_and_cache_by_symbol)
            ws_binance_ask_bid.add_handler(lambda data: check_condition.delay(data['s']))
            time.sleep(3)
    except AcquireLockException:
        logger.debug('Task run_websocket_binance_ask_bid is already running')
    except Exception as e:
        logger.exception(e)
        raise e
