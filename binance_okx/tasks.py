import logging
import threading
import time
import re
from django.utils import timezone
from django.contrib.auth import get_user_model
from django.core.cache import cache
from django.conf import settings
# from celery.utils.log import get_task_logger
from binance.um_futures import UMFutures
import okx.MarketData
import okx.PublicData
import okx.Account
from exchange.celery import app
from .helper import TaskLock
from .models import Strategy, Symbol, Account, Position, StatusLog, OkxSymbol, BinanceSymbol, Bill
from .exceptions import AcquireLockException
from .trade import get_ask_bid_prices_and_condition
from .strategy import (
    open_trade_position, watch_trade_position, open_emulate_position,
    watch_emulate_position, increase_position
)
from .ws import WebSocketOkxAskBid, WebSocketBinaceAskBid, WebSocketOkxOrders, WebSocketOkxMarketPrice
from .handlers import (
    write_ask_bid_to_csv_and_cache_by_symbol, save_ask_bid_to_cache,
    save_filled_limit_order_id, save_okx_market_price_to_cache
)
from .exchange import OkxExchange


logger = logging.getLogger(__name__)


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


# @app.task
# def update_okx_market_price() -> None:
#     try:
#         with TaskLock('okx_task_update_market_price'):
#             client = okx.PublicData.PublicAPI(flag=settings.OKX_FLAG)
#             result = client.get_mark_price(instType='SWAP')
#             for i in result['data']:
#                 symbol = ''.join(i['instId'].split('-')[:-1])
#                 market_price = float(i['markPx'])
#                 cache.set(f'okx_market_price_{symbol}', market_price)
#     except AcquireLockException:
#         logger.debug('Task update_okx_market_price is already running')
#     except Exception as e:
#         logger.exception(e)
#         raise e
#     logger.info(f'Updated okx market prices for {len(result["data"])} symbols')


@app.task
def check_if_position_is_closed() -> None:
    try:
        accounts = Account.objects.filter(exchange='okx').all()
        if not accounts:
            logger.debug('Check if position is closed: No okx accounts found')
            return
        with TaskLock('okx_task_check_if_position_is_closed'):
            for account in accounts:
                try:
                    open_positions_db = Position.objects.filter(
                        is_open=True, mode=Strategy.Mode.trade,
                        strategy__second_account=account).all()
                    if not open_positions_db:
                        logger.debug(
                            f'No found any open positions in database for account: {account.name}'
                        )
                        continue
                    exchange = OkxExchange(account)
                    open_positions = exchange.get_open_positions()
                    threads = []
                    for position in open_positions_db:
                        position.strategy._extra_log.update(symbol=position.symbol.symbol, position=position.id)
                        # exchange.check_single_position(position, open_positions)
                        thread = threading.Thread(
                            target=exchange.check_and_update_single_position,
                            args=(position, open_positions)
                        )
                        threads.append(thread)
                        thread.start()
                    for thread in threads:
                        thread.join()
                except Exception as e:
                    logger.exception(e)
                    continue
    except AcquireLockException:
        logger.debug('Task check_if_position_is_closed is already running')
    except Exception as e:
        logger.exception(e)
        raise e


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
                    exchange = OkxExchange(account)
                    bills = exchange.get_bills()
                    if not bills:
                        continue
                    new_bills: list[Bill] = [
                        Bill(bill_id=b['billId'], account=account, data=b)
                        for b in bills if b['billId'] not in bills_ids
                    ]
                    if new_bills:
                        Bill.objects.bulk_create(new_bills, ignore_conflicts=True)
                        logger.info(f'Saved {len(new_bills)} bills to db for account: {account.name}')
                        logger.info(f'Bill ids: {", ".join([str(b.bill_id) for b in new_bills])}')
                    else:
                        logger.info(f'All bills are already exist in db for account: {account.name}')
                except Exception as e:
                    logger.exception(e)
                    continue
    except AcquireLockException:
        logger.debug('Task update_bills is already running')
    except Exception as e:
        logger.exception(e)
        raise e


@app.task
def run_strategy(strategy_id: int) -> None:
    try:
        strategy = Strategy.objects.cache(id=strategy_id, enabled=True)[0]
        for symbol in strategy.symbols.all():
            watch_position_for_symbol.delay(strategy.id, symbol.symbol)
    except Exception as e:
        logger.exception(e, extra=strategy.extra_log)


@app.task
def watch_position_for_symbol(strategy_id: int, symbol: str) -> None:
    try:
        strategy = Strategy.objects.cache(id=strategy_id, enabled=True)[0]
        symbol = Symbol.objects.get(symbol=symbol)
        with TaskLock(f'watch_position_for_symbol_{strategy_id}_{symbol.symbol}'):
            position = strategy.positions.filter(symbol=symbol, is_open=True).last()
            strategy._extra_log.update(symbol=symbol.symbol, position=position.id if position else None)
            if position:
                if strategy.mode == Strategy.Mode.trade:
                    watch_trade_position(strategy, position)
                elif strategy.mode == Strategy.Mode.emulate:
                    watch_emulate_position(strategy, position)
            else:
                logger.debug('No open position found', extra=strategy.extra_log)
    except AcquireLockException:
        logger.debug('Task is already running', extra=strategy.extra_log)
    except Exception as e:
        logger.exception(e, extra=strategy.extra_log)
        raise e


@app.task
def check_condition_met_by_symbol(symbol: str) -> None:
    try:
        symbol = Symbol.objects.get(symbol=symbol)
        strategies = Strategy.objects.cache(enabled=True, symbols=symbol)
        for strategy in strategies:
            positions = Position.objects.cache(strategy=strategy, symbol=symbol, is_open=True)
            if positions:
                position = positions[0]
            else:
                position = None
            strategy._extra_log.update(symbol=symbol, position=position.id if position else None)
            condition_met, position_side, prices = get_ask_bid_prices_and_condition(strategy, symbol)
            if condition_met:
                with TaskLock(f'task_check_condition_met_{symbol}'):
                    if strategy.mode == Strategy.Mode.trade:
                        if position:
                            increase_position(strategy, position, condition_met, prices)
                        else:
                            open_trade_position(strategy, symbol, position_side, prices)
                    elif strategy.mode == Strategy.Mode.emulate:
                        open_emulate_position(strategy, symbol, position_side, prices)
    except AcquireLockException:
        logger.debug(f'{symbol} task is already running')
    except Exception as e:
        logger.exception(e)
        raise e


@app.task
def run_websocket_okx_orders() -> None:
    try:
        with TaskLock('task_run_websocket_okx_orders'):
            accounts = Account.objects.filter(exchange='okx').all()
            if not accounts:
                logger.debug('WebSocketOkxOrders: No okx accounts found')
                return
            threads = {i.name: i for i in threading.enumerate()}
            for account in accounts:
                name = f'run_forever_okx_orders_{account.id}'
                if name in threads:
                    thread = threads[name]
                    if thread.is_alive():
                        logger.debug(f'WebSocketOkxOrders is already running for account: {account.name}')
                        continue
                    else:
                        logger.debug(f'WebSocketOkxOrders is exist but not running for account: {account.name}')
                        thread._kill()
                ws_okx_orders = WebSocketOkxOrders(account)
                ws_okx_orders.start()
                ws_okx_orders.add_handler(save_filled_limit_order_id)
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
            ws_okx_ask_bid.add_handler(save_ask_bid_to_cache)
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
            ws_binance_ask_bid.add_handler(
                lambda data: check_condition_met_by_symbol.delay(data['s'])
            )
            time.sleep(3)
    except AcquireLockException:
        logger.debug('Task run_websocket_binance_ask_bid is already running')
    except Exception as e:
        logger.exception(e)
        raise e
