import logging
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
from .models import (
    Strategy, Symbol, Account, Position, StatusLog, OkxSymbol, BinanceSymbol,
    Bill
)
from .exceptions import AcquireLockException, ClosePositionException
from .strategy import (
    open_trade_position, open_emulate_position, increase_trade_position,
    place_orders_after_open_trade_position,
    calc_tp_and_place_orders_after_increase_trade_position,
    time_close_position
)
from .ws import (
    WebSocketOkxAskBid, WebSocketBinaceAskBid, WebSocketOkxOrders,
    WebSocketOkxMarketPrice, WebSocketOkxPositions, WebSocketOkxLastPrice
)
from .handlers import (
    write_ask_bid_to_csv_and_cache_by_symbol, save_okx_ask_bid_to_cache,
    save_okx_market_price_to_cache, orders_handler, closing_position_by_market,
    closing_position_by_limit
)
from .trade import OkxTrade, OkxEmulateTrade


# logger = get_task_logger(__name__)
logger = logging.getLogger(__name__)
User = get_user_model()


@app.task
def clean_db_log(days: int = 30) -> None:
    logger.info('Cleaning database log')
    date = timezone.now() - timezone.timedelta(days=days)
    records, _ = StatusLog.objects.filter(created_at__lte=date).delete()
    logger.info(f'Deleted {records} database log records')


def update_okx_symbols() -> None:
    public_api = okx.PublicData.PublicAPI(flag=settings.OKX_FLAG)
    result = public_api.get_instruments(instType='SWAP')
    symbols = result['data']
    available_symbols = {''.join(i['instId'].split('-')[:-1]) for i in symbols}
    for i in symbols:
        inst_id = ''.join(i['instId'].split('-')[:-1])
        is_active = i['state'] == 'live'
        symbol, created = OkxSymbol.objects.update_or_create(
            symbol=inst_id, defaults={'is_active': is_active, 'data': i}
        )
        if created:
            logger.info(f'Created okx symbol {symbol}')
        else:
            logger.info(f'Updated okx symbol {symbol}')
    for i in OkxSymbol.objects.all():
        if i.symbol not in available_symbols:
            i.is_active = False
            i.save(update_fields=['is_active'])
            logger.info(f'Deactivated okx symbol {i}')


def update_binance_symbols() -> None:
    client = UMFutures(show_limit_usage=True)
    result = client.exchange_info()
    symbols = result['data']['symbols']
    available_symbols = {i['symbol'] for i in symbols}
    for i in symbols:
        symbol = i['symbol']
        is_active = i['status'] == 'TRADING'
        symbol, created = BinanceSymbol.objects.update_or_create(
            symbol=symbol, defaults={'is_active': is_active, 'data': i}
        )
        if created:
            logger.info(f'Created binance symbol {symbol}')
        else:
            logger.info(f'Updated binance symbol {symbol}')
    for i in BinanceSymbol.objects.all():
        if i.symbol not in available_symbols:
            i.is_active = False
            i.save(update_fields=['is_active'])
            logger.info(f'Deactivated binance symbol {i}')


def remove_not_active_symbols_from_strategies() -> None:
    for strategy in Strategy.objects.all():
        symbols = strategy.symbols.all()
        for symbol in symbols:
            if not symbol.is_active:
                strategy.symbols.remove(symbol)
                logger.info(f'Removed not active symbol {symbol} from strategy {strategy}')


@app.task
def update_symbols() -> None:
    try:
        update_binance_symbols()
        update_okx_symbols()
        okx_symbols = list(OkxSymbol.objects.order_by('symbol'))
        binance_symbols = list(BinanceSymbol.objects.order_by('symbol'))
        available_symbols = set()
        for i in okx_symbols:
            for j in binance_symbols:
                okx_symbol = re.sub(r'\d', '', i.symbol)
                binance_symbol = re.sub(r'\d', '', j.symbol)
                if i.symbol == j.symbol or okx_symbol == binance_symbol:
                    if i.symbol != j.symbol:
                        logger.info(
                            f'Found similar symbols, okx: {i.symbol}, '
                            f'binance: {j.symbol}'
                        )
                    available_symbols.add(i.symbol)
                    is_active = i.is_active and j.is_active
                    Symbol.objects.update_or_create(
                        symbol=i.symbol,
                        defaults={'okx': i, 'binance': j, 'is_active': is_active}
                    )
                    break
        for i in Symbol.objects.all():
            if i.symbol not in available_symbols:
                i.is_active = False
                i.save(update_fields=['is_active'])
                logger.info(f'Deactivated symbol {i}')
        remove_not_active_symbols_from_strategies()
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
                    bills = get_bills(account)
                    logger.debug(
                        f'Got {len(bills)} last bills from exchange',
                        extra={'position': account.name}
                    )
                    if not bills:
                        continue
                    new_bills: list[Bill] = [
                        Bill(
                            bill_id=b['billId'],
                            order_id=b['ordId'],
                            trade_id=b['tradeId'],
                            account=account,
                            data=b,
                            symbol_id=''.join(b['instId'].split('-')[:-1]),
                            mode=Strategy.Mode.trade
                        )
                        for b in bills if b['billId'] not in bills_ids
                    ]
                    if new_bills:
                        Bill.objects.bulk_create(new_bills, ignore_conflicts=True)
                        for bill in new_bills:
                            logger.info(
                                f'Saved bill_id={bill.bill_id}, '
                                f'trade_id={bill.trade_id}, '
                                f'subType={bill.data["subType"]}, '
                                f'sz={bill.data["sz"]}, '
                                f'px={bill.data["px"]}, '
                                f'ordId={bill.order_id}',
                                extra={'symbol': bill.symbol, 'position': account.name}
                            )
                    else:
                        logger.debug(
                            'All bills are already exist in database',
                            extra={'position': account.name}
                        )
                except Exception as e:
                    logger.exception(e)
                    continue
    except AcquireLockException:
        logger.debug('Task update_bills is currently running')
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
            strategy = Strategy.objects.get(
                enabled=True, mode='trade', second_account=account, symbols=symbol
            )
        except Strategy.DoesNotExist:
            logger.error(
                'Not found enabled strategy', extra={'symbol': symbol.symbol}
            )
            return
        strategy._extra_log.update(symbol=symbol.symbol)
        position = strategy.get_last_open_trade_position(symbol.symbol)
        if position:
            strategy._extra_log.update(position=position.id)
            if data['pos'] == 0:
                position.is_open = False
                position.position_data['uTime'] = data['uTime']
                position.save(update_fields=['is_open', 'position_data'])
                position.add_trade_id(data['tradeId'])
                logger.info(
                    f'Position was closed, trade_id={data["tradeId"]}',
                    extra=strategy.extra_log
                )
            else:
                previous_pos = position.position_data['pos']
                position.position_data = data
                position.save(update_fields=['position_data'])
                position.add_trade_id(data['tradeId'])
                logger.info(
                    f'Updated position data in database, avgPx={data["avgPx"]}, '
                    f'sz={data["pos"]}, usdt={data["notionalUsd"]}, '
                    f'side={data["posSide"]}, trade_id={data["tradeId"]}',
                    extra=strategy.extra_log
                )
                if position.increased:
                    if previous_pos < data['pos']:
                        calc_tp_and_place_orders_after_increase_trade_position(position)
        else:
            if data['pos'] != 0:
                position = Position.objects.create(
                    position_data=data, strategy=strategy, symbol=symbol,
                    account=account, trade_ids=[data['tradeId']]
                )
                strategy._extra_log.update(position=position.id)
                logger.info(
                    f'Created new position in database, avgPx={data["avgPx"]}, '
                    f'sz={data["pos"]}, usdt={data["notionalUsd"]}, '
                    f'side={data["posSide"]}, trade_id={data["tradeId"]}',
                    extra=strategy.extra_log
                )
                place_orders_after_open_trade_position(position)
            else:
                logger.error(
                    'Not found open position in database, but position data not empty: '
                    f'pos={data["pos"]}, side={data["posSide"]}, '
                    f'trade_id={data["tradeId"]}, account={account.name}',
                    extra=strategy.extra_log
                )
                return
    except Exception as e:
        logger.exception(e, extra=strategy.extra_log)
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
                position = strategy.get_last_open_trade_position(symbol.symbol)
                if position:
                    strategy._extra_log.update(symbol=symbol.symbol, position=position.id)
                    if time_close_position(strategy, position):
                        trade = OkxTrade(strategy, position.symbol, position.sz, position.side)
                        trade.close_entire_position()
            elif strategy.mode == Strategy.Mode.emulate:
                position = strategy.get_last_open_emulate_position(symbol.symbol)
                if position:
                    strategy._extra_log.update(symbol=symbol.symbol, position=position.id)
                    if time_close_position(strategy, position):
                        trade = OkxEmulateTrade(strategy, position.symbol)
                        if position.side == 'long':
                            close_price = position.symbol.okx.bid_price
                        elif position.side == 'short':
                            close_price = position.symbol.okx.ask_price
                        trade.close_position(position, close_price, position.sz)
    except AcquireLockException:
        logger.trace(
            'Task check_position_close_time is currently running',
            extra=strategy.extra_log
        )
    except ClosePositionException as e:
        logger.error(e, extra=strategy.extra_log)
        if re.search('Position .+? exist', str(e)):
            position.is_open = False
            position.save(update_fields=['is_open'])
    except Exception as e:
        logger.exception(e, extra=strategy.extra_log)
        raise e


@app.task
def open_or_increase_position(
    strategy_id: int, symbol: str, position_side: str, prices: dict
) -> None:
    symbol = Symbol.objects.cache(symbol=symbol)[0]
    strategy = Strategy.objects.cache(id=strategy_id, enabled=True)[0]
    strategy._extra_log.update(symbol=symbol)
    try:
        with TaskLock(f'open_or_increase_position_{strategy_id}_{symbol.symbol}'):
            cache.set(f'okx_ask_bid_prices_{symbol}', prices)
            logger.debug(f'Caching {prices=}', extra=strategy.extra_log)
            position = Position.objects.filter(
                strategy=strategy, symbol=symbol, is_open=True).last()
            strategy._extra_log.update(
                symbol=symbol, position=position.id if position else None)
            if strategy.mode == Strategy.Mode.trade:
                if position:
                    if position.side == position_side:
                        if position.stop_loss_breakeven_set and not position.increased:
                            increase_trade_position(strategy, position, prices)
                else:
                    open_trade_position(strategy, symbol, position_side, prices)
            elif strategy.mode == Strategy.Mode.emulate:
                if not position:
                    open_emulate_position(strategy, symbol, position_side, prices)
    except AcquireLockException:
        logger.critical(
            'Task open_or_increase_position is currently running',
            extra=strategy.extra_log
        )
    except Exception as e:
        logger.exception(e, extra=strategy.extra_log)
        TaskLock(f'main_lock_{strategy.id}_{symbol}').release()
        logger.warning(
            'Main lock released. Exception occurred',
            extra=strategy.extra_log
        )
        raise e


@app.task(bind=True)
def check_condition(self, data: dict) -> None:
    try:
        with TaskLock(f'task_check_condition_{data["s"]}'):
            symbol = Symbol.objects.cache(symbol=data['s'])[0]
            strategies = Strategy.objects.cache(enabled=True, symbols=symbol)
            for strategy in strategies:
                try:
                    lock = TaskLock(f'main_lock_{strategy.id}_{symbol}', timeout=None)
                    if lock.locked():
                        continue
                    strategy._extra_log.update(symbol=symbol)
                    condition_met, position_side, prices = (
                        check_all_conditions(strategy, symbol, int(data['cts']))
                    )
                    if condition_met:
                        if lock.acquire():
                            logger.warning(
                                'Main lock acquired after condition met',
                                extra=strategy.extra_log
                            )
                            if strategy.reverse:
                                logger.warning(
                                    f'Reverse is enabled, reversing "{position_side}"',
                                    extra=strategy.extra_log
                                )
                                if position_side == 'long':
                                    position_side = 'short'
                                elif position_side == 'short':
                                    position_side = 'long'
                            open_or_increase_position.delay(
                                strategy.id, symbol.symbol, position_side, prices
                            )
                except Exception as e:
                    logger.exception(e, extra=strategy.extra_log)
    except AcquireLockException:
        logger.trace('Task check_condition is currently running')


@app.task
def run_websocket_okx_positions() -> None:
    try:
        with TaskLock('task_run_websocket_okx_positions'):
            accounts = Account.objects.filter(exchange='okx').all()
            if not accounts:
                logger.debug('WebSocketOkxPositions no okx accounts found')
                return
            for account in accounts:
                ws = WebSocketOkxPositions(account=account)
                if ws.is_alive():
                    logger.debug('Alive and running', extra={'symbol': ws.name})
                    continue
                else:
                    ws.kill()
                ws.start()
                ws.add_handler(
                    lambda data: create_or_update_position.delay(data)
                )
                time.sleep(3)
    except AcquireLockException:
        logger.debug('Task run_websocket_okx_positions is currently running')
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
            for account in accounts:
                ws = WebSocketOkxOrders(account=account)
                if ws.is_alive():
                    logger.debug('Alive and running', extra={'symbol': ws.name})
                    continue
                else:
                    ws.kill()
                ws.start()
                ws.add_handler(lambda data: orders_handler.delay(data))
                time.sleep(3)
    except AcquireLockException:
        logger.debug('Task run_websocket_okx_orders is currently running')
    except Exception as e:
        logger.exception(e)
        raise e


@app.task
def run_websocket_okx_last_price() -> None:
    try:
        with TaskLock('task_run_websocket_okx_last_price'):
            subscribed_inst_ids = set(
                cache.get('okx_last_price_subscribed_inst_ids', [])
            )
            strategy_inst_ids = set(
                Strategy.objects.filter(enabled=True)
                .values_list('symbols__okx__data__instId', flat=True)
                .distinct()
            )
            unusing_inst_ids = subscribed_inst_ids - strategy_inst_ids
            for inst_id in unusing_inst_ids:
                ws = WebSocketOkxMarketPrice(symbol=inst_id)
                ws.kill()
            for inst_id in strategy_inst_ids:
                try:
                    ws = WebSocketOkxLastPrice(symbol=inst_id)
                    if ws.is_alive():
                        logger.debug('Alive and running', extra={'symbol': ws.name})
                    else:
                        ws.kill()
                        ws.start()
                        ws.add_handler(lambda data: closing_position_by_limit.delay(data))
                except Exception as e:
                    logger.critical(e)
                finally:
                    time.sleep(0.5)
            cache.set('okx_last_price_subscribed_inst_ids', list(strategy_inst_ids))
    except AcquireLockException:
        logger.debug('Task run_websocket_okx_last_price is currently running')
    except Exception as e:
        logger.exception(e)
        raise e


@app.task
def run_websocket_okx_market_price() -> None:
    try:
        with TaskLock('task_run_websocket_okx_market_price'):
            subscribed_inst_ids = set(
                cache.get('okx_market_price_subscribed_inst_ids', [])
            )
            strategy_inst_ids = set(
                Strategy.objects.filter(enabled=True)
                .values_list('symbols__okx__data__instId', flat=True)
                .distinct()
            )
            unusing_inst_ids = subscribed_inst_ids - strategy_inst_ids
            for inst_id in unusing_inst_ids:
                ws = WebSocketOkxMarketPrice(symbol=inst_id)
                ws.kill()
            for inst_id in strategy_inst_ids:
                try:
                    ws = WebSocketOkxMarketPrice(symbol=inst_id)
                    extra = {'symbol': ws.symbol, 'position': ws.name}
                    if ws.is_alive():
                        logger.debug('Alive and running', extra=extra)
                    else:
                        ws.kill()
                        ws.start()
                        ws.add_handler(save_okx_market_price_to_cache)
                except Exception as e:
                    logger.critical(e, extra=extra)
                finally:
                    time.sleep(0.5)
            cache.set('okx_market_price_subscribed_inst_ids', list(strategy_inst_ids))
    except AcquireLockException:
        logger.debug('Task run_websocket_okx_market_price is currently running')
    except Exception as e:
        logger.exception(e)
        raise e


@app.task
def run_websocket_okx_ask_bid() -> None:
    try:
        with TaskLock('task_run_websocket_okx_ask_bid'):
            subscribed_inst_ids = set(
                cache.get('okx_ask_bid_subscribed_inst_ids', [])
            )
            strategy_inst_ids = set(
                Strategy.objects.filter(enabled=True)
                .values_list('symbols__okx__data__instId', flat=True)
                .distinct()
            )
            unusing_inst_ids = subscribed_inst_ids - strategy_inst_ids
            for inst_id in unusing_inst_ids:
                ws = WebSocketOkxAskBid(symbol=inst_id)
                ws.kill()
            for inst_id in strategy_inst_ids:
                try:
                    ws = WebSocketOkxAskBid(symbol=inst_id)
                    extra = {'symbol': ws.symbol, 'position': ws.name}
                    if ws.is_alive():
                        logger.debug('Alive and running', extra=extra)
                    else:
                        ws.kill()
                        ws.start()
                        ws.add_handler(save_okx_ask_bid_to_cache)
                        ws.add_handler(closing_position_by_market)
                except Exception as e:
                    logger.critical(e, extra=extra)
                finally:
                    time.sleep(0.5)
            cache.set('okx_ask_bid_subscribed_inst_ids', list(strategy_inst_ids))
    except AcquireLockException:
        logger.debug('Task run_websocket_okx_ask_bid is currently running')
    except Exception as e:
        logger.exception(e)
        raise e


@app.task
def run_websocket_binance_ask_bid() -> None:
    try:
        with TaskLock('task_run_websocket_binance_ask_bid'):
            subscribed_inst_ids = set(
                cache.get('binance_ask_bid_subscribed_inst_ids', [])
            )
            strategy_inst_ids = set(
                Strategy.objects.filter(enabled=True)
                .values_list('symbols__symbol', flat=True)
                .distinct()
            )
            unusing_inst_ids = subscribed_inst_ids - strategy_inst_ids
            for inst_id in unusing_inst_ids:
                ws = WebSocketBinaceAskBid(symbol=inst_id)
                ws.kill()
            for inst_id in strategy_inst_ids:
                try:
                    ws = WebSocketBinaceAskBid(symbol=inst_id)
                    extra = {'symbol': ws.symbol, 'position': ws.name}
                    if ws.is_alive():
                        logger.debug('Alive and running', extra=extra)
                    else:
                        ws.kill()
                        ws.start()
                        ws.add_handler(write_ask_bid_to_csv_and_cache_by_symbol)
                        ws.add_handler(lambda data: check_condition.delay(data))
                except Exception as e:
                    logger.critical(e, extra=extra)
                finally:
                    time.sleep(0.5)
            cache.set('binance_ask_bid_subscribed_inst_ids', list(strategy_inst_ids))
    except AcquireLockException:
        logger.debug('Task run_websocket_binance_ask_bid is currently running')
    except Exception as e:
        logger.exception(e)
        raise e
