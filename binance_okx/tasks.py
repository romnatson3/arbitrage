import logging
import threading
import time
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
from .helper import CachePrice, TaskLock
from .models import Strategy, Symbol, Account, Position, Execution, StatusLog, OkxSymbol, BinanceSymbol
from .exceptions import GetPositionException, GetExecutionException, AcquireLockException
from .misc import convert_dict_values
from .trade import get_ask_bid_prices_and_condition
from .strategy import (
    open_trade_position, watch_trade_position, open_emulate_position,
    watch_emulate_position
)
from .handlers import save_filled_limit_order_id
from .ws import WebSocketOrders


logger = logging.getLogger(__name__)
FLAG = settings.OKX_FLAG


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
    public_api = okx.PublicData.PublicAPI(flag=FLAG)
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
            if i.symbol == j.symbol:
                Symbol.objects.get_or_create(symbol=i.symbol, okx=i, binance=j)
                break


@app.task
def update_okx_market_price() -> None:
    try:
        with TaskLock('okx_task_update_market_price'):
            client = okx.PublicData.PublicAPI(flag=FLAG)
            result = client.get_mark_price(instType='SWAP')
            for i in result['data']:
                symbol = ''.join(i['instId'].split('-')[:-1])
                market_price = float(i['markPx'])
                cache.set(f'okx_market_price_{symbol}', market_price)
    except AcquireLockException:
        logger.debug('Task update_okx_market_price is already running')
    except Exception as e:
        logger.exception(e)
        raise e
    logger.info(f'Updated okx market prices for {len(result["data"])} symbols')


@app.task
def update_okx_ask_bid_price() -> None:
    try:
        with TaskLock('okx_task_update_ask_bid_price'):
            client = okx.MarketData.MarketAPI(flag=FLAG)
            result = client.get_tickers(instType='SWAP')
            cache_price = CachePrice('okx')
            for i in result['data']:
                symbol = ''.join(i['instId'].split('-')[:-1])
                cache_price.push_ask(symbol, i['askPx'])
                cache_price.push_bid(symbol, i['bidPx'])
    except AcquireLockException:
        logger.debug('Task update_okx_ask_bid_price is already running')
    except Exception as e:
        logger.exception(e)
        raise e
    logger.info(f'Updated okx ask/bid prices for {len(result["data"])} symbols')


@app.task
def update_binance_ask_bid_price() -> None:
    try:
        with TaskLock('binance_task_update_ask_bid_price'):
            client = UMFutures(show_limit_usage=True)
            result = client.book_ticker(symbol=None)
            cache_price = CachePrice('binance')
            for i in result['data']:
                symbol = i['symbol']
                cache_price.push_ask(symbol, i['askPrice'])
                cache_price.push_bid(symbol, i['bidPrice'])
    except AcquireLockException:
        logger.debug('Task update_binance_ask_bid_price is already running')
    except Exception as e:
        logger.exception(e)
        raise e
    logger.info(f'Updated binance ask/bid prices for {len(result["data"])} symbols')


@app.task
def update_ask_bid_price() -> None:
    update_okx_ask_bid_price.delay()
    update_binance_ask_bid_price.delay()


@app.task
def check_if_position_is_closed() -> None:
    accounts = Account.objects.filter(exchange='okx').all()
    if not accounts:
        logger.debug('No okx accounts found')
        return
    try:
        with TaskLock('okx_task_check_if_position_is_closed'):
            for account in accounts:
                try:
                    open_positions_db = Position.objects.filter(
                        is_open=True, mode=Strategy.Mode.trade,
                        strategy__second_account=account
                    ).all()
                    if not open_positions_db:
                        logger.debug(f'No found any open positions in database for account: {account.name}')
                        continue
                    client = okx.Account.AccountAPI(
                        account.api_key, account.api_secret, account.api_passphrase,
                        flag='1' if account.testnet else '0',
                        debug=False
                    )
                    result = client.get_positions(instType='SWAP')
                    if result['code'] != '0':
                        raise GetPositionException(f'Failed to get positions data. {result["msg"]}')
                    open_positions = {i['instId']: convert_dict_values(i) for i in result['data']}
                    logger.info(
                        f'Found {len(open_positions)} open positions in exchange for account: {account.name}. '
                        f'{", ".join(sorted(list(open_positions)))}'
                    )
                    last_execution = Execution.objects.filter(
                        position__mode=Strategy.Mode.trade).order_by('bill_id').last()
                    if not last_execution:
                        logger.debug('No found any executions in database')
                        result = client.get_account_bills(instType='SWAP', mgnMode='isolated', type=2)
                    else:
                        result = client.get_account_bills(
                            instType='SWAP', mgnMode='isolated', type=2,
                            before=last_execution.bill_id
                        )
                    if result['code'] != '0':
                        raise GetExecutionException(result)
                    all_executions = [convert_dict_values(i) for i in result['data']]
                    for position in open_positions_db:
                        try:
                            position.strategy._extra_log.update(symbol=position.symbol.symbol)
                            if position.symbol.okx.inst_id in open_positions:
                                logger.debug(
                                    f'Position "{position}" is still open in exchange',
                                    extra=position.strategy.extra_log
                                )
                                if (position.position_data['pos'] !=
                                    open_positions[position.symbol.okx.inst_id]['pos']):
                                    logger.warning(
                                        f'Position "{position}" size is different in database and exchange',
                                        extra=position.strategy.extra_log
                                    )
                                    position.position_data = open_positions[position.symbol.okx.inst_id]
                                    position.save()
                                    logger.info(
                                        f'Updated position "{position}"',
                                        extra=position.strategy.extra_log
                                    )
                                else:
                                    continue
                            else:
                                logger.warning(
                                    f'Position {position} is closed in exchange',
                                    extra=position.strategy.extra_log
                                )
                                position.is_open = False
                                position.save()
                            executions = [
                                i for i in all_executions
                                if i['instId'] == position.symbol.okx.inst_id
                            ]
                            if not executions:
                                logger.warning(
                                    f'No found any new executions for position {position}',
                                    extra=position.strategy.extra_log
                                )
                                continue
                            logger.info(
                                f'Found {len(executions)} executions for position {position}',
                                extra=position.strategy.extra_log
                            )
                            for e in executions:
                                if Execution.sub_type.get(e['subType']):
                                    e['subType'] = Execution.sub_type[e['subType']]
                                execution = Execution.objects.create(
                                    position=position, bill_id=e['billId'],
                                    trade_id=e['tradeId'], data=e
                                )
                                logger.info(
                                    f'Saved execution {execution.bill_id=} {execution.trade_id=}',
                                    extra=position.strategy.extra_log
                                )
                        except Exception as e:
                            logger.exception(e, extra=position.strategy.extra_log)
                            continue
                except Exception as e:
                    logger.exception(e)
                    continue
    except AcquireLockException:
        logger.debug('Task check_if_position_is_closed is already running')
    except Exception as e:
        logger.exception(e)
        raise e


@app.task
def run_strategy(strategy_id: int) -> None:
    try:
        strategy = Strategy.objects.cache(id=strategy_id, enabled=True, set=True)[0]
        for symbol in strategy.symbols.all():
            if strategy.mode == Strategy.Mode.trade:
                trade_strategy_for_symbol.delay(strategy.id, symbol.symbol)
            elif strategy.mode == Strategy.Mode.emulate:
                emulate_strategy_for_symbol.delay(strategy.id, symbol.symbol)
    except Exception as e:
        logger.exception(e, extra=strategy.extra_log)


@app.task
def trade_strategy_for_symbol(strategy_id: int, symbol: str) -> None:
    try:
        strategy = Strategy.objects.cache(id=strategy_id)[0]
        symbol = Symbol.objects.get(symbol=symbol)
        strategy._extra_log.update(symbol=symbol.symbol)
        with TaskLock(f'task_strategy_{strategy_id}_{symbol.symbol}'):
            position = strategy.positions.filter(symbol=symbol, is_open=True).last()
            if position:
                watch_trade_position(strategy, position)
                return
            condition_met, position_side, prices = get_ask_bid_prices_and_condition(strategy, symbol)
            if condition_met:
                open_trade_position(strategy, symbol, position_side, prices)
    except AcquireLockException:
        logger.debug('Task is already running', extra=strategy.extra_log)
    except Exception as e:
        logger.exception(e, extra=strategy.extra_log)
        raise e


@app.task
def emulate_strategy_for_symbol(strategy_id: int, symbol: str) -> None:
    try:
        strategy = Strategy.objects.cache(id=strategy_id)[0]
        symbol = Symbol.objects.get(symbol=symbol)
        strategy._extra_log.update(symbol=symbol.symbol)
        with TaskLock(f'task_emulate_strategy_{strategy_id}_{symbol.symbol}'):
            position = strategy.positions.filter(symbol=symbol, is_open=True).last()
            if position:
                watch_emulate_position(strategy, position)
                return
            condition_met, position_side, prices = get_ask_bid_prices_and_condition(strategy, symbol)
            if condition_met:
                open_emulate_position(strategy, symbol, position_side, prices)
    except AcquireLockException:
        logger.debug('Task is already running', extra=strategy.extra_log)
    except Exception as e:
        logger.exception(e, extra=strategy.extra_log)
        raise e


@app.task
def run_websocket_orders() -> None:
    try:
        with TaskLock('task_run_websocket_orders'):
            threads = {i.name: i for i in threading.enumerate()}
            accounts = Account.objects.filter(exchange='okx').all()
            if not accounts:
                logger.debug('No okx accounts found')
                return
            for account in accounts:
                name = f'run_forever_{account.id}'
                if name in threads:
                    thread = threads[name]
                    if thread.is_alive():
                        logger.debug(f'WebSocketOrders is already running for account: {account.name}')
                        continue
                    else:
                        logger.debug(f'WebSocketOrders is exist but not running for account: {account.name}')
                        thread._kill()
                ws_orders = WebSocketOrders(account)
                ws_orders.start()
                ws_orders.add_handler(save_filled_limit_order_id)
                time.sleep(3)
    except AcquireLockException:
        logger.debug('Task run_websocket_orders is already running')
    except Exception as e:
        logger.exception(e)
        raise e
