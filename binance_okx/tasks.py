import logging
import threading
import time
from typing import Any, Dict, List, Optional
from django.utils import timezone
from django.contrib.auth import get_user_model
from django.core.cache import cache
from django.conf import settings
from django.db.models import Func, Value, DateTimeField, CharField, F
from django.db.models.expressions import RawSQL
# from celery.utils.log import get_task_logger
from binance.um_futures import UMFutures
import okx.MarketData
import okx.PublicData
import okx.Account
from exchange.celery import app
from .helper import CachePrice, TaskLock
from .models import Strategy, Symbol, Account, Position, Execution, StatusLog, OkxSymbol, BinanceSymbol, Bill
from .exceptions import GetPositionException, AcquireLockException, GetBillsException
from .misc import convert_dict_values
from .trade import OkxTrade, get_ask_bid_prices_and_condition
from .strategy import (
    open_trade_position, watch_trade_position, open_emulate_position,
    watch_emulate_position
)
from .handlers import save_filled_limit_order_id
from .ws import WebSocketOrders
from .ws_ask_bid import WebSocketOkxAskBid, WebSocketBinaceAskBid
from .handlers import write_ask_bid_to_csv_by_symbol, save_ask_bid_to_cache


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
            if i.symbol == j.symbol:
                Symbol.objects.get_or_create(symbol=i.symbol, okx=i, binance=j)
                break


@app.task
def update_okx_market_price() -> None:
    try:
        with TaskLock('okx_task_update_market_price'):
            client = okx.PublicData.PublicAPI(flag=settings.OKX_FLAG)
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
            client = okx.MarketData.MarketAPI(flag=settings.OKX_FLAG)
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


class OkxExchange():
    def __init__(self, account: Account):
        self.account = account
        self.client = okx.Account.AccountAPI(
            account.api_key, account.api_secret, account.api_passphrase,
            flag='1' if account.testnet else '0',
            debug=False
        )

    def get_open_positions(self) -> None:
        result = self.client.get_positions(instType='SWAP')
        if result['code'] != '0':
            raise GetPositionException(f'Failed to get positions data. {result}')
        open_positions = {i['instId']: convert_dict_values(i) for i in result['data']}
        logger.info(
            f'Found {len(open_positions)} open positions in exchange for account: {self.account.name}'
        )
        logger.info(f'Symbols: {", ".join(sorted(list(open_positions)))}')
        return open_positions

    def get_bills(self, bill_id: Optional[int] = None) -> list[dict[str, Any]]:
        if not bill_id:
            result = self.client.get_account_bills(instType='SWAP', mgnMode='isolated', type=2)
        else:
            result = self.client.get_account_bills(
                instType='SWAP', mgnMode='isolated', type=2,
                before=bill_id
            )
        if result['code'] != '0':
            raise GetBillsException(result)
        bills = list(map(convert_dict_values, result['data']))
        for i in bills:
            i['subType'] = Execution.sub_type.get(i['subType'], i['subType'])
        if bill_id:
            logger.info(
                f'Found {len(bills)} new bills in exchange, before {bill_id=}, '
                f'for account: {self.account.name}'
            )
        else:
            logger.info(f'Got {len(bills)} last bills from exchange for account: {self.account.name}')
        return bills

    def get_new_executions_for_position(self, position: Position) -> Optional[List[Dict[str, Any]]]:
        position.strategy._extra_log.update(symbol=position.symbol.symbol)
        end_time = time.time() + settings.RECEIVE_TIMEOUT
        last_bill_id = position.executions.values_list('bill_id', flat=True).order_by('bill_id').last()
        if not last_bill_id:
            logger.debug(
                f'No found any executions in database for position "{position}"',
                extra=position.strategy.extra_log
            )
        while time.time() < end_time:
            if last_bill_id:
                logger.debug(
                    f'Trying to get new executions for position "{position}", '
                    f'before bill_id: {last_bill_id}',
                    extra=position.strategy.extra_log
                )
                where = {
                    'account': self.account,
                    'data__instId': position.symbol.okx.inst_id,
                    'bill_id__gt': last_bill_id
                }
            else:
                logger.debug(
                    f'Trying to get new executions for position "{position}", '
                    f'after position creation time "{position.position_data["cTime"]}"',
                    extra=position.strategy.extra_log
                )
                where = {
                    'account': self.account,
                    'data__instId': position.symbol.okx.inst_id,
                    'ts__gte': F('ctime')
                }
            executions = (
                Bill.objects.annotate(
                    ctime_str=Value(position.position_data['cTime'], output_field=CharField()),
                    ctime=Func(
                        'ctime_str',
                        Value('DD-MM-YYYY HH24:MI:SS.US'),
                        function='to_timestamp',
                        output_field=DateTimeField()
                    ),
                    ts_str=RawSQL("data->>'ts'", []),
                    ts=Func(
                        'ts_str',
                        Value('DD-MM-YYYY HH24:MI:SS.US'),
                        function='to_timestamp',
                        output_field=DateTimeField()
                    )).filter(**where).values_list('data', flat=True)
            )
            if executions:
                logger.info(
                    f'Found {len(executions)} executions for position "{position}"',
                    extra=position.strategy.extra_log
                )
                return executions
            else:
                if position.is_open:
                    if last_bill_id:
                        logger.debug(
                            f'Not found any new executions for open position "{position}"',
                            extra=position.strategy.extra_log
                        )
                        return
                    else:
                        logger.warning(
                            f'Not found any executions for open position "{position}"',
                            extra=position.strategy.extra_log
                        )
                else:
                    logger.warning(
                        f'Not found any new executions for closed position "{position}"',
                        extra=position.strategy.extra_log
                    )
            time.sleep(2)
        else:
            logger.critical(
                f'Failed to get executions for position "{position}"',
                extra=position.strategy.extra_log
            )

    def check_single_position(self, position: Position, open_positions: Dict[str, Any]) -> None:
        try:
            position.strategy._extra_log.update(symbol=position.symbol.symbol)
            if position.symbol.okx.inst_id in open_positions:
                logger.debug(
                    f'Position "{position}" is still open in exchange',
                    extra=position.strategy.extra_log
                )
            else:
                logger.warning(
                    f'Position {position} is closed in exchange',
                    extra=position.strategy.extra_log
                )
                position.is_open = False
                position.save()
                logger.info(
                    f'Position "{position}" is closed in database',
                    extra=position.strategy.extra_log
                )
            executions = self.get_new_executions_for_position(position)
            if executions:
                for execution in executions:
                    OkxTrade.save_execution(execution, position)
        except Exception as e:
            logger.exception(e, extra=position.strategy.extra_log)


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
                        # exchange.check_single_position(position, open_positions)
                        thread = threading.Thread(
                            target=exchange.check_single_position,
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
                # try:
                #     bills_ids: set[int] = set(
                #         Bill.objects
                #         .filter(account=account)
                #         .values_list('bill_id', flat=True)
                #     )
                #     exchange = OkxExchange(account)
                #     last_bill_id: Optional[int] = max(bill_ids) if bill_ids else None
                #     bills = exchange.get_bills(last_bill_id)
                #     if not bills:
                #         continue
                #     new_bills: list[Bill] = [
                #         Bill(bill_id=b['billId'], account=account, data=b)
                #         for b in bills if b['billId'] not in bills_ids
                #     ]
                #     if new_bills:
                #         bills = Bill.objects.bulk_create(new_bills, ignore_conflicts=True)
                #         logger.info(f'Saved {len(new_bills)} new bills to db for account: {account.name}')
                #     else:
                #         logger.warning(f'All bills are already exist in db for account: {account.name}')
                # except Exception as e:
                #     logger.exception(e)
                #     continue
    except AcquireLockException:
        logger.debug('Task update_bills is already running')
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
def run_websocket_okx_orders() -> None:
    try:
        with TaskLock('task_run_websocket_okx_orders'):
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


ws_okx_ask_bid = WebSocketOkxAskBid()
ws_binance_ask_bid = WebSocketBinaceAskBid()


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
            ws_binance_ask_bid.add_handler(save_ask_bid_to_cache)
            ws_binance_ask_bid.add_handler(write_ask_bid_to_csv_by_symbol)
            time.sleep(3)
    except AcquireLockException:
        logger.debug('Task run_websocket_binance_ask_bid is already running')
    except Exception as e:
        logger.exception(e)
        raise e
