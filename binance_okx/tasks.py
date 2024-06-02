import logging
import websocket
from websocket._exceptions import WebSocketConnectionClosedException, WebSocketException
import threading
import json
import base64
import hmac
import time
import ctypes
from typing import Callable
from django.utils import timezone
from django.contrib.auth import get_user_model
from django.core.cache import cache
# from celery.utils.log import get_task_logger
from celery.signals import worker_ready
from binance.um_futures import UMFutures
import okx.MarketData
import okx.PublicData
import okx.Account
from exchange.celery import app
from .helper import CachePrice, TaskLock
from .models import Strategy, Symbol, Account, Position, Execution, StatusLog, OkxSymbol, BinanceSymbol
from .exceptions import GetPositionException, GetExecutionException, AcquireLockException
from .misc import convert_dict_values
from .trade import open_position, check_price_condition, watch_position
from .handlers import save_filled_limit_order_id


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
    public_api = okx.PublicData.PublicAPI(flag='0')
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
            client = okx.PublicData.PublicAPI(flag='0')
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
            client = okx.MarketData.MarketAPI(flag='0')
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
def check_if_position_is_closed() -> None:
    accounts = Account.objects.filter(exchange='okx').all()
    if not accounts:
        logger.debug('No okx accounts found')
        return
    try:
        with TaskLock('okx_task_check_if_position_is_closed'):
            for account in accounts:
                try:
                    open_positions_db = Position.objects.filter(is_open=True, strategy__second_account=account).all()
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
                    open_positions_ex = {i['instId']: convert_dict_values(i) for i in result['data']}
                    logger.info(
                        f'Found {len(open_positions_ex)} open positions in exchange for account: {account.name}. '
                        f'{", ".join(sorted(list(open_positions_ex)))}'
                    )
                    last_execution = Execution.objects.order_by('bill_id').last()
                    result = client.get_account_bills(
                        instType='SWAP', mgnMode='isolated', type=2,
                        before=last_execution.bill_id
                    )
                    if result['code'] != '0':
                        raise GetExecutionException(result['data'][0]['sMsg'])
                    all_executions = [convert_dict_values(i) for i in result['data']]
                    for position in open_positions_db:
                        try:
                            position.strategy._extra_log.update(symbol=position.symbol.symbol)
                            if position.symbol.okx.inst_id in open_positions_ex:
                                logger.debug(
                                    f'Position "{position}" is still open in exchange',
                                    extra=position.strategy.extra_log
                                )
                                if position.size != open_positions_ex[position.symbol.okx.inst_id]['availPos']:
                                    logger.warning(
                                        f'Position "{position}" size is different in database and exchange',
                                        extra=position.strategy.extra_log
                                    )
                                    position.position_data = open_positions_ex[position.symbol.okx.inst_id]
                                    position.save()
                                    logger.info(f'Updated position "{position}"', extra=position.strategy.extra_log)
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
                                    position=position, bill_id=e['billId'], trade_id=e['tradeId'], data=e
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
            strategy_for_symbol.delay(strategy.id, symbol.symbol)
    except Exception as e:
        logger.exception(e, extra=strategy.extra_log)


@app.task
def strategy_for_symbol(strategy_id: int, symbol: str) -> None:
    try:
        strategy = Strategy.objects.cache(id=strategy_id)[0]
        strategy._extra_log.update(symbol=symbol)
        with TaskLock(f'task_strategy_{strategy_id}_{symbol}'):
            position = strategy.positions.filter(symbol__symbol=symbol, is_open=True).last()
            if position:
                watch_position(strategy, position)
                return
            condition_met, position_side, prices = check_price_condition(strategy, symbol)
            if condition_met:
                open_position(strategy, symbol, position_side, prices)
    except AcquireLockException:
        logger.debug('Task is already running', extra=strategy.extra_log)
    except Exception as e:
        logger.exception(e, extra=strategy.extra_log)
        raise e


class WebSocketOrders():
    def __new__(cls, *args, **kwargs):
        if not hasattr(cls, 'instance'):
            cls.instance = super().__new__(cls)
        return cls.instance

    def __init__(self, account: Account) -> None:
        self.is_run = False
        self.ws = websocket.WebSocket()
        self._run_forever_thread = None
        self._threads = []
        self.account = account
        production_url = 'wss://ws.okx.com:8443/ws/v5/private'
        demo_trading_url = 'wss://wspap.okx.com:8443/ws/v5/private?brokerId=9999'
        self.url = demo_trading_url if account.testnet else production_url
        self._handlers = []

    def _get_login_subscribe(self) -> dict:
        ts = str(int(time.time()))
        sign = ts + 'GET' + '/users/self/verify'
        mac = hmac.new(
            bytes(self.account.api_secret, encoding='utf8'),
            bytes(sign, encoding='utf-8'),
            digestmod='sha256'
        )
        sign = base64.b64encode(mac.digest()).decode(encoding='utf-8')
        login = {
            'op': 'login',
            'args': [{
                'apiKey': self.account.api_key,
                'passphrase': self.account.api_passphrase,
                'timestamp': ts,
                'sign': sign
            }]
        }
        return login

    def _get_orders_subscribe(self) -> dict:
        return {
            'op': 'subscribe',
            'args': [{
                'channel': 'orders',
                'instType': 'SWAP'
            }]
        }

    def _connect(self):
        self.ws.connect(self.url)
        logger.info(f'Connected to {self.url}')

    def _login(self):
        self.ws.send(json.dumps(self._get_login_subscribe()))

    def _subscribe_orders(self):
        self.ws.send(json.dumps(self._get_orders_subscribe()))
        logger.info('Subscribed to orders')

    def ping(self):
        while self.is_run and self._threads[0].is_alive():
            try:
                if not self.ws.connected:
                    logger.debug('Socket is not connected')
                    continue
                self.ws.send('ping')
                logger.debug('Ping sent')
            except Exception as e:
                logger.error(f'Ping error: {e}')
                continue
            finally:
                time.sleep(15)
        else:
            logger.debug('Ping stopped')

    def message_handler(self, message: str) -> None | dict:
        if message == 'pong':
            logger.debug('Pong received')
            return
        try:
            message = json.loads(message)
        except json.decoder.JSONDecodeError:
            return
        event = message.get('event')
        data = message.get('data')
        if event == 'error':
            logger.error(message)
        elif event == 'subscribe':
            logger.info(f'Subscribe {message}')
        elif event == 'login':
            logger.info(f'Logged in to account: {self.account.name}')
            self._subscribe_orders()
        elif data:
            return data

    def run_forever(self) -> None:
        while self.is_run:
            try:
                self._connect()
                self._login()
                while self.is_run:
                    message = self.ws.recv()
                    data = self.message_handler(message)
                    if data:
                        logger.debug(data)
                        for handler in self._handlers:
                            handler(self.account.id, data[0])
            except WebSocketConnectionClosedException:
                logger.warning('Connection closed')
            except WebSocketException as e:
                logger.exception(e)
                self.ws.close()
            finally:
                time.sleep(3)
        else:
            self.ws.close()
            logger.info('WebSocket connection is closed')

    def launch(self):
        run_forever_thread = threading.Thread(
            target=self.run_forever, daemon=True, name=f'run_forever_{self.account.id}')
        run_forever_thread.start()
        self._threads.append(run_forever_thread)
        ping_thread = threading.Thread(target=self.ping, daemon=True)
        ping_thread.start()
        self._threads.append(ping_thread)
        logger.info('WebSocketOrders is started')

    def start(self):
        if self.is_run:
            logger.warning('WebSocketOrders is already running')
            return
        self.is_run = True
        self.launch()

    def stop(self):
        self.is_run = False
        logger.warning('WebSocketOrders is stopped')

    def _kill(self):
        for thread in self._threads:
            thread_id = ctypes.c_long(thread.ident)
            res = ctypes.pythonapi.PyThreadState_SetAsyncExc(thread_id, ctypes.py_object(SystemExit))
            if res == 0:
                raise ValueError('Nonexistent thread id')
            elif res > 1:
                ctypes.pythonapi.PyThreadState_SetAsyncExc(thread_id, 0)
                raise SystemError('PyThreadState_SetAsyncExc failed')
            logger.info(f'Thread {thread} is killed')

    def add_handler(self, callback: Callable[[int, dict], None]) -> None:
        self._handlers.append(callback)


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
