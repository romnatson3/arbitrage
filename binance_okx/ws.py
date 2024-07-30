import logging
import websocket
from typing import Any
from websocket._exceptions import (
    WebSocketConnectionClosedException, WebSocketException, WebSocketPayloadException
)
import json
import ctypes
import threading
import time
import base64
import hmac
from typing import Callable
from django.conf import settings
from binance_okx.models import Strategy, Account


logger = logging.getLogger(__name__)


class WebSocketOkxAskBid():
    def __init__(self) -> None:
        self.is_run = False
        self.ws = websocket.WebSocket()
        production_url = 'wss://ws.okx.com:8443/ws/v5/public'
        demo_trading_url = 'wss://wspap.okx.com:8443/ws/v5/public'
        self.url = production_url if settings.OKX_FLAG == '0' else demo_trading_url
        self._handlers = []
        self._subscribed_inst_ids = []
        self._inst_id_field_path = 'symbols__okx__data__instId'
        self._threads = dict(
            run_forever_okx_ask_bid=None, ping_okx_ask_bid=None, monitor_okx_ask_bid=None)
        self.previous_ask_bid: dict[str, list[float]] = {}

    def monitoring_inst_ids(self) -> None:
        while self.is_run:
            try:
                if not self.ws.connected:
                    logger.debug('Monitoring not started. Socket is not connected')
                    continue
                strategy_inst_ids = set(
                    Strategy.objects.filter(enabled=True)
                    .values_list(self._inst_id_field_path, flat=True).distinct()
                )
                subscribed_inst_ids = set(self._subscribed_inst_ids)
                new_inst_ids = strategy_inst_ids - subscribed_inst_ids
                if new_inst_ids:
                    logger.info(f'Found unregistered {new_inst_ids=}')
                    self.subscribe_inst_id(new_inst_ids)
                unusing_inst_ids = subscribed_inst_ids - strategy_inst_ids
                if unusing_inst_ids:
                    logger.info(f'Fount unusing {unusing_inst_ids=}')
                    self.unsubscribe_inst_id(unusing_inst_ids)
            except Exception as e:
                logger.error(f'Monitoring error: {e}')
                continue
            finally:
                time.sleep(15)
        else:
            logger.debug('Monitoring stopped')

    def subscribe_inst_id(self, inst_ids: list[str]) -> dict:
        for inst_id in inst_ids:
            d = {
                'op': 'subscribe',
                'args': [{
                    'channel': 'tickers',
                    'instId': inst_id
                }]
            }
            self.ws.send(json.dumps(d))
            time.sleep(0.2)
            self._subscribed_inst_ids.append(inst_id)
            logger.info(f'Subscribed to {inst_id=}')

    def unsubscribe_inst_id(self, inst_ids: list[str]) -> dict:
        for inst_id in inst_ids:
            d = {
                'op': 'unsubscribe',
                'args': [{
                    'channel': 'tickers',
                    'instId': inst_id
                }]
            }
            self.ws.send(json.dumps(d))
            time.sleep(0.2)
            if inst_id in self._subscribed_inst_ids:
                self._subscribed_inst_ids.remove(inst_id)
            logger.info(f'Unsubscribed from {inst_id=}')

    def _connect(self):
        self.ws.connect(self.url)
        logger.info(f'{self.__class__.__name__} connected to {self.url}')

    def add_handler(self, callback: Callable[[int, dict], None]) -> None:
        self._handlers.append(callback)

    def ping(self):
        while self.is_run:
            try:
                if not self.ws.connected:
                    logger.debug(f'{self.__class__.__name__} ping not started. Socket is not connected')
                    continue
                self.ws.send('ping')
                logger.debug(f'{self.__class__.__name__} ping sent')
            except Exception as e:
                logger.error(f'{self.__class__.__name__} ping error: {e}')
                continue
            finally:
                time.sleep(25)
        else:
            logger.debug('Ping stopped')

    def _message_handler(self, message: str) -> None | dict:
        if message == 'pong':
            logger.debug(f'{self.__class__.__name__} pong received')
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
        elif event == 'unsubscribe':
            logger.info(f'Unsubscribe {message}')
        elif data:
            data = data[0]
            keys = set(['instId', 'askPx', 'bidPx', 'ts'])
            if not set(data.keys()).issuperset(keys):
                logger.error(f'Not all keys are in message. {data=}')
                return
            previous_ask_bid = self.previous_ask_bid.get(data['instId'])
            if previous_ask_bid:
                if previous_ask_bid[0] == data['askPx'] and previous_ask_bid[1] == data['bidPx']:
                    return
            self.previous_ask_bid[data['instId']] = [data['askPx'], data['bidPx']]
            return {k: v for k, v in data.items() if k in keys}

    def init(self):
        self._connect()
        self._subscribed_inst_ids = []

    def run_forever(self) -> None:
        while self.is_run:
            try:
                self.init()
                while self.is_run:
                    try:
                        message = self.ws.recv()
                        data = self._message_handler(message)
                        if data:
                            logger.debug(data)
                            for handler in self._handlers:
                                handler(data)
                    except WebSocketPayloadException as e:
                        logger.error(e)
                    except WebSocketException:
                        raise
                    except Exception as e:
                        logger.exception(e)
            except WebSocketConnectionClosedException:
                logger.warning(f'{self.__class__.__name__} connection closed')
            except WebSocketException as e:
                logger.exception(e)
                self.ws.close()
            finally:
                time.sleep(3)
        else:
            self.ws.close()
            logger.info(f'{self.__class__.__name__} is stopped')

    def launch(self):
        try:
            names = list(self._threads.keys())
            run_forever_thread = threading.Thread(
                target=self.run_forever, daemon=True, name=names[0])
            run_forever_thread.start()
            self._threads[run_forever_thread.name] = run_forever_thread
            logger.info(f'{self.__class__.__name__} is started')
            ping_thread = threading.Thread(target=self.ping, daemon=True, name=names[1])
            ping_thread.start()
            self._threads[ping_thread.name] = ping_thread
            logger.info('Ping thread is started')
            monitor_thread = threading.Thread(
                target=self.monitoring_inst_ids, daemon=True, name=names[2])
            monitor_thread.start()
            self._threads[monitor_thread.name] = monitor_thread
            logger.info('Monitoring thread is started')
        except Exception as e:
            logger.exception(e)
            raise

    def start(self):
        if self.is_run:
            logger.warning(f'{self.__class__.__name__} is already running')
            return
        self.is_run = True
        self.launch()

    def stop(self):
        self.is_run = False
        logger.warning(f'{self.__class__.__name__} is stopping')

    def _kill(self):
        self.is_run = False
        for name, thread in self._threads.items():
            if not thread or not thread.is_alive():
                continue
            thread_id = ctypes.c_long(thread.ident)
            res = ctypes.pythonapi.PyThreadState_SetAsyncExc(thread_id, ctypes.py_object(SystemExit))
            if res == 0:
                raise ValueError('Nonexistent thread id')
            elif res > 1:
                ctypes.pythonapi.PyThreadState_SetAsyncExc(thread_id, 0)
                raise SystemError('PyThreadState_SetAsyncExc failed')
            logger.info(f'Thread {thread} is killed')


class WebSocketBinaceAskBid(WebSocketOkxAskBid):
    def __init__(self) -> None:
        super().__init__()
        self.url = 'wss://fstream.binance.com/ws'
        self._inst_id_field_path = 'symbols__symbol'
        self._threads = dict(run_forever_binance_ask_bid=None, monitor_binance_ask_bid=None)

    def subscribe_inst_id(self, inst_ids: list[str]) -> dict:
        for inst_id in inst_ids:
            d = {
                'method': 'SUBSCRIBE',
                'params': [f'{inst_id.lower()}@bookTicker'],
                'id': 1
            }
            self.ws.send(json.dumps(d))
            time.sleep(0.2)
            self._subscribed_inst_ids.append(inst_id)
            logger.info(f'Subscribed to {inst_id=}')

    def unsubscribe_inst_id(self, inst_ids: list[str]) -> dict:
        for inst_id in inst_ids:
            d = {
                'method': 'UNSUBSCRIBE',
                'params': [f'{inst_id.lower()}@bookTicker'],
                'id': 1
            }
            self.ws.send(json.dumps(d))
            time.sleep(0.2)
            if inst_id in self._subscribed_inst_ids:
                self._subscribed_inst_ids.remove(inst_id)
            logger.info(f'Unsubscribed from {inst_id=}')

    def _message_handler(self, message: str) -> None | dict:
        try:
            message = json.loads(message)
        except json.decoder.JSONDecodeError:
            logger.error(f'Can not decode message. {message=}')
            return
        if 'result' in message and message['result'] is None:
            logger.debug('Empty result')
            return
        keys = set(['s', 'b', 'a', 'E'])
        if not set(message.keys()).issuperset(keys):
            logger.error(f'Not all keys are in message. {message=}')
            return
        previous_ask_bid = self.previous_ask_bid.get(message['s'])
        if previous_ask_bid:
            if previous_ask_bid[0] == message['a'] and previous_ask_bid[1] == message['b']:
                return
        self.previous_ask_bid[message['s']] = [message['a'], message['b']]
        return {k: v for k, v in message.items() if k in keys}

    def launch(self):
        try:
            run_forever_thread = threading.Thread(
                target=self.run_forever, daemon=True, name='run_forever_binance_ask_bid')
            run_forever_thread.start()
            self._threads[run_forever_thread.name] = run_forever_thread
            logger.info(f'{self.__class__.__name__} is started')
            monitor_thread = threading.Thread(
                target=self.monitoring_inst_ids, daemon=True, name='monitor_binance_ask_bid')
            monitor_thread.start()
            self._threads[monitor_thread.name] = monitor_thread
            logger.info('Monitoring thread is started')
        except Exception as e:
            logger.exception(e)
            raise


class WebSocketOkxMarketPrice(WebSocketOkxAskBid):
    def __init__(self) -> None:
        super().__init__()
        self._threads = dict(
            run_forever_okx_market_price=None, ping_okx_market_price=None, monitor_okx_market_price=None)
        self.previous_market_price: dict[str, float] = {}

    def subscribe_inst_id(self, inst_ids: list[str]) -> dict:
        for inst_id in inst_ids:
            d = {
                'op': 'subscribe',
                'args': [{
                    'channel': 'mark-price',
                    'instId': inst_id
                }]
            }
            self.ws.send(json.dumps(d))
            time.sleep(0.2)
            self._subscribed_inst_ids.append(inst_id)
            logger.info(f'Subscribed to {inst_id=}')

    def unsubscribe_inst_id(self, inst_ids: list[str]) -> dict:
        for inst_id in inst_ids:
            d = {
                'op': 'unsubscribe',
                'args': [{
                    'channel': 'mark-price',
                    'instId': inst_id
                }]
            }
            self.ws.send(json.dumps(d))
            time.sleep(0.2)
            if inst_id in self._subscribed_inst_ids:
                self._subscribed_inst_ids.remove(inst_id)
            logger.info(f'Unsubscribed from {inst_id=}')

    def _message_handler(self, message: str) -> None | dict:
        if message == 'pong':
            logger.debug(f'{self.__class__.__name__} pong received')
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
        elif event == 'unsubscribe':
            logger.info(f'Unsubscribe {message}')
        elif data:
            data = data[0]
            previous_market_price = self.previous_market_price.get(data['instId'])
            if previous_market_price:
                if previous_market_price == data['markPx']:
                    return
            self.previous_market_price[data['instId']] = data['markPx']
            return {k: v for k, v in data.items() if k in ['instId', 'markPx', 'ts']}


class WebSocketOkxOrders(WebSocketOkxAskBid):
    def __init__(self, account: Account) -> None:
        super().__init__()
        self.account = account
        production_url = 'wss://ws.okx.com:8443/ws/v5/private'
        demo_trading_url = 'wss://wspap.okx.com:8443/ws/v5/private?brokerId=9999'
        self.url = demo_trading_url if account.testnet else production_url
        self._threads = {
            f'run_forever_okx_orders_{account.id}': None,
            f'ping_okx_orders_{account.id}': None
        }

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

    def _login(self):
        self.ws.send(json.dumps(self._get_login_subscribe()))

    def _subscribe(self):
        self.ws.send(json.dumps(
            {
                'op': 'subscribe',
                'args': [{
                    'channel': 'orders',
                    'instType': 'SWAP'
                }]
            }
        ))
        logger.info('Subscribed to orders')

    def _message_handler(self, message: str) -> None | dict:
        if message == 'pong':
            logger.debug(f'{self.__class__.__name__} pong received')
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
            self._subscribe()
        elif data:
            data = data[0]
            data['account_id'] = self.account.id
            return data

    def init(self):
        self._connect()
        self._login()

    def launch(self):
        names = list(self._threads.keys())
        run_forever_thread = threading.Thread(
            target=self.run_forever, daemon=True, name=names[0])
        run_forever_thread.start()
        self._threads[run_forever_thread.name] = run_forever_thread
        ping_thread = threading.Thread(target=self.ping, daemon=True, name=names[1])
        ping_thread.start()
        self._threads[ping_thread.name] = ping_thread
        logger.info(f'{self.__class__.__name__} for {self.account.name} is started')


class WebSocketOkxPositions(WebSocketOkxOrders):
    def __init__(self, account: Account) -> None:
        super().__init__(account)
        self._threads = {
            f'run_forever_okx_positions_{account.id}': None,
            f'ping_okx_positions_{account.id}': None
        }
        self.previous_positions: dict[str, dict[str, Any]] = {}

    def _subscribe(self):
        self.ws.send(json.dumps(
            {
                'op': 'subscribe',
                'args': [{
                    'channel': 'positions',
                    'instType': 'SWAP'
                }]
            }
        ))
        logger.info('Subscribed to positions')

    def _message_handler(self, message: str) -> None | dict:
        if message == 'pong':
            logger.debug(f'{self.__class__.__name__} pong received')
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
            self._subscribe()
        elif data:
            data = data[0]
            data['account_id'] = self.account.id
            key = f'{self.account.id}_{data["instId"]}'
            previous_position = self.previous_positions.get(key)
            if previous_position:
                if previous_position['pos'] == data['pos'] and previous_position['cTime'] == data['cTime']:
                    return
            self.previous_positions[key] = data
            return data
