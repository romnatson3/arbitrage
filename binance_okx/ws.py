import logging
import websocket
from datetime import datetime
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
    def __init__(self, **kwargs) -> None:
        self.is_run = False
        self.ws = websocket.WebSocket()
        if settings.OKX_FLAG == '0':
            self.url = 'wss://ws.okx.com:8443/ws/v5/public'  # Production
        else:
            self.url = 'wss://wspap.okx.com:8443/ws/v5/public'  # Testnet
        self.handlers = []
        self.subscribed_inst_ids = []
        self._inst_id_field_path = 'symbols__okx__data__instId'
        self._previous_ask_bid: dict[str, list[float]] = {}
        self.methods_names = kwargs.get('methods_names', ['run_forever', 'ping', 'monitoring_inst_ids'])
        self.threads: dict = self._get_threads_names()

    def _get_threads_names(self) -> dict:
        threads = {}
        self.name: str = self.__class__.__name__
        for method in self.methods_names:
            if hasattr(self, method):
                threads[f'{method}_{self.name}'] = None
        return threads

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
                subscribed_inst_ids = set(self.subscribed_inst_ids)
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
            self.subscribed_inst_ids.append(inst_id)
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
            if inst_id in self.subscribed_inst_ids:
                self.subscribed_inst_ids.remove(inst_id)
            logger.info(f'Unsubscribed from {inst_id=}')

    def _connect(self):
        self.ws.connect(self.url)
        logger.info(f'{self.name} connected to {self.url}')

    def add_handler(self, callback: Callable[[int, dict], None]) -> None:
        self.handlers.append(callback)

    def ping(self):
        while self.is_run:
            try:
                if not self.ws.connected:
                    logger.debug(f'{self.name} ping not started. Socket is not connected')
                    continue
                self.ws.send('ping')
                logger.debug(f'{self.name} ping sent')
            except Exception as e:
                logger.error(f'{self.name} ping error: {e}')
                continue
            finally:
                time.sleep(25)
        else:
            logger.debug('Ping stopped')

    def _message_handler(self, message: str) -> None | dict:
        if message == 'pong':
            logger.debug(f'{self.name} pong received')
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
            return data[0]

    def _post_message_handler(self, data: dict) -> None | dict:
        previous_ask_bid = self._previous_ask_bid.get(data['instId'])
        if previous_ask_bid:
            if previous_ask_bid[0] == data['askPx'] and previous_ask_bid[1] == data['bidPx']:
                return
        self._previous_ask_bid[data['instId']] = [data['askPx'], data['bidPx']]
        keys = ['instId', 'askPx', 'askSz', 'bidPx', 'bidSz', 'ts']
        data = {k: v for k, v in data.items() if k in keys}
        return data

    def init(self):
        self._connect()
        self.subscribed_inst_ids = []

    def run_forever(self) -> None:
        while self.is_run:
            try:
                self.init()
                while self.is_run:
                    try:
                        message = self.ws.recv()
                        data = self._message_handler(message)
                        if data:
                            data = self._post_message_handler(data)
                            if data:
                                logger.debug(data)
                                for handler in self.handlers:
                                    handler(data)
                    except WebSocketPayloadException as e:
                        logger.error(e)
                    except WebSocketException:
                        raise
                    except Exception as e:
                        logger.exception(e)
            except WebSocketConnectionClosedException:
                logger.warning(f'{self.name} connection closed')
            except WebSocketException as e:
                logger.exception(e)
                self.ws.close()
            finally:
                time.sleep(3)
        else:
            self.ws.close()
            logger.info(f'{self.name} is stopped')

    def launch(self):
        try:
            for method in self.methods_names:
                if hasattr(self, method):
                    target = getattr(self, method)
                    for name in self.threads:
                        if method in name:
                            break
                    else:
                        name = f'{method}_{self.name}'
                    thread = threading.Thread(target=target, name=name, daemon=True)
                    thread.start()
                    logger.info(f'Thread {thread} is started')
                    self.threads[name] = thread
        except Exception as e:
            logger.exception(e)
            raise

    def start(self):
        if self.is_run:
            logger.warning(f'{self.name} is already running')
            return
        self.is_run = True
        self.launch()

    def stop(self):
        self.is_run = False
        logger.warning(f'{self.name} is stopping')

    def kill(self):
        for thread in self.threads.values():
            if not thread or not thread.is_alive():
                continue
            thread_id = ctypes.c_long(thread.ident)
            res = ctypes.pythonapi.PyThreadState_SetAsyncExc(thread_id, ctypes.py_object(SystemExit))
            if res == 0:
                raise ValueError('Nonexistent thread id')
            elif res > 1:
                ctypes.pythonapi.PyThreadState_SetAsyncExc(thread_id, 0)
                raise SystemError('PyThreadState_SetAsyncExc failed')
            elif res == 1:
                logger.info(f'Thread {thread} is killed')
        self.is_run = False
        self.ws.close()


class WebSocketOkxLastPrice(WebSocketOkxAskBid):
    def __init__(self, **kwargs) -> None:
        kwargs['methods_names'] = ['run_forever', 'ping', 'monitoring_inst_ids']
        super().__init__(**kwargs)
        self._previous_last_price: dict[str, float] = {}

    def _post_message_handler(self, data: dict) -> None | dict:
        previous_last_price = self._previous_last_price.get(data['instId'])
        if previous_last_price:
            if previous_last_price[0] == data['last'] and previous_last_price[1] == data['lastSz']:
                return
        self._previous_last_price[data['instId']] = [data['last'], data['lastSz']]
        keys = ['instId', 'last', 'lastSz', 'ts']
        data = {k: v for k, v in data.items() if k in keys}
        return data


class WebSocketBinaceAskBid(WebSocketOkxAskBid):
    def __init__(self, **kwargs) -> None:
        kwargs['methods_names'] = ['run_forever', 'monitoring_inst_ids']
        super().__init__(**kwargs)
        self.url = 'wss://fstream.binance.com/ws'
        self._inst_id_field_path = 'symbols__symbol'

    def subscribe_inst_id(self, inst_ids: list[str]) -> dict:
        for inst_id in inst_ids:
            d = {
                'method': 'SUBSCRIBE',
                'params': [f'{inst_id.lower()}@bookTicker'],
                'id': 1
            }
            self.ws.send(json.dumps(d))
            time.sleep(0.2)
            self.subscribed_inst_ids.append(inst_id)
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
            if inst_id in self.subscribed_inst_ids:
                self.subscribed_inst_ids.remove(inst_id)
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
        return message

    def _post_message_handler(self, message: dict) -> None | dict:
        previous_ask_bid = self._previous_ask_bid.get(message['s'])
        if previous_ask_bid:
            if previous_ask_bid[0] == message['a'] and previous_ask_bid[1] == message['b']:
                return
        self._previous_ask_bid[message['s']] = [message['a'], message['b']]
        keys = ['s', 'b', 'B', 'a', 'A', 'E']
        return {k: v for k, v in message.items() if k in keys}


class WebSocketOkxMarketPrice(WebSocketOkxAskBid):
    def __init__(self, **kwargs) -> None:
        kwargs['methods_names'] = ['run_forever', 'ping', 'monitoring_inst_ids']
        super().__init__(**kwargs)
        self._previous_market_price: dict[str, float] = {}

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
            self.subscribed_inst_ids.append(inst_id)
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
            if inst_id in self.subscribed_inst_ids:
                self.subscribed_inst_ids.remove(inst_id)
            logger.info(f'Unsubscribed from {inst_id=}')

    def _post_message_handler(self, data: dict) -> None | dict:
        previous_market_price = self._previous_market_price.get(data['instId'])
        if previous_market_price:
            if previous_market_price == data['markPx']:
                return
        self._previous_market_price[data['instId']] = data['markPx']
        date_time = datetime.fromtimestamp(int(data['ts']) / 1000).strftime('%d-%m-%Y %H:%M:%S.%f')[:-3]
        data['date_time'] = date_time
        keys = ['instId', 'markPx', 'ts', 'date_time']
        return {k: v for k, v in data.items() if k in keys}


class WebSocketOkxOrders(WebSocketOkxAskBid):
    def __init__(self, **kwargs) -> None:
        kwargs['methods_names'] = ['run_forever', 'ping']
        self.account: Account = kwargs['account']
        super().__init__(**kwargs)
        if settings.OKX_FLAG == '0':
            self.url = 'wss://ws.okx.com:8443/ws/v5/private'  # Production
        else:
            self.url = 'wss://wspap.okx.com:8443/ws/v5/private?brokerId=9999'  # Testnet

    def _get_threads_names(self) -> dict:
        threads = {}
        self.name: str = self.__class__.__name__
        for method in self.methods_names:
            if hasattr(self, method):
                threads[f'{method}_{self.name}_{self.account.id}'] = None
        return threads

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
            return data[0]

    def _post_message_handler(self, data: dict) -> None | dict:
        data['account_id'] = self.account.id
        return data

    def init(self):
        self._connect()
        self._login()


class WebSocketOkxPositions(WebSocketOkxOrders):
    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)
        self._previous_positions: dict[str, dict[str, Any]] = {}

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

    def _post_message_handler(self, data: dict) -> None | dict:
        data['account_id'] = self.account.id
        key = f'{self.account.id}_{data["instId"]}'
        previous_position = self._previous_positions.get(key)
        if previous_position:
            if previous_position['pos'] == data['pos'] and previous_position['cTime'] == data['cTime']:
                return
        self._previous_positions[key] = data
        return data
