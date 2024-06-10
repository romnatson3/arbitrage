import logging
import websocket
from websocket._exceptions import WebSocketConnectionClosedException, WebSocketException
import json
import base64
import hmac
import ctypes
import threading
import time
from typing import Callable
from .models import Account


logger = logging.getLogger(__name__)


class WebSocketOrders():
    def __new__(cls, *args, **kwargs):
        if not hasattr(cls, 'instance'):
            cls.instance = super().__new__(cls)
        return cls.instance

    def __init__(self, account: Account) -> None:
        self.is_run = False
        self.ws = websocket.WebSocket()
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
