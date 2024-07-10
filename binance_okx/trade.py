import logging
import time
import uuid
from typing import Any
from django.utils import timezone
from django.conf import settings
import okx.Trade as Trade
import okx.Account as Account
from .models import Strategy, Symbol, OkxSymbol, Execution, Position
from .exceptions import (
    PlaceOrderException, GetPositionException, GetOrderException,
    CancelOrderException, ClosePositionException, GetExecutionException
)
from .misc import convert_dict_values
from .helper import calc, get_ask_bid_prices_from_cache_by_symbol


logger = logging.getLogger(__name__)


class OkxTrade():
    def __init__(self, strategy: Strategy, symbol: Symbol, position_side: str, debug=False) -> None:
        strategy._extra_log.update(symbol=symbol.symbol)
        self.strategy = strategy
        self.symbol_okx = symbol.okx
        self.symbol = symbol
        self.position_side = position_side
        apikey = strategy.second_account.api_key
        secretkey = strategy.second_account.api_secret
        passphrase = strategy.second_account.api_passphrase
        flag = '1' if strategy.second_account.testnet else '0'
        self.trade = Trade.TradeAPI(apikey, secretkey, passphrase, flag=flag, debug=debug)
        self.account = Account.AccountAPI(apikey, secretkey, passphrase, flag=flag, debug=debug)

    def open_position(
        self,
        position_size: float = None,
        symbol: OkxSymbol = None,
        position_side: str = None
    ) -> Position:
        if not symbol:
            symbol = self.symbol_okx
        if not position_size:
            position_size = self.strategy.position_size
        if not position_side:
            position_side = self.position_side
        order_id = self.place_order(position_size, symbol, position_side)
        logger.warning(
            f'Opened {position_side} position, {position_size=}, {order_id=}',
            extra=self.strategy.extra_log
        )
        position: dict = self.get_position()
        position: Position = self.save_position(position)
        executions: list[dict] = self.get_executions_by_order_id(order_id)
        for execution in executions:
            OkxTrade.save_execution(execution, position)
        return position

    def place_order(self, position_size: float, symbol: OkxSymbol, position_side: str) -> str:
        sz = calc.get_sz(position_size, symbol)
        if position_side == 'long':
            side = 'buy'
        if position_side == 'short':
            side = 'sell'
        result = self.trade.place_order(
            instId=symbol.inst_id,
            ordType='market',
            tdMode='isolated',
            posSide=position_side,
            side=side,
            sz=sz
        )
        if result['code'] != '0':
            raise PlaceOrderException(result)
        order_id = result['data'][0]['ordId']
        logger.info(f'Placed order {sz=} {order_id=}', extra=self.strategy.extra_log)
        return order_id

    def increase_position(self):
        symbol = self.symbol_okx
        position_size = self.strategy.position_size
        position_side = self.position_side
        order_id = self.place_order(position_size, symbol, position_side)
        logger.warning(
            f'Increased {position_side} position, {position_size=}, {order_id=}',
            extra=self.strategy.extra_log
        )

    def save_position(self, data: dict) -> Position:
        position = Position.objects.filter(
            position_data__posId=data['posId'], strategy=self.strategy,
            symbol=self.symbol, is_open=True
        ).last()
        if position:
            self.strategy._extra_log.update(position=position.id)
            logger.warning('Position already exists', extra=self.strategy.extra_log)
            position.position_data = data
            position.save()
            logger.debug('Updated position', extra=self.strategy.extra_log)
            return position
        position = Position.objects.create(position_data=data, strategy=self.strategy, symbol=self.symbol)
        self.strategy._extra_log.update(position=position.id)
        logger.info('Saved position', extra=self.strategy.extra_log)
        return position

    def get_executions_by_order_id(self, order_id: int) -> list[dict[str, Any]]:
        if not isinstance(order_id, int):
            order_id = int(order_id)
        end_time = time.time() + settings.RECEIVE_TIMEOUT
        while time.time() < end_time:
            logger.debug(f'Trying to get executions for {order_id=}', extra=self.strategy.extra_log)
            result = list(
                self.strategy.second_account.bills
                .filter(data__ordId=order_id).values_list('data', flat=True)
            )
            if result:
                logger.info(
                    f'Got {len(result)} executions for {order_id=}',
                    extra=self.strategy.extra_log
                )
                return result
            time.sleep(2)
        raise GetExecutionException(
            f'Not found any executions for {order_id=}. Timeout {settings.RECEIVE_TIMEOUT}s reached'
        )

    @staticmethod
    def save_execution(data: dict, position: Position) -> None:
        execution, created = Execution.objects.get_or_create(
            bill_id=data['billId'], trade_id=data['tradeId'],
            defaults={'data': data, 'position': position}
        )
        if created:
            logger.info(
                f'Saved execution bill_id={data["billId"]}, trade_id={data["tradeId"]}, '
                f'subType={data["subType"]}, sz={data["sz"]}, px={data["px"]}, ordId={data["ordId"]}',
                extra=position.strategy.extra_log
            )
        else:
            logger.warning(
                f'Execution bill_id={data["billId"]}, trade_id={data["tradeId"]} already exists',
                extra=position.strategy.extra_log
            )

    def close_position(self, size_usdt: float, symbol: OkxSymbol = None, position_side: str = None) -> None:
        if not symbol:
            symbol = self.symbol_okx
        sz = calc.get_sz(size_usdt, symbol)
        if not position_side:
            position_side = self.position_side
        if position_side == 'long':
            side = 'sell'
        if position_side == 'short':
            side = 'buy'
        result = self.trade.place_order(
            instId=symbol.inst_id,
            ordType='market',
            tdMode='isolated',
            posSide=position_side,
            side=side,
            sz=sz
        )
        if result['code'] != '0':
            raise PlaceOrderException(result)
        order_id = result['data'][0]['ordId']
        logger.warning(
            f'Closed {position_side} position partially {size_usdt=} {order_id=}',
            extra=self.strategy.extra_log
        )

    def close_entire_position(self, symbol: OkxSymbol = None, position_side: str = None) -> None:
        try:
            self.get_position(symbol=symbol, wait=0)
        except GetPositionException:
            logger.warning('No position to close', extra=self.strategy.extra_log)
            return
        if not symbol:
            symbol = self.symbol_okx
        if not position_side:
            position_side = self.position_side
        orders = self.get_order_list()
        for order in orders:
            self.cancel_order(symbol, order['ordId'])
        result = self.trade.close_positions(
            instId=symbol.inst_id,
            posSide=position_side,
            mgnMode='isolated'
        )
        if result['code'] != '0':
            raise ClosePositionException(f'Failed to close {position_side} position. {result}')
        else:
            logger.warning(f'Closed {position_side} position', extra=self.strategy.extra_log)

    def get_position(self, symbol: OkxSymbol = None, wait: int = 10) -> dict:
        if not symbol:
            symbol = self.symbol_okx
        if wait <= 0:
            wait = 1
        end_time = time.time() + wait
        while time.time() < end_time:
            logger.debug('Trying to get position data', extra=self.strategy.extra_log)
            result = self.account.get_positions(instId=symbol.inst_id, instType='SWAP')
            if result['code'] != '0':
                raise GetPositionException(f'Failed to get position data. {result}')
            if not result['data']:
                time.sleep(1)
                continue
            data = convert_dict_values(result['data'][0])
            if data['pos']:
                logger.info(
                    f'Got position data: side={data["posSide"]}, sz={data["pos"]}, '
                    f'notionalUsd={data["notionalUsd"]}, avgPx={data["avgPx"]}',
                    extra=self.strategy.extra_log
                )
                return data
            time.sleep(1)
        raise GetPositionException(f'Failed to get position data. Timeout {wait}s reached')

    def place_stop_loss(
        self,
        price: float,
        symbol: OkxSymbol = None,
        position_side: str = None,
        sz: int = None
    ) -> str:
        if not symbol:
            symbol = self.symbol_okx
        if not position_side:
            position_side = self.position_side
        if position_side == 'long':
            side = 'sell'
        if position_side == 'short':
            side = 'buy'
        parameters = dict(
            instId=symbol.inst_id,
            tdMode='isolated',
            side=side,
            posSide=position_side,
            closeFraction=1,
            ordType='conditional',
            slTriggerPx=price,
            slOrdPx=-1,
            slTriggerPxType='mark',
            sz=sz
        )
        if sz:
            parameters.pop('closeFraction')
        else:
            parameters.pop('sz')
        result = self.trade.place_algo_order(**parameters)
        if result['code'] != '0':
            raise PlaceOrderException(result)
        order_id = result['data'][0]['algoId']
        logger.info(f'Placed stop loss {price=} {sz=} {order_id=}', extra=self.strategy.extra_log)
        return order_id

    def place_take_profit(self, price: float, symbol: OkxSymbol = None, position_side: str = None) -> str:
        if not symbol:
            symbol = self.symbol_okx
        if not position_side:
            position_side = self.position_side
        if position_side == 'long':
            side = 'sell'
        if position_side == 'short':
            side = 'buy'
        result = self.trade.place_algo_order(
            instId=symbol.inst_id,
            tdMode='isolated',
            side=side,
            posSide=position_side,
            closeFraction=1,
            ordType='conditional',
            tpTriggerPx=price,
            tpOrdPx=-1,
            tpTriggerPxType='mark'
        )
        if result['code'] != '0':
            raise PlaceOrderException(result)
        order_id = result['data'][0]['algoId']
        logger.info(f'Placed take profit {price} {order_id=}', extra=self.strategy.extra_log)
        return order_id

    def place_stop_loss_and_take_profit(
        self, stop_loss_price: float, take_profit_price: float,
        symbol: OkxSymbol = None, position_side: str = None
    ) -> str:
        if not symbol:
            symbol = self.symbol_okx
        if not position_side:
            position_side = self.position_side
        if position_side == 'long':
            side = 'sell'
        if position_side == 'short':
            side = 'buy'
        result = self.trade.place_algo_order(
            instId=symbol.inst_id,
            tdMode='isolated',
            side=side,
            posSide=position_side,
            closeFraction=1,
            ordType='oco',
            slTriggerPx=stop_loss_price,
            slOrdPx=-1,
            slTriggerPxType='mark',
            tpTriggerPx=take_profit_price,
            tpOrdPx=-1,
            tpTriggerPxType='mark'
        )
        if result['code'] != '0':
            raise PlaceOrderException(result)
        order_id = result['data'][0]['algoId']
        logger.info(
            f'Placed {stop_loss_price=} {take_profit_price=} {order_id=}',
            extra=self.strategy.extra_log
        )
        return order_id

    def place_limit_order(
        self,
        price: float,
        position_size: float = None,
        symbol: OkxSymbol = None,
        position_side: str = None
    ) -> str:
        if not symbol:
            symbol = self.symbol_okx
        if not position_size:
            position_size = self.strategy.position_size
        sz = calc.get_sz(position_size, symbol)
        if not position_side:
            position_side = self.position_side
        if position_side == 'long':
            side = 'sell'
        if position_side == 'short':
            side = 'buy'
        result = self.trade.place_order(
            instId=symbol.inst_id,
            ordType='limit',
            tdMode='isolated',
            posSide=position_side,
            side=side,
            sz=sz,
            px=price
        )
        if result['code'] != '0':
            raise PlaceOrderException(result)
        order_id = result['data'][0]['ordId']
        logger.info(f'Placed limit order {sz=} {price=} {order_id=}', extra=self.strategy.extra_log)
        return order_id

    def get_order_list(self, symbol: OkxSymbol = None) -> list:
        if not symbol:
            symbol = self.symbol_okx
        result = self.trade.get_order_list(instId=symbol.inst_id)
        if result['code'] != '0':
            raise GetOrderException(result)
        orders = []
        for order in result['data']:
            orders.append(convert_dict_values(order))
        orders_ids = [order['ordId'] for order in orders]
        logger.info(f'Got {len(orders)} orders, {orders_ids=}', extra=self.strategy.extra_log)
        return orders

    def get_order(self, symbol: OkxSymbol, order_id: str) -> dict:
        result = self.trade.get_order(instId=symbol.inst_id, ordId=order_id)
        if result['code'] != '0':
            raise GetOrderException(result)
        return convert_dict_values(result['data'][0])

    def cancel_order(self, symbol: OkxSymbol, order_id: str) -> None:
        result = self.trade.cancel_order(instId=symbol.inst_id, ordId=order_id)
        if result['code'] != '0':
            raise CancelOrderException(result)
        else:
            logger.info(f'Cancelled {order_id=}', extra=self.strategy.extra_log)

    def get_algo_order_id(self, symbol: OkxSymbol = None) -> list:
        if not symbol:
            symbol = self.symbol_okx
        result = self.trade.order_algos_list(instId=symbol.inst_id, ordType='conditional')
        if result['code'] != '0':
            raise GetOrderException(result)
        if not result['data']:
            result = self.trade.order_algos_list(instId=symbol.inst_id, ordType='oco')
            if result['code'] != '0':
                raise GetOrderException(result)
        if result['data']:
            return int(result['data'][0]['algoId'])

    def update_stop_loss(
        self,
        price: float,
        symbol: OkxSymbol = None,
        position_side: str = None,
        sz: int = None
    ) -> None:
        if not symbol:
            symbol = self.symbol_okx
        if not position_side:
            position_side = self.position_side
        algo_id = self.get_algo_order_id()
        if not algo_id:
            algo_id = self.place_stop_loss(price, symbol, position_side, sz)
            return
        parameters = dict(
            instId=symbol.inst_id,
            algoId=algo_id,
            newSlTriggerPx=price,
            newSlOrdPx=-1,
            newSlTriggerPxType='mark',
            newSz=sz
        )
        if not sz:
            parameters.pop('newSz')
        result = self.trade.amend_algo_order(**parameters)
        if result['code'] != '0':
            raise PlaceOrderException(result)
        logger.info(f'Updated stop loss {price=} {sz=}', extra=self.strategy.extra_log)

    def update_take_profit(self, price: float, symbol: OkxSymbol = None, position_side: str = None) -> None:
        if not symbol:
            symbol = self.symbol_okx
        if not position_side:
            position_side = self.position_side
        algo_id = self.get_algo_order_id()
        if not algo_id:
            algo_id = self.place_take_profit(price, symbol, position_side)
            return
        result = self.trade.amend_algo_order(
            instId=symbol.inst_id,
            algoId=algo_id,
            newTpTriggerPx=price,
            newTpOrdPx=-1,
            newTpTriggerPxType='mark'
        )
        if result['code'] != '0':
            raise PlaceOrderException(result)
        logger.info(f'Updated take profit {price=}', extra=self.strategy.extra_log)


class OkxEmulateTrade():
    def __init__(self, strategy: Strategy, symbol: Symbol) -> None:
        strategy._extra_log.update(symbol=symbol.symbol)
        self.strategy = strategy
        self.symbol = symbol

    def create_position(self, position_side: str) -> Position:
        logger.warning(
            f'Creating virtual {position_side} position, size {self.strategy.position_size} usdt',
            extra=self.strategy.extra_log
        )
        position_data = Position.get_position_empty_data()
        position_data.update(
            posSide=position_side,
            cTime=timezone.localtime().strftime('%d-%m-%Y %H:%M:%S.%f'),
            pos=calc.get_sz(self.strategy.position_size, self.symbol.okx),
            avgPx=self.symbol.okx.market_price,
            notionalUsd=self.strategy.position_size,
        )
        position = Position.objects.create(
            strategy=self.strategy, symbol=self.symbol, mode=Strategy.Mode.emulate,
            position_data=position_data
        )
        self.strategy._extra_log.update(position=position.id)
        logger.info('Created virtual position', extra=self.strategy.extra_log)
        self._create_open_execution(position)
        return position

    def close_position(self, position: Position, size_usdt: float) -> None:
        sz = calc.get_sz(size_usdt, self.symbol.okx)
        if sz >= position.position_data['pos']:
            sz = position.position_data['pos']
            base_coin = calc.get_base_coin_from_sz(sz, self.symbol.okx.ct_val)
            size_usdt = base_coin * self.symbol.okx.market_price
            position.is_open = False
            position.save(update_fields=['is_open'])
            logger.warning(
                f'Virtual position is closed completely {size_usdt=}',
                extra=self.strategy.extra_log
            )
        else:
            logger.warning(
                f'Virtual position is closed partially {size_usdt=}',
                extra=self.strategy.extra_log
            )
            position.position_data['pos'] -= sz
            position.save(update_fields=['position_data'])
        self._create_close_execution(position, size_usdt)

    def _create_open_execution(self, position: Position) -> None:
        execution_data = Execution.get_empty_data()
        execution_data.update(
            subType='Open long' if position.side == 'long' else 'Open short',
            sz=position.position_data['pos'],
            px=position.position_data['avgPx'],
            ts=timezone.localtime().strftime('%d-%m-%Y %H:%M:%S.%f'),
            fee=round(self.strategy.position_size * self.strategy.open_fee / 100, 10),
            pnl=None
        )
        Execution.objects.create(
            position=position, trade_id=str(uuid.uuid4()).split('-')[-1],
            bill_id=str(uuid.uuid4()).split('-')[-1], data=execution_data
        )
        logger.info(
            f'Created virtual {execution_data["subType"]} execution, '
            f'sz={execution_data["sz"]}, px={execution_data["px"]}, '
            f'fee={execution_data["fee"]}',
            extra=self.strategy.extra_log
        )

    def _create_close_execution(self, position: Position, size_usdt: float) -> None:
        sub_type = 'Open long' if position.side == 'long' else 'Open short'
        open_execution = position.executions.filter(data__subType=sub_type).first()
        open_fee = open_execution.data['fee']
        open_price = open_execution.data['px']
        sz = calc.get_sz(size_usdt, self.symbol.okx)
        base_coin = calc.get_base_coin_from_sz(sz, self.symbol.okx.ct_val)
        close_price = self.symbol.okx.market_price
        close_fee = round(size_usdt * self.strategy.close_fee / 100, 10)
        pnl = round(
            (close_price - open_price) * base_coin - (open_fee + close_fee),
            10
        )
        execution_data = Execution.get_empty_data()
        execution_data.update(
            subType='Close long' if position.side == 'long' else 'Close short',
            sz=sz,
            px=close_price,
            ts=timezone.localtime().strftime('%d-%m-%Y %H:%M:%S.%f'),
            fee=close_fee,
            pnl=pnl
        )
        Execution.objects.create(
            position=position, trade_id=str(uuid.uuid4()).split('-')[-1],
            bill_id=str(uuid.uuid4()).split('-')[-1], data=execution_data
        )
        logger.info(
            f'Created virtual {execution_data["subType"]} execution, '
            f'sz={execution_data["sz"]}, px={execution_data["px"]}, '
            f'fee={execution_data["fee"]}, pnl={execution_data["pnl"]}',
            extra=self.strategy.extra_log
        )


def get_take_profit_grid(strategy: Strategy, entry_price: float, spread_percent: float, position_side: str):
    if position_side == 'long':
        tp_first_price = (
            entry_price * (1 + (strategy.tp_first_price_percent + strategy.open_plus_close_fee + spread_percent) / 100)
        )
        tp_second_price = (
            entry_price * (1 + (strategy.tp_second_price_percent + strategy.open_plus_close_fee + spread_percent) / 100)
        )
    if position_side == 'short':
        tp_first_price = (
            entry_price * (1 - (strategy.tp_first_price_percent + strategy.open_plus_close_fee + spread_percent) / 100)
        )
        tp_second_price = (
            entry_price * (1 - (strategy.tp_second_price_percent + strategy.open_plus_close_fee + spread_percent) / 100)
        )
    tp_first_amount_without_fee = (
        strategy.position_size / 100 * strategy.tp_first_part_percent * (1 + (strategy.tp_first_price_percent + spread_percent) / 100)
    )
    tp_second_amount_without_fee = (
        strategy.position_size / 100 * strategy.tp_second_part_percent * (1 + (strategy.tp_second_price_percent + spread_percent) / 100)
    )
    tp_first_amount_with_fee = tp_first_amount_without_fee * (1 + strategy.open_plus_close_fee / 100)
    tp_second_amount_with_fee = tp_second_amount_without_fee * (1 + strategy.open_plus_close_fee / 100)
    return dict(
        tp_first_price=round(tp_first_price, 10),
        tp_first_part=round(tp_first_amount_with_fee, 2),
        tp_second_price=round(tp_second_price, 10),
        tp_second_part=round(tp_second_amount_with_fee, 2)
    )


def get_stop_loss_breakeven(entry_price: float, fee_percent: float, spread_percent: float, position_side: str):
    if position_side == 'long':
        stop_loss_price = (
            entry_price * (1 + (fee_percent + spread_percent) / 100)
        )
    if position_side == 'short':
        stop_loss_price = (
            entry_price * (1 - (fee_percent + spread_percent) / 100)
        )
    return round(stop_loss_price, 5)


def get_ask_bid_prices_and_condition(strategy: Strategy, symbol: Symbol) -> tuple[bool, str, dict]:
    delta_percent = None
    delta_points = None
    spread_points = None
    condition_met = False
    min_delta_percent = None
    prices = get_ask_bid_prices_from_cache_by_symbol(strategy, symbol.symbol)
    binance_previous_ask = prices['binance_previous_ask']
    binance_last_ask = prices['binance_last_ask']
    binance_previous_bid = prices['binance_previous_bid']
    binance_last_bid = prices['binance_last_bid']
    okx_previous_ask = prices['okx_previous_ask']
    okx_last_ask = prices['okx_last_ask']
    okx_previous_bid = prices['okx_previous_bid']
    okx_last_bid = prices['okx_last_bid']
    position_side = prices['position_side']
    date_time_last_prices = prices['date_time_last_prices']
    # logger.debug(
    #     f'{binance_previous_ask=}, {binance_last_ask=}, '
    #     f'{binance_previous_bid=}, {binance_last_bid=} '
    #     f'{okx_previous_ask=}, {okx_last_ask=}, '
    #     f'{okx_previous_bid=}, {okx_last_bid=}',
    #     extra=strategy.extra_log
    # )
    if not all([
        binance_previous_ask, binance_last_ask, binance_previous_bid, binance_last_bid,
        okx_previous_ask, okx_last_ask, okx_previous_bid, okx_last_bid
    ]):
        logger.debug('Not all prices are available', extra=strategy.extra_log)
        return condition_met, position_side, dict()
    if position_side == 'long':
        logger.debug(
            f'First condition for long position met {binance_last_bid=} > {binance_previous_bid=}',
            extra=strategy.extra_log
        )
        binance_delta_percent = (
            (binance_last_bid - binance_previous_bid) / binance_previous_bid * 100
        )
        okx_delta_percent = (
            (okx_last_ask - okx_previous_ask) / okx_previous_ask * 100
        )
        delta_points = (binance_last_bid - okx_last_ask) / symbol.okx.tick_size
    elif position_side == 'short':
        logger.debug(
            f'First condition for short position met {binance_last_ask=} < {binance_previous_ask=}',
            extra=strategy.extra_log
        )
        binance_delta_percent = (
            (binance_previous_ask - binance_last_ask) / binance_previous_ask * 100
        )
        okx_delta_percent = (
            (okx_previous_bid - okx_last_bid) / okx_previous_bid * 100
        )
        delta_points = (okx_last_bid - binance_last_ask) / symbol.okx.tick_size
    spread_percent = (
        (okx_last_ask - okx_last_bid) / okx_last_bid * 100
    )
    spread_points = (okx_last_ask - okx_last_bid) / symbol.okx.tick_size
    if position_side:
        if okx_delta_percent >= 0:
            logger.debug(
                f'{spread_percent=:.5f}, {spread_points=:.2f}, {delta_points=:.2f}',
                extra=strategy.extra_log
            )
            logger.debug(
                f'{binance_delta_percent=:.5f}, {okx_delta_percent=:.5f}, {position_side=}',
                extra=strategy.extra_log
            )
            target_profit = strategy.tp_first_price_percent if strategy.close_position_parts else strategy.target_profit
            min_delta_percent = strategy.open_plus_close_fee + spread_percent + target_profit
            delta_percent = binance_delta_percent - okx_delta_percent
            if delta_percent >= min_delta_percent:
                condition_met = True
                logger.info(
                    f'Second condition for {position_side} position met '
                    f'{delta_percent=:.5f} >= {min_delta_percent=:.5f}',
                    extra=strategy.extra_log
                )
            else:
                logger.debug(
                    f'Second condition for {position_side} position not met '
                    f'{delta_percent=:.5f} < {min_delta_percent=:.5f}',
                    extra=strategy.extra_log
                )
        else:
            logger.debug(
                f'Second condition for {position_side} position not met, {okx_delta_percent=:.5f} < 0',
                extra=strategy.extra_log
            )
    prices = dict(
        binance_previous_ask=binance_previous_ask,
        binance_last_ask=binance_last_ask,
        binance_previous_bid=binance_previous_bid,
        binance_last_bid=binance_last_bid,
        okx_previous_ask=okx_previous_ask,
        okx_last_ask=okx_last_ask,
        okx_previous_bid=okx_previous_bid,
        okx_last_bid=okx_last_bid,
        spread_points=spread_points,
        spread_percent=spread_percent,
        delta_points=delta_points,
        delta_percent=delta_percent,
        target_delta=min_delta_percent,
        date_time_last_prices=date_time_last_prices
    )
    return condition_met, position_side, prices
