import logging
import random
import time
from math import floor
from decimal import Decimal, ROUND_DOWN
from datetime import datetime
from django.utils import timezone
from types import SimpleNamespace as Namespace
import okx.Trade as Trade
import okx.Account as Account
from .models import Strategy, Symbol, OkxSymbol, Execution, Position
from .exceptions import (
    PlaceOrderException, GetPositionException, GetOrderException,
    CancelOrderException, ClosePositionException
)
from .misc import convert_dict_values
from .helper import calc


logger = logging.getLogger(__name__)


class OkxTrade():
    def __init__(
        self, strategy: Strategy, symbol: Symbol, size_contract: float,
        position_side: str, debug=False
    ) -> None:
        strategy._extra_log.update(symbol=symbol.symbol)
        self.strategy = strategy
        self.symbol_okx = symbol.okx
        self.symbol = symbol
        self.size_contract = size_contract
        self.position_side = position_side
        apikey = strategy.second_account.api_key
        secretkey = strategy.second_account.api_secret
        passphrase = strategy.second_account.api_passphrase
        flag = '1' if strategy.second_account.testnet else '0'
        self.trade = Trade.TradeAPI(apikey, secretkey, passphrase, flag=flag, debug=debug)
        self.account = Account.AccountAPI(apikey, secretkey, passphrase, flag=flag, debug=debug)

    def place_order(self, symbol: OkxSymbol, size_contract: float, position_side: str) -> str:
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
            sz=size_contract
        )
        if result['code'] != '0':
            raise PlaceOrderException(
                f'Failed to place order. {result}. {size_contract=}, '
                f'{position_side=}, {symbol.market_price=}'
            )
        order_id = result['data'][0]['ordId']
        logger.info(
            f'Placed order {size_contract=}, {position_side=} {order_id=}',
            extra=self.strategy.extra_log
        )
        return order_id

    def open_position(self, increase: bool = False) -> None:
        size_contract = self.size_contract
        symbol = self.symbol_okx
        position_side = self.position_side
        order_id = self.place_order(symbol, size_contract, position_side)
        if not increase:
            logger.warning(
                f'Opened {position_side} position, {size_contract=}, {order_id=}',
                extra=self.strategy.extra_log
            )
        else:
            logger.warning(
                f'Increased {position_side} position, {size_contract=}, {order_id=}',
                extra=self.strategy.extra_log
            )

    def get_position(self, symbol: OkxSymbol = None) -> dict:
        if not symbol:
            symbol = self.symbol_okx
        result = self.account.get_positions(instId=symbol.inst_id, instType='SWAP')
        if result['code'] != '0':
            raise GetPositionException(f'Failed to get position data. {result}')
        if result['data']:
            data = convert_dict_values(result['data'][0])
            if data['pos']:
                logger.info(
                    f'Got position data: side={data["posSide"]}, sz={data["pos"]}, '
                    f'notionalUsd={data["notionalUsd"]}, avgPx={data["avgPx"]}',
                    extra=self.strategy.extra_log
                )
                return data
        raise GetPositionException('Failed to get position data')

    def close_position(self, size_contract: float) -> float:
        if self.position_side == 'long':
            side = 'sell'
        if self.position_side == 'short':
            side = 'buy'
        result = self.trade.place_order(
            instId=self.symbol_okx.inst_id,
            ordType='market',
            tdMode='isolated',
            posSide=self.position_side,
            side=side,
            sz=size_contract
        )
        if result['code'] != '0':
            raise PlaceOrderException(result)
        order_id = result['data'][0]['ordId']
        logger.warning(
            f'Closed {self.position_side} position partially {size_contract=}, {order_id=}',
            extra=self.strategy.extra_log
        )
        return size_contract

    def close_entire_position(self, symbol: OkxSymbol = None, position_side: str = None) -> None:
        if not symbol:
            symbol = self.symbol_okx
        if not position_side:
            position_side = self.position_side
        orders = self.get_order_list()
        for order in orders:
            self.cancel_order(order['ordId'], symbol)
        result = self.trade.close_positions(
            instId=symbol.inst_id,
            posSide=position_side,
            mgnMode='isolated'
        )
        if result['code'] != '0':
            raise ClosePositionException(f'Failed to close {position_side} position. {result}')
        else:
            logger.warning(f'Closed entire {position_side} position', extra=self.strategy.extra_log)

    def place_stop_loss(
        self,
        price: float,
        sz: int = None,
        symbol: OkxSymbol = None,
        position_side: str = None
    ) -> str:
        price = f'{price:.10f}'.rstrip('0')
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
            raise PlaceOrderException(
                f'Failed to place stop loss. {result}. {price=}, {sz=}, {symbol.market_price=}'
            )
        order_id = result['data'][0]['algoId']
        logger.info(
            f'Placed stop_loss_price={price}, {sz=}, {symbol.market_price=}, {order_id=}',
            extra=self.strategy.extra_log
        )
        return order_id

    def place_take_profit(self, price: float, symbol: OkxSymbol = None, position_side: str = None) -> str:
        price = f'{price:.10f}'.rstrip('0')
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
            raise PlaceOrderException(
                f'Failed to place take profit. {result}. {price=}, {symbol.market_price=}'
            )
        order_id = result['data'][0]['algoId']
        logger.info(f'Placed take profit {price=} {order_id=}', extra=self.strategy.extra_log)
        return order_id

    def place_stop_loss_and_take_profit(
        self, stop_loss_price: float, take_profit_price: float,
        symbol: OkxSymbol = None, position_side: str = None
    ) -> str:
        stop_loss_price = f'{stop_loss_price:.10f}'.rstrip('0')
        take_profit_price = f'{take_profit_price:.10f}'.rstrip('0')
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

    def place_limit_order(self, price: float, size_contract: float) -> str:
        price = f'{price:.10f}'.rstrip('0')
        if self.position_side == 'long':
            side = 'sell'
        if self.position_side == 'short':
            side = 'buy'
        result = self.trade.place_order(
            instId=self.symbol_okx.inst_id,
            ordType='limit',
            tdMode='isolated',
            posSide=self.position_side,
            side=side,
            sz=size_contract,
            px=price
        )
        if result['code'] != '0':
            raise PlaceOrderException(
                f'Failed to place limit order. {result}. {size_contract=}, '
                f'{price=}, position_side={self.position_side}, '
                f'market_price={self.symbol_okx.market_price}'
            )
        order_id = result['data'][0]['ordId']
        logger.info(
            f'Placed limit order {size_contract=}, {price=}, {order_id=}',
            extra=self.strategy.extra_log
        )
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

    def get_order(self, order_id: str, symbol: OkxSymbol) -> dict:
        result = self.trade.get_order(instId=symbol.inst_id, ordId=order_id)
        if result['code'] != '0':
            raise GetOrderException(result)
        return convert_dict_values(result['data'][0])

    def cancel_order(self, order_id: str, symbol: OkxSymbol) -> None:
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
        sz: int = None,
        symbol: OkxSymbol = None,
        position_side: str = None
    ) -> str:
        if not symbol:
            symbol = self.symbol_okx
        if not position_side:
            position_side = self.position_side
        algo_id = self.get_algo_order_id()
        if not algo_id:
            algo_id = self.place_stop_loss(price, sz, symbol, position_side)
            return algo_id
        price = f'{price:.10f}'.rstrip('0')
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
            raise PlaceOrderException(
                f'Failed to update stop loss. {result}. {price=}, {sz=}, {symbol.market_price=}'
            )
        logger.info(f'Updated stop_loss_price={price} {sz=}', extra=self.strategy.extra_log)
        order_id = result['data'][0]['algoId']
        return order_id

    def update_take_profit(self, price: float, symbol: OkxSymbol = None, position_side: str = None) -> None:
        price = f'{price:.10f}'.rstrip('0')
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
            raise PlaceOrderException(
                f'Failed to update take profit. {result}. {price=}, {symbol.market_price=}'
            )
        logger.info(f'Updated take profit {price=}', extra=self.strategy.extra_log)


class OkxEmulateTrade():
    def __init__(self, strategy: Strategy, symbol: Symbol) -> None:
        strategy._extra_log.update(symbol=symbol.symbol)
        self.strategy = strategy
        self.symbol = symbol

    def create_position(
        self, open_price: float, size_contract: float, size_usdt: float,
        position_side: str, date_time: str
    ) -> Position:
        position_data = Position.get_position_empty_data()
        position_data.update(
            posSide=position_side,
            cTime=date_time,
            pos=size_contract,
            avgPx=open_price,
            notionalUsd=size_usdt,
        )
        position = Position.objects.create(
            strategy=self.strategy, symbol=self.symbol, mode=Strategy.Mode.emulate,
            position_data=position_data, account=self.strategy.second_account
        )
        self.strategy._extra_log.update(position=position.id)
        logger.warning(
            f'Created virtual {position_side} position, {size_contract=}, {size_usdt=} '
            f'at {date_time}, avgPx={position_data["avgPx"]}',
            extra=self.strategy.extra_log
        )
        self.create_open_execution(position, date_time)
        return position

    def create_open_execution(self, position: Position, date_time: str) -> None:
        execution_data = Execution.get_empty_data()
        execution_data.update(
            subType='Open long' if position.side == 'long' else 'Open short',
            sz=position.sz,
            px=position.position_data['avgPx'],
            ts=date_time,
            fee=round(position.size_usdt * self.strategy.open_fee / 100, 10),
            pnl=None
        )
        Execution.objects.create(
            position=position,
            trade_id=str(random.randint(500000000, 999999999)),
            bill_id=str(int(time.time() * 10000000)),
            data=execution_data
        )
        logger.info(
            f'Created virtual {execution_data["subType"]} execution, '
            f'sz={execution_data["sz"]}, px={execution_data["px"]}, '
            f'fee={execution_data["fee"]}',
            extra=self.strategy.extra_log
        )

    def close_position(
        self,
        position: Position,
        close_price: float,
        size_contract: float,
        date_time: str = '',
    ) -> None:
        if not date_time:
            date_time = datetime.fromtimestamp(
                timezone.now().timestamp()).strftime('%d-%m-%Y %H:%M:%S.%f')[:-3]
        size_usdt = calc.get_usdt_from_sz(self.symbol.okx, size_contract, close_price)
        if size_contract == position.sz:
            position.is_open = False
            logger.warning(
                'Virtual position is closed completely '
                f'{close_price=} {size_contract=}, {size_usdt=}',
                extra=self.strategy.extra_log
            )
        else:
            lot_sz = str(self.symbol.okx.lot_sz)
            pos = position.sz - size_contract
            position.position_data['pos'] = float(
                Decimal(pos).quantize(Decimal(lot_sz), rounding=ROUND_DOWN)
            )
            position.position_data['notionalUsd'] = round(
                position.size_usdt - size_usdt, 2
            )
            logger.warning(
                'Virtual position is closed partially '
                f'{close_price=} {size_contract=}, {size_usdt=}',
                extra=self.strategy.extra_log
            )
        position.save(update_fields=['is_open', 'sl_tp_data', 'position_data'])
        self.create_close_execution(
            position, close_price, size_contract, size_usdt, date_time
        )

    def create_close_execution(
        self, position: Position, close_price: float, size_contract: float,
        size_usdt: float, date_time: str
    ) -> None:
        sub_type = 'Open long' if position.side == 'long' else 'Open short'
        open_execution = position.executions.filter(data__subType=sub_type).first()
        tp_first_part_percent = self.strategy.tp_first_part_percent
        tp_second_part_percent = 100 - tp_first_part_percent
        sl_tp_data = Namespace(**position.sl_tp_data)
        if (position.sz == size_contract and sl_tp_data.first_part_closed) or sl_tp_data.second_part_closed:
            open_fee = round(open_execution.data['fee'] * tp_second_part_percent / 100, 10)
        elif sl_tp_data.first_part_closed and not sl_tp_data.second_part_closed:
            open_fee = round(open_execution.data['fee'] * tp_first_part_percent / 100, 10)
        else:
            open_fee = open_execution.data['fee']
        open_price = open_execution.data['px']
        base_coin = calc.get_base_coin_from_sz(size_contract, self.symbol.okx.ct_val)
        close_fee = round(size_usdt * self.strategy.close_fee / 100, 10)
        if position.side == 'long':
            pnl = round((close_price - open_price) * base_coin - (open_fee + close_fee), 10)
        elif position.side == 'short':
            pnl = round((open_price - close_price) * base_coin - (open_fee + close_fee), 10)
        execution_data = Execution.get_empty_data()
        execution_data.update(
            subType='Close long' if position.side == 'long' else 'Close short',
            sz=size_contract,
            px=close_price,
            ts=date_time,
            fee=close_fee,
            pnl=pnl
        )
        Execution.objects.create(
            position=position,
            trade_id=str(random.randint(500000000, 999999999)),
            bill_id=str(int(time.time() * 10000000)),
            data=execution_data
        )
        logger.info(
            f'Created virtual {execution_data["subType"]} execution, '
            f'sz={execution_data["sz"]}, px={execution_data["px"]}, '
            f'fee={execution_data["fee"]}, pnl={execution_data["pnl"]}',
            extra=self.strategy.extra_log
        )


def get_take_profit_grid(position: Position, entry_price: float, spread_percent: float, position_side: str) -> dict:
    strategy = position.strategy
    lot_sz = position.symbol.okx.lot_sz
    tick_size = position.symbol.okx.tick_size
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
    tp_first_part = position.sz * strategy.tp_first_part_percent / 100
    # tp_first_part = round(floor(tp_first_part / lot_sz) * lot_sz, 2)
    tp_first_part = float(Decimal(tp_first_part).quantize(Decimal(str(lot_sz)), rounding=ROUND_DOWN))
    # strategy.tp_second_part_percent = 100 - strategy.tp_first_part_percent
    # tp_second_part = position.sz * strategy.tp_second_part_percent / 100
    tp_second_part = position.sz - tp_first_part
    # tp_second_part = round(floor(tp_second_part / lot_sz) * lot_sz, 2)
    tp_second_part = float(Decimal(tp_second_part).quantize(Decimal(str(lot_sz)), rounding=ROUND_DOWN))
    return dict(
        # tp_first_price=round(tp_first_price, 4),
        tp_first_price=float(Decimal(tp_first_price).quantize(Decimal(str(tick_size)), rounding=ROUND_DOWN)),
        tp_first_part=tp_first_part,
        # tp_second_price=round(tp_second_price, 4),
        tp_second_price=float(Decimal(tp_second_price).quantize(Decimal(str(tick_size)), rounding=ROUND_DOWN)),
        tp_second_part=tp_second_part
    )


def get_stop_loss_breakeven(
    symbol: OkxSymbol, entry_price: float, fee_percent: float,
    spread_percent: float, position_side: str
) -> float:
    if position_side == 'long':
        stop_loss_price = (
            entry_price * (1 + (fee_percent + spread_percent) / 100)
        )
    if position_side == 'short':
        stop_loss_price = (
            entry_price * (1 - (fee_percent + spread_percent) / 100)
        )
    # return round(stop_loss_price, 4)
    stop_loss_price = (
        Decimal(stop_loss_price)
        .quantize(Decimal(str(symbol.tick_size)), rounding=ROUND_DOWN)
    )
    return float(stop_loss_price)
