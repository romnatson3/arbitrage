import logging
import time
import uuid
from django.utils import timezone
import okx.Trade as Trade
import okx.Account as Account
from .models import Strategy, Symbol, OkxSymbol, Execution, Position
from .exceptions import (
    PlaceOrderException, GetPositionException, GetOrderException,
    CancelOrderException, ClosePositionException, GetExecutionException
)
from .misc import convert_dict_values
from .helper import calc, CachePrice


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
        sz = calc.get_sz(position_size, symbol)
        if not position_side:
            position_side = self.position_side
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
        logger.info(
            f'Opened {position_side} position, {position_size=}, {sz=}, {order_id=}',
            extra=self.strategy.extra_log
        )
        position: dict = self.get_position()
        position: Position = self._save_position(position)
        executions: list[dict] = self.get_executions(order_id)
        for execution in executions:
            self._save_executions(execution, position)
        return position

    def _save_position(self, data: dict) -> Position:
        position = Position.objects.filter(
            position_data__posId=data['posId'], strategy=self.strategy,
            symbol=self.symbol, is_open=True
        ).last()
        if position:
            logger.warning(f'Position "{position}" already exists', extra=self.strategy.extra_log)
            position.position_data = data
            position.save()
            logger.debug(f'Updated position {position}', extra=self.strategy.extra_log)
            return position
        position = Position.objects.create(position_data=data, strategy=self.strategy, symbol=self.symbol)
        logger.info(f'Saved position {position}', extra=self.strategy.extra_log)
        return position

    def get_executions(self, order_id: str) -> list:
        end_time = time.time() + 10
        while time.time() < end_time:
            logger.debug(f'Trying to get executions for {order_id=}', extra=self.strategy.extra_log)
            result = self.account.get_account_bills(instType='SWAP', mgnMode='isolated', type=2)
            if result['code'] != '0':
                raise GetExecutionException(result['data'][0]['sMsg'])
            executions = []
            for execution in result['data']:
                if execution['ordId'] == order_id:
                    e = convert_dict_values(execution)
                    if Execution.sub_type.get(e['subType']):
                        e['subType'] = Execution.sub_type[e['subType']]
                    executions.append(convert_dict_values(execution))
            if executions:
                logger.info(f'Got {len(executions)} executions for {order_id=}', extra=self.strategy.extra_log)
                return executions
            time.sleep(1)
        raise GetExecutionException(
            f'Failed to get executions for {order_id=}',
            extra=self.strategy.extra_log
        )

    def _save_executions(self, data: dict, position: Position) -> Execution:
        bill_id = data['billId']
        trade_id = data['tradeId']
        execution = Execution.objects.filter(bill_id=bill_id, trade_id=trade_id).first()
        if execution:
            logger.warning(f'Execution {bill_id=} {trade_id=} already exists', extra=self.strategy.extra_log)
            return execution
        execution = Execution.objects.create(data=data, position=position, bill_id=bill_id, trade_id=trade_id)
        logger.info(f'Saved execution {bill_id=} {trade_id=}', extra=self.strategy.extra_log)
        return execution

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
        logger.info(
            f'Closed {position_side} position partially {size_usdt=} {order_id=}',
            extra=self.strategy.extra_log
        )

    def close_entire_position(self, symbol: OkxSymbol = None, position_side: str = None) -> None:
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
            raise ClosePositionException(
                f'Failed to close {position_side} position. {result["msg"]}'
            )
        else:
            logger.info(f'Closed {position_side} position', extra=self.strategy.extra_log)

    def get_position(self, symbol: OkxSymbol = None) -> dict:
        if not symbol:
            symbol = self.symbol_okx
        end_time = time.time() + 10
        while time.time() < end_time:
            logger.debug('Trying to get position data', extra=self.strategy.extra_log)
            result = self.account.get_positions(instId=symbol.inst_id, instType='SWAP')
            if result['code'] != '0':
                raise GetPositionException(f'Failed to get position data. {result["msg"]}')
            data = convert_dict_values(result['data'][0])
            if data['pos']:
                logger.info('Got position data', extra=self.strategy.extra_log)
                return data
            time.sleep(1)
        raise GetPositionException('Failed to get position data. Timeout')

    def place_stop_loss(self, price: float, symbol: OkxSymbol = None, position_side: str = None) -> str:
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
            slTriggerPx=price,
            slOrdPx=-1,
            slTriggerPxType='mark'
        )
        if result['code'] != '0':
            raise PlaceOrderException(result)
        order_id = result['data'][0]['algoId']
        logger.info(f'Placed stop loss {price} {order_id=}', extra=self.strategy.extra_log)
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
            raise GetOrderException(result['data'][0]['sMsg'])
        orders = []
        for order in result['data']:
            orders.append(convert_dict_values(order))
        logger.info(f'Got {len(orders)} orders', extra=self.strategy.extra_log)
        return orders

    def get_order(self, symbol: OkxSymbol, order_id: str) -> dict:
        result = self.trade.get_order(instId=symbol.inst_id, ordId=order_id)
        if result['code'] != '0':
            raise GetOrderException(result['data'][0]['sMsg'])
        return convert_dict_values(result['data'][0])

    def cancel_order(self, symbol: OkxSymbol, order_id: str) -> None:
        result = self.trade.cancel_order(instId=symbol.inst_id, ordId=order_id)
        if result['code'] != '0':
            raise CancelOrderException(result['data'][0]['sMsg'])
        else:
            logger.info(f'Cancelled {order_id=}', extra=self.strategy.extra_log)

    def get_algo_order_id(self, symbol: OkxSymbol = None) -> list:
        if not symbol:
            symbol = self.symbol_okx
        result = self.trade.order_algos_list(instId=symbol.inst_id, ordType='conditional')
        if result['code'] != '0':
            raise GetOrderException(result['data'][0]['sMsg'])
        if not result['data']:
            result = self.trade.order_algos_list(instId=symbol.inst_id, ordType='oco')
            if result['code'] != '0':
                raise GetOrderException(result['data'][0]['sMsg'])
        if result['data']:
            return int(result['data'][0]['algoId'])

    def update_stop_loss(self, price: float, symbol: OkxSymbol = None, position_side: str = None) -> None:
        if not symbol:
            symbol = self.symbol_okx
        if not position_side:
            position_side = self.position_side
        algo_id = self.get_algo_order_id()
        if not algo_id:
            algo_id = self.place_stop_loss(price, symbol, position_side)
            return
        result = self.trade.amend_algo_order(
            instId=symbol.inst_id,
            algoId=algo_id,
            newSlTriggerPx=price,
            newSlOrdPx=-1,
            newSlTriggerPxType='mark'
        )
        if result['code'] != '0':
            raise PlaceOrderException(result['data'][0]['sMsg'])
        logger.info(f'Updated stop loss {price=}', extra=self.strategy.extra_log)

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
            raise PlaceOrderException(result['data'][0]['sMsg'])
        logger.info(f'Updated take profit {price=}', extra=self.strategy.extra_log)


class OkxEmulateTrade():
    def __init__(self, strategy: Strategy, symbol: Symbol) -> None:
        strategy._extra_log.update(symbol=symbol.symbol)
        self.strategy = strategy
        self.symbol = symbol

    def create_position(self, position_side: str) -> Position:
        logger.info(
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
        logger.info(f'Created virtual position "{position}"', extra=self.strategy.extra_log)
        self._create_open_execution(position)
        return position

    def close_position(self, position: Position, size_usdt: float) -> None:
        if size_usdt >= self.strategy.position_size:
            size_usdt = self.strategy.position_size
            position.is_open = False
            position.save(update_fields=['is_open'])
            logger.info(
                f'Virtual position "{position}" is closed completely {size_usdt=}',
                extra=self.strategy.extra_log
            )
        else:
            logger.info(
                f'Virtual position "{position}" is closed partially {size_usdt=}',
                extra=self.strategy.extra_log
            )
            sz = calc.get_sz(size_usdt, self.symbol.okx)
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
    position_side = None
    delta_percent = None
    delta_points = None
    spread_points = None
    condition_met = False
    first_exchange = CachePrice(strategy.first_account.exchange)
    second_exchange = CachePrice(strategy.second_account.exchange)
    second_exchange_previous_ask = second_exchange.get_ask_previous_price(symbol.symbol)
    second_exchange_previous_bid = second_exchange.get_bid_previous_price(symbol.symbol)
    second_exchange_last_ask = second_exchange.get_ask_last_price(symbol.symbol)
    second_exchange_last_bid = second_exchange.get_bid_last_price(symbol.symbol)
    first_exchange_previous_ask = first_exchange.get_ask_previous_price(symbol.symbol)
    first_exchange_previous_bid = first_exchange.get_bid_previous_price(symbol.symbol)
    first_exchange_last_ask = first_exchange.get_ask_last_price(symbol.symbol)
    first_exchange_last_bid = first_exchange.get_bid_last_price(symbol.symbol)
    if second_exchange_previous_ask < first_exchange_previous_ask:
        logger.debug(
            'First condition for long position met '
            f'{first_exchange_previous_ask=} < {second_exchange_previous_ask=}',
            extra=strategy.extra_log
        )
        first_exchange_delta_percent = (
            (first_exchange_previous_bid - first_exchange_last_bid) / first_exchange_previous_bid * 100
        )
        second_exchange_delta_percent = (
            (second_exchange_previous_ask - second_exchange_last_ask) / second_exchange_previous_ask * 100
        )
        position_side = 'long'
    elif second_exchange_previous_bid > first_exchange_previous_bid:
        logger.debug(
            'First condition for short position met '
            f'{first_exchange_previous_bid=} > {second_exchange_previous_bid=}',
            extra=strategy.extra_log
        )
        first_exchange_delta_percent = (
            (first_exchange_previous_ask - first_exchange_last_ask) / first_exchange_previous_ask * 100
        )
        second_exchange_delta_percent = (
            (second_exchange_previous_bid - second_exchange_last_bid) / second_exchange_previous_bid * 100
        )
        position_side = 'short'
    spread_percent = (
        (second_exchange_last_ask - second_exchange_last_bid) / second_exchange_last_bid * 100
    )
    spread_points = (second_exchange_last_ask - second_exchange_last_bid) / symbol.okx.tick_size
    if position_side:
        min_delta_percent = strategy.open_plus_close_fee + spread_percent + strategy.target_profit
        delta_percent = first_exchange_delta_percent - second_exchange_delta_percent
        if position_side == 'long':
            delta_points = (first_exchange_last_bid - second_exchange_last_ask) / symbol.okx.tick_size
        if position_side == 'short':
            delta_points = (first_exchange_last_ask - second_exchange_last_bid) / symbol.okx.tick_size
        if delta_percent >= min_delta_percent:
            condition_met = True
            logger.info(
                f'Second condition for {position_side} position met '
                f'{delta_percent=:.10f} >= {min_delta_percent=:.10f}',
                extra=strategy.extra_log
            )
        else:
            logger.debug(
                f'Second condition for {position_side} position not met '
                f'{delta_percent=:.10f} < {min_delta_percent=:.10f}',
                extra=strategy.extra_log
            )
    prices = dict(
        first_exchange_last_bid=first_exchange_last_bid,
        first_exchange_last_ask=first_exchange_last_ask,
        second_exchange_last_bid=second_exchange_last_bid,
        second_exchange_last_ask=second_exchange_last_ask,
        spread_points=spread_points,
        spread_percent=spread_percent,
        delta_points=delta_points,
        delta_percent=delta_percent
    )
    return condition_met, position_side, prices
