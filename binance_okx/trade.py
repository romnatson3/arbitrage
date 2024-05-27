import logging
import time
from django.core.cache import cache
import okx.Trade as Trade
import okx.Account as Account
from .models import Strategy, Symbol, OkxSymbol, Execution, Position
from .helper import calc
from .exceptions import (
    PlaceOrderException, GetPositionException, GetOrderException,
    CancelOrderException, ClosePositionException, GetExecutionException
)
from .misc import convert_dict_values


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
        self.position_cache_key = f'okx_position_{self.symbol_okx.inst_id}_{strategy.second_account.id}'
        self.position = Position.objects.filter(strategy=strategy, symbol=symbol, is_open=True).last()

    def open_position(
        self,
        position_size: float = None,
        inst_id: str = None,
        position_side: str = None
    ) -> None:
        if not inst_id:
            inst_id = self.symbol_okx.inst_id
            symbol_okx = self.symbol_okx
        else:
            symbol_okx = OkxSymbol.objects.get(data__instId=inst_id)
        if not position_size:
            position_size = self.strategy.position_size
        sz = calc.get_sz(position_size, symbol_okx)
        if not position_side:
            position_side = self.position_side
        if position_side == 'long':
            side = 'buy'
        if position_side == 'short':
            side = 'sell'
        result = self.trade.place_order(
            instId=inst_id,
            ordType='market',
            tdMode='isolated',
            posSide=position_side,
            side=side,
            sz=sz
        )
        if result['code'] != '0':
            raise PlaceOrderException(result)
        order_id = result['data'][0]['ordId']
        logger.info(f'Opened {position_side} position, {order_id=}', extra=self.strategy.extra_log)
        position: dict = self.get_position()
        self.position: Position = self._save_position(position)
        executions: list[dict] = self.get_executions(order_id)
        for execution in executions:
            self._save_executions(execution)

    def _save_position(self, data: dict) -> Position:
        position_data = Position.get_position_empty_data()
        position_data.update(
            avgPx=data['avgPx'],
            availPos=data['availPos'],
            notionalUsd=data['notionalUsd'],
            fee=data['fee'],
            instId=data['instId'],
            lever=data['lever'],
            posId=data['posId'],
            tradeId=data['tradeId'],
            posSide=data['posSide'],
            cTime=data['cTime'],
            uTime=data['uTime'],
            upl=data['upl']
        )
        position = Position.objects.filter(
            position_data__posId=position_data['posId'],
            position_data__tradeId=position_data['tradeId']
        ).first()
        if position:
            logger.warning(f'Position "{position}" already exists', extra=self.strategy.extra_log)
            return position
        position = Position.objects.create(position_data=position_data, strategy=self.strategy, symbol=self.symbol)
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

    def _save_executions(self, data: dict) -> Execution:
        bill_id = data['billId']
        trade_id = data['tradeId']
        execution = Execution.objects.filter(bill_id=bill_id).first()
        if execution:
            logger.warning(f'Execution {bill_id=} {trade_id=} already exists', extra=self.strategy.extra_log)
            return execution
        execution = Execution.objects.create(data=data, position=self.position, bill_id=bill_id, trade_id=trade_id)
        logger.info(f'Saved execution {bill_id=} {trade_id=}', extra=self.strategy.extra_log)
        return execution

    def close_position(self, size: float, inst_id: str = None, position_side: str = None) -> None:
        if not inst_id:
            inst_id = self.symbol_okx.inst_id
            symbol_okx = self.symbol_okx
        else:
            symbol_okx = OkxSymbol.objects.get(data__instId=inst_id)
        sz = calc.get_sz(size, symbol_okx)
        if not position_side:
            position_side = self.position_side
        if position_side == 'long':
            side = 'sell'
        if position_side == 'short':
            side = 'buy'
        result = self.trade.place_order(
            instId=inst_id,
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
            f'Closed {position_side} position partially {size=} {order_id=}',
            extra=self.strategy.extra_log
        )
        executions: list[dict] = self.get_executions(order_id)
        for execution in executions:
            self._save_executions(execution)

    def close_entire_position(self, inst_id: str = None, position_side: str = None) -> None:
        if not inst_id:
            inst_id = self.symbol_okx.inst_id
        if not position_side:
            position_side = self.position_side
        orders = self.get_order_list()
        for order in orders:
            self.cancel_order(order['ordId'])
        result = self.trade.close_positions(
            instId=inst_id,
            posSide=position_side,
            mgnMode='isolated'
        )
        if result['code'] != '0':
            raise ClosePositionException(
                f'Failed to close {position_side} position. {result["msg"]}'
            )
        else:
            logger.info(f'Closed {position_side} position', extra=self.strategy.extra_log)

    def get_position(self, inst_id: str = None) -> dict:
        if not inst_id:
            inst_id = self.symbol_okx.inst_id
        end_time = time.time() + 10
        while time.time() < end_time:
            logger.debug('Trying to get position data', extra=self.strategy.extra_log)
            result = self.account.get_positions(instId=inst_id, instType='SWAP')
            if result['code'] != '0':
                raise GetPositionException(f'Failed to get position data. {result["msg"]}')
            data = convert_dict_values(result['data'][0])
            if data['availPos']:
                logger.info('Got position data', extra=self.strategy.extra_log)
                cache.set(self.position_cache_key, data)
                return data
            time.sleep(1)
        raise GetPositionException('Failed to get position data. Timeout')

    def get_cached_position(self, inst_id: str = None) -> dict:
        if not inst_id:
            inst_id = self.symbol_okx.inst_id
        data = cache.get(self.position_cache_key)
        if not data:
            logger.info('Position data not found in cache', extra=self.strategy.extra_log)
            return {}
        return data

    def place_stop_loss(self, price: float, inst_id: str = None, position_side: str = None) -> str:
        if not inst_id:
            inst_id = self.symbol_okx.inst_id
        if not position_side:
            position_side = self.position_side
        if position_side == 'long':
            side = 'sell'
        if position_side == 'short':
            side = 'buy'
        result = self.trade.place_algo_order(
            instId=inst_id,
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
            raise PlaceOrderException(result['data'][0]['sMsg'])
        order_id = result['data'][0]['algoId']
        logger.info(f'Placed stop loss {price} {order_id=}', extra=self.strategy.extra_log)
        return order_id

    def place_take_profit(self, price: float, inst_id: str = None, position_side: str = None) -> str:
        if not inst_id:
            inst_id = self.symbol_okx.inst_id
        if not position_side:
            position_side = self.position_side
        if position_side == 'long':
            side = 'sell'
        if position_side == 'short':
            side = 'buy'
        result = self.trade.place_algo_order(
            instId=inst_id,
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
            raise PlaceOrderException(result['data'][0]['sMsg'])
        order_id = result['data'][0]['algoId']
        logger.info(f'Placed take profit {price} {order_id=}', extra=self.strategy.extra_log)
        return order_id

    def place_stop_loss_and_take_profit(
        self, stop_loss_price: float,
        take_profit_price: float,
        inst_id: str = None,
        position_side: str = None
    ) -> str:
        if not inst_id:
            inst_id = self.symbol_okx.inst_id
        if not position_side:
            position_side = self.position_side
        if position_side == 'long':
            side = 'sell'
        if position_side == 'short':
            side = 'buy'
        result = self.trade.place_algo_order(
            instId=inst_id,
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
            raise PlaceOrderException(result['data'][0]['sMsg'])
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
        inst_id: str = None,
        position_side: str = None
    ) -> str:
        if not inst_id:
            inst_id = self.symbol_okx.inst_id
            symbol_okx = self.symbol_okx
        else:
            symbol_okx = OkxSymbol.objects.get(data__instId=inst_id)
        if not position_size:
            position_size = self.strategy.position_size
        sz = calc.get_sz(position_size, symbol_okx)
        if not position_side:
            position_side = self.position_side
        if position_side == 'long':
            side = 'sell'
        if position_side == 'short':
            side = 'buy'
        result = self.trade.place_order(
            instId=inst_id,
            ordType='limit',
            tdMode='isolated',
            posSide=position_side,
            side=side,
            sz=sz,
            px=price
        )
        if result['code'] != '0':
            raise PlaceOrderException(result['data'][0]['sMsg'])
        order_id = result['data'][0]['ordId']
        logger.info(f'Placed limit order {sz=} {price=} {order_id=}', extra=self.strategy.extra_log)
        return order_id

    def get_order_list(self, inst_id: str = None) -> list:
        if not inst_id:
            inst_id = self.symbol_okx.inst_id
        result = self.trade.get_order_list(instId=self.symbol_okx.inst_id)
        if result['code'] != '0':
            raise GetOrderException(result['data'][0]['sMsg'])
        orders = []
        for order in result['data']:
            orders.append(convert_dict_values(order))
        logger.info(f'Got {len(orders)} orders', extra=self.strategy.extra_log)
        return orders

    def get_order(self, inst_id: str, order_id: str) -> dict:
        result = self.trade.get_order(instId=inst_id, ordId=order_id)
        if result['code'] != '0':
            raise GetOrderException(result['data'][0]['sMsg'])
        return convert_dict_values(result['data'][0])

    def cancel_order(self, order_id: str) -> None:
        result = self.trade.cancel_order(instId=self.symbol_okx.inst_id, ordId=order_id)
        if result['code'] != '0':
            raise CancelOrderException(result['data'][0]['sMsg'])
        else:
            logger.info(f'Cancelled {order_id=}', extra=self.strategy.extra_log)

    def get_algo_order_id(self, inst_id: str = None) -> list:
        if not inst_id:
            inst_id = self.symbol_okx.inst_id
        result = self.trade.order_algos_list(instId=inst_id, ordType='conditional')
        if result['code'] != '0':
            raise GetOrderException(result['data'][0]['sMsg'])
        if not result['data']:
            result = self.trade.order_algos_list(instId=inst_id, ordType='oco')
            if result['code'] != '0':
                raise GetOrderException(result['data'][0]['sMsg'])
        if result['data']:
            return int(result['data'][0]['algoId'])

    def update_stop_loss(self, price: float, inst_id: str = None, position_side: str = None) -> None:
        if not inst_id:
            inst_id = self.symbol_okx.inst_id
        if not position_side:
            position_side = self.position_side
        algo_id = self.get_algo_order_id()
        if not algo_id:
            algo_id = self.place_stop_loss(price, inst_id, position_side)
            return
        result = self.trade.amend_algo_order(
            instId=inst_id,
            algoId=algo_id,
            newSlTriggerPx=price,
            newSlOrdPx=-1,
            newSlTriggerPxType='mark'
        )
        if result['code'] != '0':
            raise PlaceOrderException(result['data'][0]['sMsg'])
        logger.info(f'Updated stop loss {price=}', extra=self.strategy.extra_log)

    def update_take_profit(self, price: float, inst_id: str = None, position_side: str = None) -> None:
        if not inst_id:
            inst_id = self.symbol_okx.inst_id
        if not position_side:
            position_side = self.position_side
        algo_id = self.get_algo_order_id()
        if not algo_id:
            algo_id = self.place_take_profit(price, inst_id, position_side)
            return
        result = self.trade.amend_algo_order(
            instId=inst_id,
            algoId=algo_id,
            newTpTriggerPx=price,
            newTpOrdPx=-1,
            newTpTriggerPxType='mark'
        )
        if result['code'] != '0':
            raise PlaceOrderException(result['data'][0]['sMsg'])
        logger.info(f'Updated take profit {price=}', extra=self.strategy.extra_log)


def take_profit_grid(strategy: Strategy, entry_price: float, spread_percent: float, position_side: str):
    if position_side == 'long':
        tp_first_price = (
            entry_price * (1 + (strategy.tp_first_price_percent + 2 * strategy.fee_percent + spread_percent) / 100)
        )
        tp_second_price = (
            entry_price * (1 + (strategy.tp_second_price_percent + 2 * strategy.fee_percent + spread_percent) / 100)
        )
    if position_side == 'short':
        tp_first_price = (
            entry_price * (1 - (strategy.tp_first_price_percent + 2 * strategy.fee_percent + spread_percent) / 100)
        )
        tp_second_price = (
            entry_price * (1 - (strategy.tp_second_price_percent + 2 * strategy.fee_percent + spread_percent) / 100)
        )
    tp_first_amount_without_fee = (
        strategy.position_size / 100 * strategy.tp_first_part_percent * (1 + (strategy.tp_first_price_percent + spread_percent) / 100)
    )
    tp_second_amount_without_fee = (
        strategy.position_size / 100 * strategy.tp_second_part_percent * (1 + (strategy.tp_second_price_percent + spread_percent) / 100)
    )
    tp_first_amount_with_fee = tp_first_amount_without_fee * (1 + (2 * strategy.fee_percent) / 100)
    tp_second_amount_with_fee = tp_second_amount_without_fee * (1 + (2 * strategy.fee_percent) / 100)
    return [
        [tp_first_price, tp_first_amount_with_fee],
        [tp_second_price, tp_second_amount_with_fee]
    ]


def stop_loss_breakeven(entry_price: float, fee_percent: float, spread_percent: float, position_side: str):
    if position_side == 'long':
        stop_loss_price = (
            entry_price * (1 + (2 * fee_percent + spread_percent) / 100)
        )
    if position_side == 'short':
        stop_loss_price = (
            entry_price * (1 - (2 * fee_percent + spread_percent) / 100)
        )
    return stop_loss_price
