import logging
import okx.Trade as Trade
import okx.Account as Account
from .models import Strategy, Symbol
from .helper import calc
from .exceptions import (
    PlaceOrderException, GetPositionException, GetOrderException,
    CancelOrderException, ClosePositionException
)
from .misc import convert_dict_values


logger = logging.getLogger(__name__)


class OkxTrade():
    def __init__(self, strategy: Strategy, symbol: Symbol, position_side: str, debug=False) -> None:
        strategy._extra_log.update(symbol=symbol.symbol)
        self.strategy = strategy
        self.symbol = symbol.okx
        self.position_side = position_side
        apikey = strategy.second_account.api_key
        secretkey = strategy.second_account.api_secret
        passphrase = strategy.second_account.api_passphrase
        flag = '1' if strategy.second_account.testnet else '0'
        self.trade = Trade.TradeAPI(apikey, secretkey, passphrase, flag=flag, debug=debug)
        self.account = Account.AccountAPI(apikey, secretkey, passphrase, flag=flag, debug=debug)

    def open_position(self, sz: float = 0.0, position_side: str = '') -> str:
        if not sz:
            sz = calc.get_sz(self.strategy.position_size, self.symbol)
        if not position_side:
            position_side = self.position_side
        if position_side == 'long':
            side = 'buy'
        if position_side == 'short':
            side = 'sell'
        result = self.trade.place_order(
            instId=self.symbol.inst_id,
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
        return order_id

    def close_position(self, position_side: str = '', inst_id: str = '') -> None:
        if not inst_id:
            inst_id = self.symbol.inst_id
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

    def get_position(self, inst_id: str = '') -> dict:
        if not inst_id:
            inst_id = self.symbol.inst_id
        result = self.account.get_positions(instId=inst_id, instType='SWAP')
        if result['code'] != '0':
            raise GetPositionException(f'Failed to get positions data. {result["msg"]}')
        data = convert_dict_values(result['data'][0])
        logger.info('Got positions data', extra=self.strategy.extra_log)
        return data

    def place_stop_loss(self, price: float, position_side: str = '') -> str:
        if not position_side:
            position_side = self.position_side
        if position_side == 'long':
            side = 'sell'
        if position_side == 'short':
            side = 'buy'
        result = self.trade.place_algo_order(
            instId=self.symbol.inst_id,
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

    def place_take_profit(self, price: float, position_side: str = '') -> str:
        if not position_side:
            position_side = self.position_side
        if position_side == 'long':
            side = 'sell'
        if position_side == 'short':
            side = 'buy'
        result = self.trade.place_algo_order(
            instId=self.symbol.inst_id,
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
        self, stop_loss_price: float, take_profit_price: float, position_side: str = ''
    ) -> str:
        if not position_side:
            position_side = self.position_side
        if position_side == 'long':
            side = 'sell'
        if position_side == 'short':
            side = 'buy'
        result = self.trade.place_algo_order(
            instId=self.symbol.inst_id,
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

    def place_limit_order(self, price: float, sz: float = 0.0, position_side: str = '') -> str:
        if not sz:
            sz = calc.get_sz(self.strategy.position_size, self.symbol)
        if not position_side:
            position_side = self.position_side
        if position_side == 'long':
            side = 'sell'
        if position_side == 'short':
            side = 'buy'
        result = self.trade.place_order(
            instId=self.symbol.inst_id,
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

    def get_order_list(self, inst_id: str = '') -> list:
        if not inst_id:
            inst_id = self.symbol.inst_id
        result = self.trade.get_order_list(instId=self.symbol.inst_id)
        if result['code'] != '0':
            raise GetOrderException(result['data'][0]['sMsg'])
        orders = []
        for order in result['data']:
            orders.append(convert_dict_values(order))
        logger.info(f'Got {len(orders)} orders', extra=self.strategy.extra_log)
        return orders

    def cancel_order(self, order_id: str) -> None:
        result = self.trade.cancel_order(instId=self.symbol.inst_id, ordId=order_id)
        if result['code'] != '0':
            raise CancelOrderException(result['data'][0]['sMsg'])
        else:
            logger.info(f'Cancelled {order_id=}', extra=self.strategy.extra_log)
