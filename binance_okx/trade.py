import logging
import time
from django.utils import timezone
from django.core.cache import cache
from types import SimpleNamespace as Namespace
import okx.Trade as Trade
import okx.Account as Account
from .models import Strategy, Symbol, OkxSymbol, Execution, Position
from .helper import calc
from .exceptions import (
    PlaceOrderException, GetPositionException, GetOrderException,
    CancelOrderException, ClosePositionException, GetExecutionException
)
from .misc import convert_dict_values
from .helper import CachePrice, CacheOrderId


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
        inst_id: str = None,
        position_side: str = None
    ) -> Position:
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
                return data
            time.sleep(1)
        raise GetPositionException('Failed to get position data. Timeout')

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
        self, stop_loss_price: float, take_profit_price: float,
        inst_id: str = None, position_side: str = None
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


def get_take_profit_grid(strategy: Strategy, entry_price: float, spread_percent: float, position_side: str):
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
    return dict(
        tp_first_price=round(tp_first_price, 5),
        tp_first_part=round(tp_first_amount_with_fee, 2),
        tp_second_price=round(tp_second_price, 5),
        tp_second_part=round(tp_second_amount_with_fee, 2)
    )


def get_stop_loss_breakeven(entry_price: float, fee_percent: float, spread_percent: float, position_side: str):
    if position_side == 'long':
        stop_loss_price = (
            entry_price * (1 + (2 * fee_percent + spread_percent) / 100)
        )
    if position_side == 'short':
        stop_loss_price = (
            entry_price * (1 - (2 * fee_percent + spread_percent) / 100)
        )
    return round(stop_loss_price, 5)


def check_price_condition(strategy: Strategy, symbol: str) -> tuple[bool, str, dict]:
    position_side = None
    delta_percent = None
    condition_met = False
    first_exchange = CachePrice(strategy.first_account.exchange)
    second_exchange = CachePrice(strategy.second_account.exchange)
    second_exchange_previous_ask = second_exchange.get_ask_previous_price(symbol)
    second_exchange_previous_bid = second_exchange.get_bid_previous_price(symbol)
    second_exchange_last_ask = second_exchange.get_ask_last_price(symbol)
    second_exchange_last_bid = second_exchange.get_bid_last_price(symbol)
    first_exchange_previous_ask = first_exchange.get_ask_previous_price(symbol)
    first_exchange_previous_bid = first_exchange.get_bid_previous_price(symbol)
    first_exchange_last_ask = first_exchange.get_ask_last_price(symbol)
    first_exchange_last_bid = first_exchange.get_bid_last_price(symbol)
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
    if position_side:
        min_delta_percent = 2 * strategy.fee_percent + spread_percent + strategy.target_profit
        delta_percent = first_exchange_delta_percent - second_exchange_delta_percent
        if delta_percent >= min_delta_percent:
            condition_met = True
            logger.info(
                f'Second condition for {position_side} position met '
                f'{delta_percent=:.5f} >= {min_delta_percent=}',
                extra=strategy.extra_log
            )
        else:
            logger.debug(
                f'Second condition for {position_side} position not met '
                f'{delta_percent=:.5f} < {min_delta_percent=}',
                extra=strategy.extra_log
            )
    prices = dict(
        first_exchange_last_bid=first_exchange_last_bid,
        first_exchange_last_ask=first_exchange_last_ask,
        second_exchange_last_bid=second_exchange_last_bid,
        second_exchange_last_ask=second_exchange_last_ask,
        spread_percent=round(spread_percent, 5),
        delta_percent=round(delta_percent, 5) if delta_percent else None
    )
    return condition_met, position_side, prices


def open_position(strategy: Strategy, symbol: str, position_side: str, prices: dict) -> None:
    symbol = Symbol.objects.get(symbol=symbol)
    funding_time = symbol.okx.funding_time
    edge_time = funding_time - timezone.timedelta(minutes=strategy.time_to_funding)
    if timezone.localtime() > edge_time:
        logger.warning(f'Funding time {funding_time} is too close', extra=strategy.extra_log)
        if strategy.only_profit:
            funding_rate = symbol.okx.funding_rate
            if (funding_rate > 0 and position_side == 'long') or (funding_rate < 0 and position_side == 'short'):
                logger.warning(
                    f'Funding rate {funding_rate:.5} is unfavorable for the current position. Skip',
                    extra=strategy.extra_log
                )
                return
            else:
                logger.warning(
                    f'Funding rate {funding_rate:.5} is favorable for the {position_side} position. Open',
                    extra=strategy.extra_log
                )
    logger.info(f'Opening {position_side} position', extra=strategy.extra_log)
    trade = OkxTrade(strategy, symbol, position_side)
    position = trade.open_position()
    if strategy.close_position_parts:
        take_profit_grid = get_take_profit_grid(
            strategy, position.entry_price, prices['spread_percent'], position_side)
        position.sl_tp_data.update(take_profit_grid)
        if strategy.close_position_type == 'limit':
            order_id = trade.place_limit_order(
                take_profit_grid['tp_first_price'], take_profit_grid['tp_first_part']
            )
            position.sl_tp_data['tp_first_limit_order_id'] = order_id
            trade.place_limit_order(
                take_profit_grid['tp_second_price'], take_profit_grid['tp_second_part']
            )
    if strategy.target_profit:
        position.sl_tp_data['take_profit_price'] = calc.get_take_profit_price(
            position.entry_price, strategy.target_profit, strategy.fee_percent,
            prices['spread_percent'], position_side
        )
        if not strategy.close_position_parts:
            if strategy.close_position_type == 'market':
                trade.update_take_profit(position.sl_tp_data['take_profit_price'])
            if strategy.close_position_type == 'limit':
                trade.place_limit_order(
                    position.sl_tp_data['take_profit_price'],
                    strategy.position_size
                )
    if strategy.stop_loss:
        position.sl_tp_data['stop_loss_price'] = calc.get_stop_loss_price(
            position.entry_price, strategy.stop_loss, position_side
        )
        trade.update_stop_loss(position.sl_tp_data['stop_loss_price'])
    if strategy.stop_loss_breakeven:
        position.sl_tp_data['stop_loss_breakeven'] = get_stop_loss_breakeven(
            position.entry_price, strategy.fee_percent, prices['spread_percent'], position_side
        )
    logger.info(f'Updated sl_tp_data for position "{position}"', extra=strategy.extra_log)
    position.ask_bid_data.update(
        bid_first_exchange=prices['first_exchange_last_bid'],
        ask_first_exchange=prices['first_exchange_last_ask'],
        bid_second_exchange=prices['second_exchange_last_bid'],
        ask_second_exchange=prices['second_exchange_last_ask'],
        spread_percent=prices['spread_percent'],
        delta_percent=prices['delta_percent']
    )
    _, _, prices_entry = check_price_condition(strategy, symbol)
    position.ask_bid_data.update(
        bid_first_exchange_entry=prices_entry['first_exchange_last_bid'],
        ask_first_exchange_entry=prices_entry['first_exchange_last_ask'],
        bid_second_exchange_entry=prices_entry['second_exchange_last_bid'],
        ask_second_exchange_entry=prices_entry['second_exchange_last_ask'],
        spread_percent_entry=prices_entry['spread_percent'],
        delta_percent_entry=prices_entry['delta_percent']
    )
    logger.info(f'Updated ask_bid_data for position "{position}"', extra=strategy.extra_log)
    position.save()


def watch_position(strategy: Strategy, position: Position) -> None:
    logger.debug(
        f'Position "{position}" is open for symbol {position.symbol}', extra=strategy.extra_log
    )
    position_data = Namespace(**position.position_data)
    trade = OkxTrade(strategy, position.symbol, position.side)
    if strategy.time_to_close:
        tz = timezone.get_current_timezone()
        open_time = timezone.datetime.strptime(
            position_data.cTime, '%d-%m-%Y %H:%M:%S.%f').astimezone(tz)
        close_time = open_time + timezone.timedelta(minutes=strategy.time_to_close)
        seconds_to_close = (close_time - timezone.localtime()).total_seconds()
        logger.debug(
            f'Time to close {round(seconds_to_close//60)} minutes {round(seconds_to_close%60)} seconds',
            extra=strategy.extra_log
        )
        if seconds_to_close <= 0:
            logger.warning(f'Close time {close_time} reached', extra=strategy.extra_log)
            trade.close_entire_position()
            return
    if strategy.close_position_parts:
        take_profit_grid = get_take_profit_grid(
            strategy, position.entry_price, position.ask_bid_data['spread_percent'], position.side)
        position.sl_tp_data.update(take_profit_grid)
        position.save(update_fields=['sl_tp_data'])
        sl_tp_data = Namespace(**position.sl_tp_data)
        if strategy.close_position_type == 'limit':
            if not sl_tp_data.first_part_closed:
                cache_orders_ids = CacheOrderId(strategy.second_account.id, position.symbol.okx.inst_id)
                if sl_tp_data.tp_first_limit_order_id in cache_orders_ids.get_orders():
                    logger.info(
                        f'First take profit limit order {sl_tp_data.tp_first_limit_order_id} is filled',
                        extra=strategy.extra_log
                    )
                    position.sl_tp_data['first_part_closed'] = True
                    if strategy.stop_loss_breakeven:
                        trade.update_stop_loss(sl_tp_data.stop_loss_breakeven)
                        logger.info(
                            f'Updated stop loss to breakeven {sl_tp_data.stop_loss_breakeven}',
                            extra=strategy.extra_log
                        )
                        position.sl_tp_data['stop_loss_breakeven_set'] = True
                    position.save(update_fields=['sl_tp_data'])
        if strategy.close_position_type == 'market':
            if position.side == 'long':
                if not sl_tp_data.first_part_closed:
                    if sl_tp_data.tp_first_price <= position.symbol.okx.market_price:
                        logger.debug(
                            f'{sl_tp_data.tp_first_price=} <= {position.symbol.okx.market_price=}',
                            extra=strategy.extra_log
                        )
                        trade.close_position(sl_tp_data.tp_first_part, position.symbol.okx.inst_id, position.side)
                        position.sl_tp_data['first_part_closed'] = True
                        logger.info(
                            f'First part of position "{position}" is closed',
                            extra=strategy.extra_log
                        )
                        if strategy.stop_loss_breakeven:
                            trade.update_stop_loss(sl_tp_data.stop_loss_breakeven)
                            position.sl_tp_data['stop_loss_breakeven_set'] = True
                        position.save(update_fields=['sl_tp_data'])
                else:
                    if not sl_tp_data.second_part_closed:
                        if sl_tp_data.tp_second_price <= position.symbol.okx.market_price:
                            logger.debug(
                                f'{sl_tp_data.tp_second_price=} <= {position.symbol.okx.market_price=}',
                                extra=strategy.extra_log
                            )
                            trade.close_position(sl_tp_data.tp_second_part, position.symbol.okx.inst_id, position.side)
                            position.sl_tp_data['second_part_closed'] = True
                            logger.info(
                                f'Second part of position "{position}" is closed',
                                extra=strategy.extra_log
                            )
                            position.save(update_fields=['sl_tp_data'])
            if position.side == 'short':
                if not sl_tp_data.first_part_closed:
                    if sl_tp_data.tp_first_price >= position.symbol.okx.market_price:
                        logger.debug(
                            f'{sl_tp_data.tp_first_price=} >= {position.symbol.okx.market_price=}',
                            extra=strategy.extra_log
                        )
                        trade.close_position(sl_tp_data.tp_first_part, position.symbol.okx.inst_id, position.side)
                        position.sl_tp_data['first_part_closed'] = True
                        logger.info(
                            f'First part of position "{position}" is closed',
                            extra=strategy.extra_log
                        )
                        if strategy.stop_loss_breakeven:
                            trade.update_stop_loss(sl_tp_data.stop_loss_breakeven)
                            position.sl_tp_data['stop_loss_breakeven_set'] = True
                        position.save(update_fields=['sl_tp_data'])
                else:
                    if not sl_tp_data.second_part_closed:
                        if sl_tp_data.tp_second_price >= position.symbol.okx.market_price:
                            logger.debug(
                                f'{sl_tp_data.tp_second_price=} >= {position.symbol.okx.market_price=}',
                                extra=strategy.extra_log
                            )
                            trade.close_position(sl_tp_data.tp_second_part, position.symbol.okx.inst_id, position.side)
                            position.sl_tp_data['second_part_closed'] = True
                            logger.info(
                                f'Second part of position "{position}" is closed',
                                extra=strategy.extra_log
                            )
                            position.save(update_fields=['sl_tp_data'])
