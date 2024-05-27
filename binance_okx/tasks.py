import logging
from django.utils import timezone
from django.contrib.auth import get_user_model
from django.core.cache import cache
# from celery.utils.log import get_task_logger
from exchange.celery import app
from .models import StatusLog, OkxSymbol, BinanceSymbol
from .exceptions import AcquireLockException
from binance.um_futures import UMFutures
import okx.MarketData
import okx.PublicData
from .helper import CachePrice
from .helper import TaskLock
from .models import Strategy, Symbol, Account, Position, Execution
from .exceptions import GetPositionException, GetExecutionException
import okx.Account
from .misc import convert_dict_values


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
def update_okx_positions() -> None:
    accounts = Account.objects.filter(exchange='okx').all()
    if not accounts:
        logger.debug('No okx accounts found')
        return
    try:
        with TaskLock('okx_task_update_positions'):
            for account in accounts:
                try:
                    apikey = account.api_key
                    secretkey = account.api_secret
                    passphrase = account.api_passphrase
                    flag = '1' if account.testnet else '0'
                    client = okx.Account.AccountAPI(apikey, secretkey, passphrase, flag=flag, debug=False)
                    result = client.get_positions(instType='SWAP')
                    if result['code'] != '0':
                        raise GetPositionException(f'Failed to get positions data. {result["msg"]}')
                    if not result['data']:
                        logger.debug(f'No found any positions for account: {account.name}')
                        for i in cache.keys(f'okx_position_*_{account.id}'):
                            cache.delete(i)
                        continue
                    for i in result['data']:
                        symbol = i['instId']
                        i = convert_dict_values(i)
                        cache.set(f'okx_position_{symbol}_{account.id}', i)
                    cached_positions = {i.split('_')[-2] for i in cache.keys(f'okx_position_*_{account.id}')}
                    new_positions = {i['instId'] for i in result['data']}
                    for symbol in cached_positions - new_positions:
                        cache.delete(f'okx_position_{symbol}_{account.id}')
                    logger.info(
                        f'Found {len(result["data"])} open positions for account: {account.name}. '
                        f'{", ".join(sorted(list(new_positions)))}'
                    )
                except Exception as e:
                    logger.exception(e)
                    continue
    except AcquireLockException:
        logger.debug('Task update_okx_positions is already running')
    except Exception as e:
        logger.exception(e)
        raise e


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
                    for position in open_positions_db:
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
                        last_execution = position.executions.last()
                        result = client.get_account_bills(
                            instType='SWAP', mgnMode='isolated', type=2,
                            before=last_execution.bill_id
                        )
                        if result['code'] != '0':
                            raise GetExecutionException(result['data'][0]['sMsg'])
                        if not result['data']:
                            logger.warning(
                                f'No found any new executions for position {position}',
                                extra=position.strategy.extra_log
                            )
                            continue
                        logger.info(
                            f'Found {len(result["data"])} executions for position {position}',
                            extra=position.strategy.extra_log
                        )
                        for e in [convert_dict_values(i) for i in result['data']]:
                            if Execution.sub_type.get(e['subType']):
                                e['subType'] = Execution.sub_type[e['subType']]
                            execution, created = Execution.objects.get_or_create(
                                position=position, bill_id=e['billId'], trade_id=e['tradeId'],
                                data=e
                            )
                            if created:
                                logger.info(
                                    f'Saved execution {execution.bill_id=} {execution.trade_id=}',
                                    extra=position.strategy.extra_log
                                )
                            else:
                                logger.warning(
                                    f'Execution {execution.bill_id=} {execution.trade_id=} already exists',
                                    extra=position.strategy.extra_log
                                )
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
        strategy = Strategy.objects.cache(id=strategy_id, set=True)[0]
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
            logger.debug('Run strategy', extra=strategy.extra_log)
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
            if 'position_side' in locals():
                spread_percent = (
                    (second_exchange_last_ask - second_exchange_last_bid) / second_exchange_last_bid * 100
                )
                min_delta_percent = 2 * strategy.fee_percent + spread_percent + strategy.target_profit
                delta_percent = first_exchange_delta_percent - second_exchange_delta_percent
                if delta_percent >= min_delta_percent:
                    logger.info(
                        f'Second condition for {position_side} position met '
                        f'{delta_percent=:.5f} >= {min_delta_percent=}',
                        extra=strategy.extra_log
                    )
                    logger.warning(f'Open {position_side} position', extra=strategy.extra_log)
                else:
                    logger.debug(
                        f'Second condition for {position_side} position not met '
                        f'{delta_percent=:.5f} < {min_delta_percent=}',
                        extra=strategy.extra_log
                    )
    except AcquireLockException:
        logger.debug('Task is already running', extra=strategy.extra_log)
    except Exception as e:
        logger.exception(e, extra=strategy.extra_log)
        raise e
