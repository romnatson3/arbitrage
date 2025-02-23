import logging
import json
from django_redis import get_redis_connection
from datetime import datetime
from django.utils import timezone
import okx.PublicData as PublicData
from django.db import models, transaction
from django.db.models import QuerySet, F, Subquery
from django.db.models.functions import JSONObject
from django.core.cache import cache
from django.contrib.auth.models import AbstractUser
from django_celery_beat.models import PeriodicTask, IntervalSchedule


connection = get_redis_connection('default')
logger = logging.getLogger(__name__)


class BaseModel(models.Model):
    class Meta:
        abstract = True

    created_at = models.DateTimeField('created_at', auto_now_add=True)
    updated_at = models.DateTimeField('updated_at', auto_now=True)


class User(AbstractUser):
    first_name = None
    last_name = None

    def __str__(self):
        return self.username


class StatusLogManager(models.Manager):
    def get_queryset(self):
        return (
            super().get_queryset()
            .select_related('strategy')
        )


class StatusLog(BaseModel):
    class Meta:
        ordering = ('-created_at',)
        verbose_name = 'Logging'
        verbose_name_plural = 'Logging'

    LOG_LEVELS = (
        (logging.NOTSET, 'NotSet'),
        (logging.INFO, 'Info'),
        (logging.WARNING, 'Warning'),
        (logging.DEBUG, 'Debug'),
        (logging.ERROR, 'Error'),
        (logging.FATAL, 'Fatal'),
    )

    objects = StatusLogManager()

    logger_name = models.CharField(max_length=100)
    level = models.PositiveSmallIntegerField(choices=LOG_LEVELS, default=logging.ERROR, db_index=True)
    msg = models.TextField()
    trace = models.TextField(blank=True, null=True)
    strategy = models.ForeignKey(
        'Strategy', on_delete=models.CASCADE, related_name='status_logs',
        help_text='Strategy', null=True
    )
    symbol = models.CharField(max_length=20, blank=True, null=True)
    position = models.CharField(max_length=20, blank=True, null=True)


class Account(BaseModel):
    class Meta:
        verbose_name = 'Account'
        verbose_name_plural = 'Accounts'

    class Exchange(models.TextChoices):
        # binance = 'binance', 'Binance'
        okx = 'okx', 'OKX'

    name = models.CharField('name', max_length=255, blank=False, null=False, unique=True, help_text='Account name')
    exchange = models.CharField('exchange', choices=Exchange.choices, help_text='Exchange', default=Exchange.okx)
    api_key = models.CharField('api_key', max_length=255, blank=False, null=False, help_text='API key')
    api_secret = models.CharField('api_secret', max_length=255, blank=False, null=False, help_text='API secret')
    api_passphrase = models.CharField('api_passphrase', max_length=255, blank=True, null=True, help_text='API passphrase')
    testnet = models.BooleanField('testnet', default=False)

    def __str__(self):
        return f'{self.name}'


class BinanceSymbol(BaseModel):
    class Meta:
        verbose_name = 'Binance Symbol'
        verbose_name_plural = 'Binance Symbols'

    symbol = models.CharField(primary_key=True, unique=True, max_length=20)
    data = models.JSONField(default=dict, help_text='Instrument data')
    is_active = models.BooleanField(default=True, help_text='Is active')

    def __str__(self):
        return self.symbol


class OkxSymbol(BaseModel):
    class Meta:
        verbose_name = 'OKX Symbol'
        verbose_name_plural = 'OKX Symbols'

    symbol = models.CharField(primary_key=True, unique=True, max_length=20)
    data = models.JSONField(default=dict, help_text='Instrument data')
    is_active = models.BooleanField(default=True, help_text='Is active')

    @property
    def inst_id(self) -> str:
        return self.data['instId']

    @property
    def lot_sz(self) -> str:
        return self.data['lotSz']

    @property
    def ct_val(self) -> float:
        return float(self.data['ctVal'])

    @property
    def tick_size(self) -> float:
        return float(self.data['tickSz'])

    @property
    def market_price(self) -> float:
        return cache.get(f'okx_market_price_{self.symbol}', 0.0)

    @property
    def ask_price(self) -> float:
        key = f'okx_ask_bid_{self.symbol}'
        last_record = connection.zrange(key, -1, -1)
        if last_record:
            data = json.loads(last_record[0])
            return data['ask_price']
        return 0.0

    @property
    def bid_price(self) -> float:
        key = f'okx_ask_bid_{self.symbol}'
        last_record = connection.zrange(key, -1, -1)
        if last_record:
            data = json.loads(last_record[0])
            return data['bid_price']
        return 0.0

    @property
    def last_price_and_size(self) -> tuple:
        key = f'okx_last_price_{self.symbol}'
        last_record = connection.zrange(key, -1, -1)
        if last_record:
            data = json.loads(last_record[0])
            return data['last_price'], data['last_size']
        return 0.0, 0.0

    @property
    def funding_time(self) -> datetime:
        client = PublicData.PublicAPI(flag='0', debug=False)
        result = client.get_funding_rate(instId=self.inst_id)
        funding_time = result['data'][0]['fundingTime']
        tz = timezone.get_current_timezone()
        return datetime.fromtimestamp(int(funding_time) / 1000).astimezone(tz)

    @property
    def funding_rate(self) -> float:
        client = PublicData.PublicAPI(flag='0', debug=False)
        result = client.get_funding_rate(instId=self.inst_id)
        return float(result['data'][0]['fundingRate'])

    def __str__(self):
        return self.symbol


class SymbolManager(models.Manager):
    def get_queryset(self) -> QuerySet:
        return (
            super().get_queryset()
            .select_related('okx', 'binance')
        )

    def cache(self, **kwargs) -> QuerySet:
        timeout = kwargs.pop('_timeout', 60)
        key = f'symbols_{kwargs}'
        queryset = cache.get(key)
        if queryset is not None:
            return queryset
        else:
            queryset = self.filter(**kwargs).order_by('symbol')
            cache.set(key, queryset, timeout=timeout)
            return queryset


class Symbol(BaseModel):
    class Meta:
        verbose_name = 'Symbol'
        verbose_name_plural = 'Symbols'

    objects = SymbolManager()

    symbol = models.CharField(primary_key=True, unique=True, max_length=20)
    okx = models.ForeignKey('OkxSymbol', on_delete=models.CASCADE, related_name='okx', help_text='OKX Symbol')
    binance = models.ForeignKey('BinanceSymbol', on_delete=models.CASCADE, related_name='binance', help_text='Binance Symbol')
    is_active = models.BooleanField(default=True, help_text='Is active')

    def __str__(self):
        return self.symbol

    def __repr__(self):
        return self.__str__()


class StrategyManager(models.Manager):
    def get_queryset(self) -> QuerySet:
        return (
            super().get_queryset()
            .select_related('second_account')
            .prefetch_related('symbols', 'positions')
        )

    def cache(self, **kwargs) -> QuerySet:
        timeout = kwargs.pop('timeout', 60)
        key = f'strategies_{kwargs}'
        queryset = cache.get(key)
        if queryset is not None:
            return queryset
        else:
            queryset = self.filter(**kwargs).order_by('-id')
            cache.set(key, queryset, timeout=timeout)
            return queryset


class Strategy(BaseModel):
    class Meta:
        verbose_name = 'Strategy'
        verbose_name_plural = 'Strategies'

    class ClosePositionType(models.TextChoices):
        limit = 'limit', 'Limit'
        market = 'market', 'Market'

    class Mode(models.TextChoices):
        trade = 'trade', 'Trade'
        emulate = 'emulate', 'Emulate'

    objects = StrategyManager()

    name = models.CharField('name', max_length=255, blank=False, null=False, help_text='Strategy name')
    enabled = models.BooleanField('Enabled', default=True, help_text='Is enabled')
    task = models.ForeignKey('django_celery_beat.PeriodicTask', models.RESTRICT, verbose_name='Task', null=True)
    first_account = models.ForeignKey(Account, on_delete=models.CASCADE, related_name='strategies_set', help_text='Binance account', null=True)
    second_account = models.ForeignKey(Account, on_delete=models.CASCADE, related_name='strategies', help_text='OKX account', verbose_name='OKX account')
    symbols = models.ManyToManyField(Symbol, related_name='strategies', help_text='Symbols')
    position_size = models.FloatField('Position size', default=0.0, help_text='Max position size, USDT')
    taker_fee = models.FloatField('Taker fee', default=0.0, help_text='Taker fee, %, market order')
    maker_fee = models.FloatField('Maker fee', default=0.0, help_text='Maker fee, %, limit order')
    take_profit = models.FloatField('Take profit', default=0.0, help_text='Take profit, %')
    stop_loss = models.FloatField('Stop loss', default=0.0, help_text='Stop loss, %')
    close_position_type = models.CharField('Close position type', choices=ClosePositionType.choices, default=ClosePositionType.market, help_text='Close position type')
    time_to_close = models.IntegerField('Time to close', default=0, help_text='Time to close, seconds')
    close_position_parts = models.BooleanField('Close position parts', default=False)
    stop_loss_breakeven = models.BooleanField('Breakeven', default=False, help_text='Stop loss break even')
    tp_first_price_percent = models.FloatField('Take profit price', default=0.0, help_text='Take profit price for first part, %', blank=True)
    tp_first_part_percent = models.FloatField('First part', default=0.0, help_text='Take profit first part, %', blank=True)
    tp_second_price_percent = models.FloatField('Take profit price', default=0.0, help_text='Take profit price for second part, %', blank=True)
    tp_second_part_percent = models.FloatField('Second part', default=0.0, help_text='Take profit second part, %', blank=True)
    time_to_funding = models.IntegerField('Time to funding', default=0, help_text='Time to funding, minutes')
    only_profit = models.BooleanField('Only profit', default=False, help_text='Trading only in the direction of funding')
    mode = models.CharField('Mode', choices=Mode.choices, default=Mode.trade, help_text='Algorithm mode')
    search_duration = models.IntegerField('Search duration', default=0, help_text='Search duration, milliseconds')
    simultaneous_opening_positions = models.BooleanField('Simultaneous opening of positions', default=False)
    reverse = models.BooleanField('Reverse', default=False, help_text='Open positions in reverse direction')

    def _create_task(self) -> PeriodicTask:
        task_name = f'strategy_{self.id}'
        task = 'binance_okx.tasks.run_strategy'
        interval, _ = IntervalSchedule.objects.get_or_create(every=1, period='seconds')
        periodic_task, exists = PeriodicTask.objects.get_or_create(
            name=task_name,
            task=task,
            interval=interval,
            args=json.dumps([self.id,]),
            enabled=self.enabled
        )
        Strategy.objects.filter(pk=self.id).update(task=periodic_task)
        return periodic_task

    def _update_task(self) -> PeriodicTask:
        task_name = f'strategy_{self.id}'
        self.task.name = task_name
        self.task.enabled = self.enabled
        self.task.args = json.dumps([self.id,])
        self.task.save(update_fields=['name', 'enabled', 'args'])
        return self.task

    @property
    def extra_log(self) -> dict:
        return self._extra_log

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self._extra_log = dict(
            strategy=self,
            symbol=None,
            position=None
        )

    def get_last_open_trade_position(self, symbol: str) -> QuerySet:
        return self.positions.filter(
            symbol__symbol=symbol, is_open=True, mode=Strategy.Mode.trade).last()

    def get_last_open_emulate_position(self, symbol: str) -> QuerySet:
        return self.positions.filter(
            symbol__symbol=symbol, is_open=True, mode=Strategy.Mode.emulate).last()

    @property
    def open_fee(self) -> float:
        return self.taker_fee

    @property
    def close_fee(self) -> float:
        if self.close_position_type == 'market':
            close_fee = self.taker_fee
        if self.close_position_type == 'limit':
            close_fee = self.maker_fee
        return close_fee

    @property
    def open_plus_close_fee(self) -> float:
        return self.open_fee + self.close_fee

    def __str__(self):
        return f'{self.id}_{self.mode}_{self.name}'

    def __repr__(self):
        return self.__str__()


class BillManager(models.Manager):
    def get_queryset(self) -> QuerySet:
        return (
            super().get_queryset()
            .prefetch_related('account', 'symbol')
        )


class Bill(BaseModel):
    class Meta:
        verbose_name = 'Bill'
        verbose_name_plural = 'Bills'
        indexes = [
            models.Index(fields=['order_id'], name='bill_order_id_idx'),
            models.Index(fields=['trade_id'], name='bill_trade_id_idx')
        ]

    objects = BillManager()

    sub_type = {
        3: 'Open long',
        4: 'Open short',
        5: 'Close long',
        6: 'Close short'
    }

    @staticmethod
    def get_empty_data() -> dict:
        data = {
            'bal': None,
            'balChg': None,
            'billId': None,
            'ccy': None,
            'clOrdId': None,
            'execType': None,
            'fee': None,
            'fillFwdPx': None,
            'fillIdxPx': None,
            'fillMarkPx': None,
            'fillMarkVol': None,
            'fillPxUsd': None,
            'fillPxVol': None,
            'fillTime': None,
            'from': None,
            'instId': None,
            'instType': None,
            'interest': None,
            'mgnMode': None,
            'notes': None,
            'ordId': None,
            'pnl': None,
            'posBal': None,
            'posBalChg': None,
            'px': None,
            'subType': None,
            'sz': None,
            'tag': None,
            'to': None,
            'tradeId': None,
            'ts': None,
            'type': None
        }
        return data

    bill_id = models.BigIntegerField('Bill ID', primary_key=True)
    order_id = models.BigIntegerField('Order ID', blank=True, null=True)
    trade_id = models.BigIntegerField('Trade ID', blank=True, null=True)
    account = models.ForeignKey('Account', on_delete=models.PROTECT, related_name='bills')
    data = models.JSONField('Data', default=dict)
    mode = models.CharField('Mode', max_length=20, blank=True, null=True)
    symbol = models.ForeignKey('Symbol', on_delete=models.PROTECT, related_name='bills', null=True)

    @property
    def amount_usdt(self) -> float:
        from .helper import calc
        base_coin = calc.get_base_coin_from_sz(
            self.data['sz'], self.symbol.okx.ct_val
        )
        usdt = base_coin * self.data['px']
        return round(usdt, 2)

    def __str__(self):
        return str(self.trade_id)


class OrderManager(models.Manager):
    def get_queryset(self) -> QuerySet:
        return (
            super().get_queryset()
            .prefetch_related('account', 'symbol')
        )


class Order(BaseModel):
    class Meta:
        verbose_name = 'Order'
        verbose_name_plural = 'Orders'
        indexes = [
            models.Index(fields=['order_id'], name='order_order_id_idx'),
            models.Index(fields=['trade_id'], name='order_trade_id_idx')
        ]

    objects = OrderManager()

    order_id = models.BigIntegerField('Order ID')
    trade_id = models.BigIntegerField('Trade ID', blank=True, null=True)
    account = models.ForeignKey('Account', on_delete=models.PROTECT, related_name='orders')
    data = models.JSONField('Data', default=dict)
    symbol = models.ForeignKey(Symbol, on_delete=models.PROTECT, related_name='orders', null=True)

    def __str__(self):
        return str(self.order_id)


class PositionManager(models.Manager):
    def get_queryset(self) -> QuerySet:
        return (
            super().get_queryset()
            .prefetch_related('symbol', 'strategy', 'account')
        )

    def create(self, *args, **kwargs):
        kwargs['sl_tp_data'] = Position.get_sl_tp_empty_data()
        kwargs['ask_bid_data'] = Position.get_ask_bid_empty_data()
        position = super().create(*args, **kwargs)
        return position

    def cache(self, **kwargs) -> QuerySet:
        timeout = kwargs.pop('timeout', 10)
        key = f'positions_{kwargs}'
        queryset = cache.get(key)
        if queryset is not None:
            return queryset
        else:
            queryset = self.filter(**kwargs).order_by('-id')
            cache.set(key, queryset, timeout=timeout)
            return queryset


class Position(BaseModel):
    class Meta:
        verbose_name = 'Position'
        verbose_name_plural = 'Positions'

    objects = PositionManager()

    @staticmethod
    def get_sl_tp_empty_data() -> dict:
        data = {
            'stop_loss_price': None,
            'stop_loss_order_id': None,
            'stop_loss_breakeven': None,
            'stop_loss_breakeven_order_id': None,
            'take_profit_price': None,
            'tp_first_price': None,
            'tp_first_part': None,
            'tp_second_price': None,
            'tp_second_part': None,
            'tp_first_limit_order_id': None,
            'tp_second_limit_order_id': None,
            'first_part_closed': None,
            'second_part_closed': None,
            'third_part_closed': None,
            'fourth_part_closed': None,
            'increased_position': None,
            'tp_third_limit_order_id': None,
            'tp_fourth_limit_order_id': None,
            'tp_third_price': None,
            'tp_third_part': None,
            'tp_fourth_price': None,
            'tp_fourth_part': None,
        }
        return data

    @staticmethod
    def get_position_empty_data() -> dict:
        data = {
            'adl': None,
            'availPos': None,
            'avgPx': None,
            'baseBal': None,
            'baseBorrowed': None,
            'baseInterest': None,
            'bePx': None,
            'bizRefId': None,
            'bizRefType': None,
            'cTime': None,
            'ccy': None,
            'clSpotInUseAmt': None,
            'closeOrderAlgo': None,
            'deltaBS': None,
            'deltaPA': None,
            'fee': None,
            'fundingFee': None,
            'gammaBS': None,
            'gammaPA': None,
            'idxPx': None,
            'imr': None,
            'instId': None,
            'instType': None,
            'interest': None,
            'last': None,
            'lever': None,
            'liab': None,
            'liabCcy': None,
            'liqPenalty': None,
            'liqPx': None,
            'margin': None,
            'markPx': None,
            'maxSpotInUseAmt': None,
            'mgnMode': None,
            'mgnRatio': None,
            'mmr': None,
            'notionalUsd': None,
            'optVal': None,
            'pendingCloseOrdLiabVal': None,
            'pnl': None,
            'pos': None,
            'posCcy': None,
            'posId': None,
            'posSide': None,
            'quoteBal': None,
            'quoteBorrowed': None,
            'quoteInterest': None,
            'realizedPnl': None,
            'spotInUseAmt': None,
            'spotInUseCcy': None,
            'thetaBS': None,
            'thetaPA': None,
            'tradeId': None,
            'uTime': None,
            'upl': None,
            'uplLastPx': None,
            'uplRatio': None,
            'uplRatioLastPx': None,
            'usdPx': None,
            'vegaBS': None,
            'vegaPA': None
        }
        return data

    @staticmethod
    def get_ask_bid_empty_data() -> dict:
        data = {
            'binance_previous_ask': None,
            'binance_last_ask': None,
            'binance_previous_bid': None,
            'binance_last_bid': None,
            'okx_previous_ask': None,
            'okx_last_ask': None,
            'okx_previous_bid': None,
            'okx_last_bid': None,
            'delta_points': None,
            'delta_percent': None,
            'target_delta': None,
            'spread_points': None,
            'spread_percent': None,
            'binance_last_ask_entry': None,
            'binance_last_bid_entry': None,
            'okx_last_ask_entry': None,
            'okx_last_bid_entry': None,
            'delta_points_entry': None,
            'delta_percent_entry': None,
            'spread_points_entry': None,
            'spread_percent_entry': None,
            'date_time_last_prices': None
        }
        return data

    position_data = models.JSONField('Position data', default=dict)
    sl_tp_data = models.JSONField('SL/TP data', default=dict)
    ask_bid_data = models.JSONField('Ask/Bid data', default=dict)
    symbol = models.ForeignKey(Symbol, on_delete=models.PROTECT, related_name='positions', blank=True, null=True)
    strategy = models.ForeignKey(Strategy, on_delete=models.PROTECT, related_name='positions', blank=True, null=True)
    is_open = models.BooleanField('Is open', default=True)
    mode = models.CharField('Mode', max_length=20, default=Strategy.Mode.trade)
    account = models.ForeignKey(Account, on_delete=models.CASCADE, related_name='positions', help_text='OKX account', blank=True, null=True)
    trade_ids = models.JSONField('Trade IDs', default=list)

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self._is_open = self.is_open
        self._stop_loss_breakeven_set = self.stop_loss_breakeven_set
        self._increased = self.increased

    @property
    def stop_loss_breakeven_set(self) -> bool:
        if self.sl_tp_data['stop_loss_breakeven_order_id']:
            return True
        else:
            return False

    @property
    def increased(self) -> bool:
        return self.sl_tp_data['increased_position']

    def add_trade_id(self, trade_id: int) -> None:
        with transaction.atomic():
            trade_ids = set(
                Position.objects.values_list('trade_ids', flat=True).get(id=self.id)
            )
            trade_ids.add(trade_id)
            trade_ids = sorted([i for i in trade_ids if i])
            Position.objects.filter(id=self.id).update(trade_ids=trade_ids)
        logger.info(
            f'Add {trade_id=} to position trade_ids',
            extra=self.strategy.extra_log | dict(position=self.id)
        )

    @property
    def side(self) -> str:
        return self.position_data['posSide']

    @property
    def size_usdt(self) -> float:
        return self.position_data['notionalUsd']

    @property
    def sz(self) -> float:
        return self.position_data['pos']

    @property
    def entry_price(self) -> float:
        return self.position_data['avgPx']

    @property
    def bills(self) -> QuerySet:
        return (
            Bill.objects.filter(
                order_id__in=(
                    Bill.objects.filter(trade_id__in=self.trade_ids)
                    .values_list('order_id', flat=True)
                )
            ).annotate(
                position=Subquery(
                    Position.objects.filter(id=self.id)
                    .annotate(
                        data=JSONObject(
                            id=F('id'),
                            data=F('position_data'),
                            ask_bid=F('ask_bid_data')
                        )
                    ).values('data')
                )
            ).order_by('bill_id')
        )

    def __str__(self):
        return str(self.id)
