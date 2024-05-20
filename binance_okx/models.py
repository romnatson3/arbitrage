import logging
import json
from datetime import datetime
from django.utils import timezone
import okx.PublicData as PublicData
from django.db import models
from django.db.models import QuerySet
from django.core.cache import cache
from django.contrib.auth.models import AbstractUser
from django_celery_beat.models import PeriodicTask, IntervalSchedule


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
            .select_related('strategy', 'created_by')
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
    created_by = models.ForeignKey(
        User, on_delete=models.CASCADE, related_name='status_logs',
        help_text='Created by', null=True
    )
    strategy = models.ForeignKey(
        'Strategy', on_delete=models.CASCADE, related_name='status_logs',
        help_text='Strategy', null=True
    )
    symbol = models.CharField(max_length=20, blank=True, null=True)


class Account(BaseModel):
    class Meta:
        verbose_name = 'Account'
        verbose_name_plural = 'Accounts'

    class Exchange(models.TextChoices):
        binance = 'binance', 'Binance'
        okx = 'okx', 'OKX'

    name = models.CharField('name', max_length=255, blank=False, null=False, unique=True, help_text='Account name')
    exchange = models.CharField('exchange', choices=Exchange.choices, help_text='Exchange', default=Exchange.binance)
    api_key = models.CharField('api_key', max_length=255, blank=False, null=False, help_text='API key')
    api_secret = models.CharField('api_secret', max_length=255, blank=False, null=False, help_text='API secret')
    api_passphrase = models.CharField('api_passphrase', max_length=255, blank=True, null=True, help_text='API passphrase')
    testnet = models.BooleanField('testnet', default=False)
    created_by = models.ForeignKey(User, on_delete=models.PROTECT, related_name='accounts', help_text='Created by')

    def __str__(self):
        return f'{self.exchange} - {self.name}'


class BinanceSymbol(BaseModel):
    class Meta:
        verbose_name = 'Binance Symbol'
        verbose_name_plural = 'Binance Symbols'

    symbol = models.CharField(primary_key=True, unique=True, max_length=20)
    data = models.JSONField(default=dict, help_text='Instrument data')

    def __str__(self):
        return self.symbol


class OkxSymbol(BaseModel):
    class Meta:
        verbose_name = 'OKX Symbol'
        verbose_name_plural = 'OKX Symbols'

    symbol = models.CharField(primary_key=True, unique=True, max_length=20)
    data = models.JSONField(default=dict, help_text='Instrument data')

    @property
    def inst_id(self) -> str:
        return self.data['instId']

    @property
    def lot_sz(self) -> str:
        return float(self.data['lotSz'])

    @property
    def ct_val(self) -> float:
        return float(self.data['ctVal'])

    @property
    def market_price(self) -> float:
        return cache.get(f'okx_market_price_{self.symbol}', 0.0)

    @property
    def funding_time(self) -> datetime:
        client = PublicData.PublicAPI(flag='0', debug=False)
        result = client.get_funding_rate(instId=self.inst_id)
        funding_time = result['data'][0]['fundingTime']
        tz = timezone.get_current_timezone()
        return datetime.fromtimestamp(int(funding_time) / 1000).astimezone(tz)

    def __str__(self):
        return self.symbol


class SymbolManager(models.Manager):
    def get_queryset(self) -> QuerySet:
        return (
            super().get_queryset()
            .select_related('okx', 'binance')
        )


class Symbol(BaseModel):
    class Meta:
        verbose_name = 'Symbol'
        verbose_name_plural = 'Symbols'

    objects = SymbolManager()

    symbol = models.CharField(primary_key=True, unique=True, max_length=20)
    okx = models.ForeignKey('OkxSymbol', on_delete=models.CASCADE, related_name='okx', help_text='OKX Symbol')
    binance = models.ForeignKey('BinanceSymbol', on_delete=models.CASCADE, related_name='binance', help_text='Binance Symbol')

    def __str__(self):
        return self.symbol


class Candle(BaseModel):
    class Meta:
        abstract = True
        verbose_name = 'Candle'
        verbose_name_plural = 'Candles'
        unique_together = ('symbol', 'time_frame')

    symbol = models.ForeignKey('Symbol', on_delete=models.CASCADE, related_name='candles', help_text='Symbol')
    time_frame = models.CharField('interval', max_length=10, help_text='Interval')
    data = models.JSONField(default=list, help_text='Candles data')

    def __str__(self):
        return f'{self.symbol} - {self.time_frame}'


class BinanceCandle(Candle):
    class Meta:
        verbose_name = 'Binance Candle'
        verbose_name_plural = 'Binance Candles'

    symbol = models.ForeignKey('BinanceSymbol', on_delete=models.CASCADE, related_name='candles', help_text='Binance Symbol')

    def __str__(self):
        return f'Binance {self.symbol} - {self.time_frame}'


class OkxCandle(Candle):
    class Meta:
        verbose_name = 'OKX Candle'
        verbose_name_plural = 'OKX Candles'

    symbol = models.ForeignKey('OkxSymbol', on_delete=models.CASCADE, related_name='candles', help_text='OKX Symbol')

    def __str__(self):
        return f'OKX {self.symbol} - {self.time_frame}'


class StrategyManager(models.Manager):
    def get_queryset(self) -> QuerySet:
        return (
            super().get_queryset()
            .select_related('first_account', 'second_account', 'created_by')
            .prefetch_related('symbols')
        )

    def cache(self, **kwargs) -> QuerySet:
        set = kwargs.pop('set', False)
        key = f'strategies_{kwargs}'
        if set:
            queryset = self.filter(**kwargs)
            cache.set(key, queryset, timeout=60)
            return queryset
        queryset = cache.get(key)
        if queryset is not None:
            return queryset
        else:
            queryset = self.filter(**kwargs)
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
    first_account = models.ForeignKey(Account, on_delete=models.CASCADE, related_name='strategies_set', help_text='First Account')
    second_account = models.ForeignKey(Account, on_delete=models.CASCADE, related_name='strategies', help_text='Second Account')
    symbols = models.ManyToManyField(Symbol, related_name='strategies', help_text='Symbols')
    created_by = models.ForeignKey(User, on_delete=models.PROTECT, related_name='strategies', help_text='Created by')
    position_size = models.FloatField('Position size', default=0.0, help_text='Max position size, USDT')
    taker_fee = models.FloatField('Taker fee', default=0.0, help_text='Taker fee, %, market order')
    maker_fee = models.FloatField('Maker fee', default=0.0, help_text='Maker fee, %, limit order')
    target_profit = models.FloatField('Target profit', default=0.0, help_text='Target profit, %')
    stop_loss = models.FloatField('Stop loss', default=0.0, help_text='Stop loss, %')
    close_position_type = models.CharField('Close position type', choices=ClosePositionType.choices, default=ClosePositionType.market, help_text='Close position type')
    time_to_close = models.IntegerField('Time to close', default=0, help_text='Time to close, seconds')
    close_position_parts = models.BooleanField('Close position parts', default=False)
    first_part_size = models.FloatField('First part size', default=0.0, help_text='First part size, %')
    second_part_size = models.FloatField('Second part size', default=0.0, help_text='Second part size, %')
    time_to_funding = models.IntegerField('Time to funding', default=0, help_text='Time to funding, minutes')
    only_profit = models.BooleanField('Only profit', default=False, help_text='Trading only in the direction of funding')
    logging = models.BooleanField('logging', default=False, help_text='Logging enabled')
    mode = models.CharField('Mode', choices=Mode.choices, default=Mode.trade, help_text='Algorithm mode')

    def _create_task(self) -> PeriodicTask:
        task_name = f'strategy_{self.id}'
        task = 'binance_okx.tasks.run_strategy'
        interval, _ = IntervalSchedule.objects.get_or_create(every=2, period='seconds')
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
            created_by=self.created_by if hasattr(self, 'created_by') else None,
            strategy=self,
            symbol=None
        )

    @property
    def fee(self) -> float:
        if self.close_position_type == 'market':
            fee = self.taker_fee
        else:
            fee = self.maker_fee
        return fee

    def __str__(self):
        return f'{self.id}_{self.name}'
