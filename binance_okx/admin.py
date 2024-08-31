import logging
import csv
from types import SimpleNamespace as Namespace
from django.contrib import admin
from django.utils import timezone
from django.utils.html import format_html
from django.contrib.auth.admin import UserAdmin as BaseUserAdmin
from django.contrib.auth import get_user_model
from django.db.models import QuerySet
from django.http import HttpResponse
from io import StringIO
from .forms import CustomUserCreationForm, CustomUserChangeForm
from .models import (
    StatusLog, Account, OkxSymbol, BinanceSymbol, Strategy, Symbol, Position, Execution, Bill
)
from .misc import get_pretty_dict, get_pretty_text, sort_data
from .helper import calc
from .forms import StrategyForm
from .filters import (
    PositionSideFilter, PositionStrategyFilter, PositionSymbolFilter,
    BillInstrumentFilter, BillSubTypeFilter
)


User = get_user_model()
logger = logging.getLogger(__name__)


admin.site.site_header = 'Binance-OKX'
admin.site.site_title = 'Binance-OKX'
admin.site.index_title = 'Binance-OKX'


@admin.register(User)
class UserAdmin(BaseUserAdmin):
    add_form = CustomUserCreationForm
    form = CustomUserChangeForm
    fieldsets = (
        ('Personal info', {'fields': ('username', 'email')}),
        (
            'Permissions',
            {
                'fields': (
                    'is_active',
                    'is_staff',
                    'is_superuser',
                    'groups',
                    'user_permissions',
                ),
            },
        ),
        ('Important dates', {'fields': ('last_login', 'date_joined')}),
    )
    list_display = ['username', 'is_staff', 'is_superuser']
    search_fields = ['username']


@admin.register(StatusLog)
class StatusLogAdmin(admin.ModelAdmin):
    list_display = (
        'colored_msg', 'position', 'strategy', 'symbol', 'create_datetime_format'
    )
    list_display_links = ('colored_msg',)
    list_filter = ('level', 'symbol', 'position')
    list_per_page = 500
    search_fields = ('msg', 'trace', 'position', 'symbol')
    fields = (
        'level', 'colored_msg', 'traceback', 'create_datetime_format', 'created_by',
        'strategy', 'symbol', 'position'
    )
    actions = ['delete_all']

    @admin.display(description='Message')
    def colored_msg(self, obj):
        if obj.level in [logging.NOTSET, logging.INFO]:
            color = 'gray'
        elif obj.level in [logging.WARNING, logging.DEBUG]:
            color = 'orange'
        else:
            color = 'red'
        return format_html(
            '<pre style="color:{color}; white-space: pre-wrap; font-size: 1.02em;">{msg}</pre>',
            color=color,
            msg=obj.msg
        )

    @admin.action(description='Delete all')
    def delete_all(self, request, queryset):
        StatusLog.objects.all().delete()
        self.message_user(request, 'All logs deleted')

    def get_queryset(self, request) -> QuerySet[StatusLog]:
        qs = super().get_queryset(request)
        if request.user.is_superuser:
            return qs
        return qs.filter(created_by=request.user)

    def traceback(self, obj):
        return format_html(
            '<pre><code>{content}</code></pre>',
            content=obj.trace if obj.trace else ''
        )

    @admin.display(description='Created at', ordering='created_at')
    def create_datetime_format(self, obj):
        return timezone.localtime(obj.created_at).strftime('%d-%m-%Y %H:%M:%S.%f')

    # def has_delete_permission(self, request, obj=None):
    #     return False

    def has_add_permission(self, request):
        return False

    def has_change_permission(self, request, obj=None):
        return False


@admin.register(Account)
class AccountAdmin(admin.ModelAdmin):
    list_display = ('id', 'name', 'exchange', 'testnet', 'api_key', 'api_secret')
    search_fields = ('name',)
    list_filter = ('name', 'testnet')
    fields = ('id', 'name', 'exchange', 'api_key', 'api_secret', 'api_passphrase', 'testnet')
    list_display_links = ('id', 'name')
    readonly_fields = ('id',)

    def get_queryset(self, request) -> QuerySet:
        qs = super().get_queryset(request)
        if request.user.is_superuser:
            return qs
        return qs.filter(created_by=request.user)

    def save_model(self, request, obj, form, change):
        if not obj.pk:
            obj.created_by = request.user
        super().save_model(request, obj, form, change)


@admin.register(Symbol)
class SymbolAdmin(admin.ModelAdmin):
    list_display = ('is_active', 'symbol', 'updated_at')
    search_fields = ('symbol',)
    fields = ('symbol', 'is_active', 'okx', 'binance', 'updated_at', 'created_at')
    ordering = ('symbol',)
    readonly_fields = ('symbol', 'okx', 'binance', 'updated_at', 'created_at')
    list_filter = ('is_active',)
    list_display_links = ('symbol',)
    list_per_page = 500

    def get_search_results(self, request, queryset, search_term):
        queryset, use_distinct = super().get_search_results(request, queryset, search_term)
        if request.headers.get('x-requested-with') == 'XMLHttpRequest':
            return Symbol.objects.filter(
                # ~Q(symbol__in=Strategy.objects.values_list('symbols', flat=True).filter(enabled=True)),
                symbol__icontains=search_term, is_active=True
            ), use_distinct
        return queryset, use_distinct


@admin.register(BinanceSymbol)
class BinanceSymbolAdmin(admin.ModelAdmin):
    list_display = ('is_active', 'symbol', 'updated_at')
    fields = ('symbol', 'is_active', 'pretty_data', 'updated_at', 'created_at')
    search_fields = ('symbol',)
    ordering = ('symbol',)
    list_display_links = ('symbol',)
    list_filter = ('is_active',)
    list_per_page = 500

    @admin.display(description='Data')
    def pretty_data(self, obj) -> str:
        return get_pretty_dict(obj.data)

    def has_delete_permission(self, request, obj=None):
        return True

    def has_add_permission(self, request):
        return False

    def has_change_permission(self, request, obj=None):
        return False


@admin.register(OkxSymbol)
class OkxSymbolAdmin(admin.ModelAdmin):
    list_display = ('is_active', 'symbol', 'updated_at')
    fields = ('symbol', 'is_active', 'pretty_data', 'updated_at', 'created_at')
    search_fields = ('symbol',)
    ordering = ('symbol',)
    list_display_links = ('symbol',)
    list_filter = ('is_active',)
    list_per_page = 500

    @admin.display(description='Data')
    def pretty_data(self, obj) -> str:
        return get_pretty_dict(obj.data)

    def has_delete_permission(self, request, obj=None):
        return True

    def has_add_permission(self, request):
        return False

    def has_change_permission(self, request, obj=None):
        return False


@admin.register(Strategy)
class StrategyAdmin(admin.ModelAdmin):
    class Media:
        js = ('binance_okx/strategy.js',)

    list_display = (
        'id', 'name', 'enabled', 'mode', 'position_size', 'close_position_type',
        'close_position_parts', '_symbols', 'updated_at'
    )
    search_fields = ('name',)
    list_filter = ()
    fieldsets = (
        (None, {'fields': (
            'id', 'name', 'enabled', 'mode', 'search_duration',
            'simultaneous_opening_positions', 'second_account'
        )}),
        # (None, {'fields': (('first_account', 'second_account'),)}),
        (None, {'fields': ('symbols',)}),
        (
            None,
            {
                'fields': (
                    'position_size', ('taker_fee', 'maker_fee'), 'take_profit', 'stop_loss',
                    'close_position_type', 'time_to_close', 'time_to_funding', 'only_profit'
                )
            }
        ),
        (
            'Take profit',
            {
                'fields': (
                    'close_position_parts', 'stop_loss_breakeven',
                    ('tp_first_price_percent', 'tp_first_part_percent'),
                    ('tp_second_price_percent')
                    # ('tp_second_price_percent', 'tp_second_part_percent')
                )
            }
        ),
        (None, {'fields': ('updated_at', 'created_at')}),
    )
    list_display_links = ('id', 'name')
    readonly_fields = ('id', 'updated_at', 'created_at')
    autocomplete_fields = ('second_account', 'symbols')
    save_on_top = True
    form = StrategyForm
    actions = ['toggle_enabled']

    def get_queryset(self, request) -> QuerySet:
        qs = super().get_queryset(request)
        if request.user.is_superuser:
            return qs
        return qs.filter(created_by=request.user)

    def save_model(self, request, obj: Strategy, form: StrategyForm, change):
        if not obj.pk:
            obj.created_by = request.user
        super().save_model(request, obj, form, change)

    @admin.display(description='Symbols')
    def _symbols(self, obj) -> str:
        return ', '.join(obj.symbols.values_list('symbol', flat=True))

    @admin.action(description='Toggle enabled/disabled')
    def toggle_enabled(self, request, queryset):
        for strategy in queryset:
            strategy.enabled = not strategy.enabled
            strategy.save()


@admin.register(Position)
class PositionAdmin(admin.ModelAdmin):
    list_display = (
        'id', 'is_open', 'strategy', '_position_side', 'mode', 'symbol', '_position_id',
        '_trade_ids', '_contract', '_amount', '_duration', 'updated_at'
    )
    fields = (
        'id', 'is_open', 'strategy', 'symbol', 'mode', 'account', '_trade_ids', '_position_data',
        '_sl_tp_data', '_ask_bid_data', 'updated_at', 'created_at'
    )
    search_fields = ('strategy__name', 'symbol__symbol')
    list_display_links = ('id', 'strategy')
    readonly_fields = (
        'id', 'updated_at', 'created_at', '_position_data', '_sl_tp_data',
        '_ask_bid_data'
    )
    list_filter = ('is_open', 'mode', PositionSideFilter, PositionStrategyFilter, PositionSymbolFilter)
    actions = ['export_csv_action', 'toggle_open_close', 'manual_fill_execution']

    def get_queryset(self, request) -> QuerySet:
        qs = super().get_queryset(request)
        if request.user.is_superuser:
            return qs.prefetch_related('executions')
        return qs.prefetch_related('executions').filter(strategy__created_by=request.user)

    # def has_delete_permission(self, request, obj=None):
    #     return False

    def has_add_permission(self, request):
        return False

    def has_change_permission(self, request, obj=None):
        return False

    @admin.display(description='Position data')
    def _position_data(self, obj) -> str:
        data: dict = sort_data(obj.position_data, Position.get_position_empty_data())
        return get_pretty_text(data)

    @admin.display(description='SL/TP data')
    def _sl_tp_data(self, obj) -> str:
        data: dict = sort_data(obj.sl_tp_data, Position.get_sl_tp_empty_data())
        return get_pretty_text(data)

    @admin.display(description='Ask/Bid data')
    def _ask_bid_data(self, obj) -> str:
        data: dict = sort_data(obj.ask_bid_data, Position.get_ask_bid_empty_data())
        return get_pretty_text(data)

    @admin.display(description='Position ID')
    def _position_id(self, obj) -> str:
        return obj.position_data.get('posId', '')

    @admin.display(description='Trade IDs')
    def _trade_ids(self, obj) -> str:
        if not obj.trade_ids:
            return ''
        return ', '.join(map(lambda x: str(x), obj.trade_ids))

    @admin.display(description='Contract size')
    def _contract(self, obj) -> str:
        pos = obj.position_data.get('pos')
        if pos:
            return round(float(pos), 2)
        return 0

    @admin.display(description='USDT amount')
    def _amount(self, obj) -> str:
        usdt = obj.position_data.get('notionalUsd')
        if usdt:
            return round(float(usdt), 2)
        return 0

    @admin.display(description='Position side')
    def _position_side(self, obj) -> str:
        return obj.position_data.get('posSide', '')

    @admin.display(description='Duration, s')
    def _duration(self, obj) -> int:
        duration = 0
        open_time = timezone.datetime.strptime(obj.position_data['cTime'], '%d-%m-%Y %H:%M:%S.%f')
        if obj.is_open:
            open_time = open_time.astimezone(timezone.get_current_timezone())
            duration = (timezone.localtime() - open_time).total_seconds()
        else:
            executions = list(obj.executions.all())
            if executions:
                executions.sort(key=lambda x: x.bill_id)
                last_execution = executions[-1]
                close_time = timezone.datetime.strptime(last_execution.data['ts'], '%d-%m-%Y %H:%M:%S.%f')
                duration = (close_time - open_time).total_seconds()
        return f'{duration:.3f}'

    @admin.action(description='Toggle open/close')
    def toggle_open_close(self, request, queryset):
        for position in queryset:
            position.is_open = not position.is_open
            position.save()
        self.message_user(request, f'Positions toggled: {queryset.count()}')

    @admin.action(description='Pull execution data manually')
    def manual_fill_execution(self, request, queryset):
        for position in queryset:
            position.strategy._extra_log.update(symbol=position.symbol.symbol, position=position.id)
            order_ids = Bill.objects.filter(data__tradeId__in=position.trade_ids).values_list('data__ordId', flat=True)
            bills = Bill.objects.filter(data__ordId__in=order_ids).all()
            executions = []
            for bill in bills:
                executions.append(
                    Execution(
                        bill_id=bill.bill_id, trade_id=bill.data['tradeId'],
                        data=bill.data, position=position
                    )
                )
            logger.info(
                f'Found {len(bills)} execution for position {position.id}',
                extra=position.strategy._extra_log
            )
            if bills:
                executions = Execution.objects.bulk_create(executions, ignore_conflicts=True)
                self.message_user(
                    request,
                    f'Found {len(executions)} executions for position {position.id}',
                    level='WARNING'
                )
            else:
                self.message_user(request, f'No executions found for position {position.id}', level='ERROR')

    @admin.action(description='Export CSV')
    def export_csv_action(self, request, queryset):
        f = StringIO()
        writer = csv.writer(f, delimiter=';')
        headers = [
            'ІД Позиції', 'Монета', 'Дата', 'Чаc', 'Тип (trade/emulate)', 'Позиція (шорт/лонг)', 'Тип (open/close)',
            'Аск_1 біржа№1 (парсинг)', 'Аск_2 біржа№1 (парсинг)',
            'Бід_1 біржа№1 (парсинг)', 'Бід_2 біржа№1 (парсинг)',
            'Аск_1 біржа№2 (парсинг)', 'Аск_2 біржа№2 (парсинг)',
            'Бід_1 біржа№2 (парсинг)', 'Бід_2 біржа№2 (парсинг)',
            'Дельта в пунктах (парсинг)', 'Дельта в % (парсинг)', 'Дельта цільова в %',
            'Спред біржа №2 в пунктах (парсинг)', 'Спред біржа №2 в % (парсинг)',
            'Аск_2 біржа№1 (вхід)', 'Бід_2 біржа№1 (вхід)',
            'Аск_2 біржа№2 (вхід)', 'Бід_2 біржа№2 (вхід)',
            'Дельта в пунктах (вхід)', 'Дельта в % (вхід)',
            'Спред біржа №2 в пунктах (вхід)', 'Спред біржа №2 в % (вхід)',
            'Обсяг в USDT', 'Дата відкриття', 'Час відкриття', 'Ціна', 'Час закриття',
            'Тривалість угоди в мілісекундах',
            'Комісія', 'Прибуток'
        ]
        writer.writerow(headers)
        executions = (
            Execution.objects.filter(position__in=queryset)
            .order_by('position__id', 'bill_id').all()
            # .order_by('position__id', '-data__subType', 'data__ts').all()
        )
        for execution in executions:
            ask_bid_data = Namespace(**execution.position.ask_bid_data)
            data = Namespace(**execution.data)
            position_data = Namespace(**execution.position.position_data)
            duration = round((
                timezone.datetime.strptime(data.ts, '%d-%m-%Y %H:%M:%S.%f') -
                timezone.datetime.strptime(position_data.cTime, '%d-%m-%Y %H:%M:%S.%f')
            ).total_seconds() * 1000)
            is_open = 'open' in data.subType.lower()
            base_coin = calc.get_base_coin_from_sz(data.sz, execution.position.symbol.okx.ct_val)
            usdt = round(base_coin * data.px, 2)
            if is_open:
                row = [
                    execution.position.id,
                    execution.position.symbol.symbol,
                    ask_bid_data.date_time_last_prices.split(' ')[0],
                    ask_bid_data.date_time_last_prices.split(' ')[1],
                    execution.position.mode,
                    position_data.posSide,
                    data.subType,
                    ask_bid_data.binance_previous_ask,
                    ask_bid_data.binance_last_ask,
                    ask_bid_data.binance_previous_bid,
                    ask_bid_data.binance_last_bid,
                    ask_bid_data.okx_previous_ask,
                    ask_bid_data.okx_last_ask,
                    ask_bid_data.okx_previous_bid,
                    ask_bid_data.okx_last_bid,
                    ask_bid_data.delta_points,
                    ask_bid_data.delta_percent,
                    ask_bid_data.target_delta,
                    ask_bid_data.spread_points,
                    ask_bid_data.spread_percent,
                    ask_bid_data.binance_last_ask_entry,
                    ask_bid_data.binance_last_bid_entry,
                    ask_bid_data.okx_last_ask_entry,
                    ask_bid_data.okx_last_bid_entry,
                    ask_bid_data.delta_points_entry,
                    ask_bid_data.delta_percent_entry,
                    ask_bid_data.spread_points_entry,
                    ask_bid_data.spread_percent_entry,
                    usdt,
                    position_data.cTime.split(' ')[0],
                    position_data.cTime.split(' ')[1],
                    data.px,
                    None if is_open else data.ts.split(' ')[1],
                    None if is_open else duration,
                    data.fee,
                    None if is_open else f'{data.pnl:.5f}'
                ]
            else:
                row = [
                    execution.position.id,
                    execution.position.symbol.symbol,
                    '',
                    '',
                    execution.position.mode,
                    position_data.posSide,
                    data.subType,
                    '',
                    '',
                    '',
                    '',
                    '',
                    '',
                    '',
                    '',
                    '',
                    '',
                    '',
                    '',
                    '',
                    '',
                    '',
                    '',
                    '',
                    '',
                    '',
                    '',
                    '',
                    usdt,
                    '',
                    '',
                    data.px,
                    '' if is_open else data.ts.split(' ')[1],
                    '' if is_open else duration,
                    data.fee,
                    '' if is_open else f'{data.pnl:.5f}'
                ]
            d = dict(zip(headers, row))
            for i, j in d.items():
                if isinstance(j, float):
                    if i in ['Дельта в пунктах (парсинг)', 'Спред біржа №2 в пунктах (парсинг)',
                             'Дельта в пунктах (вхід)', 'Спред біржа №2 в пунктах (вхід)']:
                        d[i] = f'{j:.2f}'.replace('.', ',')
                    elif i in ['Дельта в % (парсинг)', 'Дельта цільова в %',
                               'Спред біржа №2 в % (парсинг)', 'Дельта в % (вхід)',
                               'Спред біржа №2 в % (вхід)']:
                        d[i] = f'{j:.5f}'.replace('.', ',')
                    elif i in ['Комісія']:
                        d[i] = f'{j:.8f}'.replace('.', ',')
                    else:
                        d[i] = str(j).replace('.', ',')
                if isinstance(j, str):
                    if i in ['Прибуток']:
                        d[i] = f'{j}'.replace('.', ',')
            writer.writerow(d.values())
        f.seek(0)
        response = HttpResponse(f.read(), content_type='text/csv', charset='utf-8-sig')
        response['Content-Disposition'] = 'attachment; filename="positions.csv"'
        return response


@admin.register(Execution)
class ExecutionAdmin(admin.ModelAdmin):
    list_display = (
        'position', '_type', '_symbol', '_contract', '_amount', '_px', '_pnl',
        'bill_id', 'trade_id', 'created_at'
    )
    fields = (
        'id', 'position', 'bill_id', 'trade_id', '_data', 'updated_at', 'created_at'
    )
    list_display_links = ('position',)
    search_fields = ('position__id',)
    list_filter = (
        'position__strategy', 'position__mode', 'position__is_open', 'position__id'
    )
    ordering = ('-position', '-bill_id')

    def get_queryset(self, request) -> QuerySet:
        qs = super().get_queryset(request)
        if request.user.is_superuser:
            return qs
        return qs.filter(position__strategy__created_by=request.user)

    # def has_delete_permission(self, request, obj=None):
    #     return False

    def has_add_permission(self, request):
        return False

    def has_change_permission(self, request, obj=None):
        return False

    @admin.display(description='Data')
    def _data(self, obj) -> str:
        data: dict = sort_data(obj.data, Execution.get_empty_data())
        return get_pretty_text(data)

    @admin.display(description='Type', ordering='_type')
    def _type(self, obj) -> str:
        return obj.data.get('subType', '')

    @admin.display(description='Contract size')
    def _contract(self, obj) -> str:
        return obj.data.get('sz', '')

    @admin.display(description='USDT amount')
    def _amount(self, obj) -> str:
        base_coin = calc.get_base_coin_from_sz(
            obj.data['sz'], obj.position.symbol.okx.ct_val
        )
        usdt = base_coin * obj.data['px']
        return round(usdt, 2)

    @admin.display(description='Px')
    def _px(self, obj) -> str:
        return obj.data.get('px', '')

    @admin.display(description='PnL')
    def _pnl(self, obj) -> str:
        return obj.data.get('pnl', '')

    @admin.display(description='Symbol')
    def _symbol(self, obj) -> str:
        inst_id = obj.data.get('instId', '')
        if inst_id:
            symbol = ''.join(inst_id.split('-')[:-1])
        else:
            symbol = ''
        return symbol


@admin.register(Bill)
class BillAdmin(admin.ModelAdmin):
    list_display = (
        'account', 'bill_id', '_symbol', '_order_id', '_trade_id', '_sub_type',
        '_contract', '_inst_id', '_datetime', 'updated_at'
    )
    search_fields = ('bill_id',)
    list_filter = ('account', BillInstrumentFilter, BillSubTypeFilter)
    fields = ('bill_id', 'account', '_data', 'created_at', 'updated_at')
    list_display_links = ('bill_id', 'account')
    ordering = ('-bill_id',)

    # def has_delete_permission(self, request, obj=None):
    #     return False

    def has_add_permission(self, request):
        return False

    def has_change_permission(self, request, obj=None):
        return False

    @admin.display(description='Data')
    def _data(self, obj) -> str:
        data: dict = sort_data(obj.data, Execution.get_empty_data())
        return get_pretty_text(data)

    @admin.display(description='Sub type')
    def _sub_type(self, obj) -> str:
        return obj.data.get('subType', '')

    @admin.display(description='Contract size')
    def _contract(self, obj) -> str:
        return obj.data.get('sz', '')

    @admin.display(description='Instrument ID')
    def _inst_id(self, obj) -> str:
        return obj.data.get('instId', '')

    @admin.display(description='Datetime')
    def _datetime(self, obj) -> str:
        return obj.data.get('ts', '')

    @admin.display(description='Order ID')
    def _order_id(self, obj) -> str:
        return obj.data.get('ordId', '')

    @admin.display(description='Trade ID')
    def _trade_id(self, obj) -> str:
        return obj.data.get('tradeId', '')

    @admin.display(description='Symbol')
    def _symbol(self, obj) -> str:
        inst_id = obj.data.get('instId', '')
        if inst_id:
            symbol = ''.join(inst_id.split('-')[:-1])
        else:
            symbol = ''
        return symbol
