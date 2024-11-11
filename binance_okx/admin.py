import logging
import csv
import pathlib
from types import SimpleNamespace as Namespace
from django.contrib import admin
from django.contrib.admin.sites import AdminSite
from django.utils import timezone
from django.utils.html import format_html
from django.contrib.auth.admin import UserAdmin as BaseUserAdmin
from django.contrib.auth import get_user_model
from django.db.models import QuerySet
from django.http import HttpResponse
from django.urls import path, re_path, reverse
from django.shortcuts import render, redirect
from django.core.cache import cache
from django.conf import settings
from io import StringIO
from .forms import CustomUserCreationForm, CustomUserChangeForm
from .models import (
    StatusLog, Account, OkxSymbol, BinanceSymbol, Strategy, Symbol, Position,
    Bill, Order
)
from .misc import get_pretty_dict, get_pretty_text, sort_data
from .forms import StrategyForm
from .filters import (
    PositionSideFilter, PositionStrategyFilter, PositionSymbolFilter,
    BillInstrumentFilter, BillSubTypeFilter, OrderInstrumentFilter,
    OrderTypeFilter, OrderStateFilter
)


User = get_user_model()
logger = logging.getLogger(__name__)


class CustomAdminSite(AdminSite):
    def get_app_list(self, request):
        app_list = super().get_app_list(request)
        filtered_app_list = []
        for app in app_list:
            if app['app_label'] == 'binance_okx':
                app['models'] = [
                    model for model in app['models']
                    if model['object_name'] not in ['Order', 'Bill']
                ]
                if app['models']:
                    filtered_app_list.append(app)
            else:
                filtered_app_list.append(app)
        return filtered_app_list


admin.site = CustomAdminSite()
admin.sites.site = admin.site
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
        'level', 'colored_msg', 'traceback', 'create_datetime_format',
        'strategy', 'symbol', 'position'
    )
    actions = ['delete_all']

    @admin.display(description='Message')
    def colored_msg(self, obj):
        if obj.level in [logging.NOTSET, logging.INFO]:
            color = 'darkgreen'
        elif obj.level == logging.DEBUG:
            color = 'gray'
        elif obj.level == logging.WARNING:
            color = 'orange'
        elif obj.level in [logging.ERROR, logging.CRITICAL]:
            color = 'red'
        return format_html(
            '<pre style="color:{color};white-space: pre-wrap;'
            'font-size: 1em;font-family: monospace"'
            '>{msg}</pre>',
            color=color,
            msg=obj.msg
        )

    @admin.action(description='Delete all')
    def delete_all(self, request, queryset):
        StatusLog.objects.all().delete()
        self.message_user(request, 'All logs deleted')

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
        js = ('binance_okx/js/strategy.js',)

    list_display = (
        'id', 'name', 'enabled', 'mode', '_account', 'position_size',
        'close_position_type', 'close_position_parts', '_symbols', 'updated_at'
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
    change_list_template = 'admin/strategy_change_list.html'

    @admin.display(description='Symbols')
    def _symbols(self, obj) -> str:
        return ', '.join(obj.symbols.values_list('symbol', flat=True))

    @admin.display(description='Account')
    def _account(self, obj) -> str:
        return obj.second_account.name

    @admin.action(description='Toggle enabled/disabled')
    def toggle_enabled(self, request, queryset):
        for strategy in queryset:
            strategy.enabled = not strategy.enabled
            strategy.save()

    def get_urls(self):
        urls = super().get_urls()
        csv_url = [
            path('csv_list/', self.csv_list, name='csv_list'),
            re_path(r'csv_list/(?P<filename>.+\.csv)', self.csv_list, name='csv_list')
        ]
        return csv_url + urls

    def csv_list(self, request, *args, **kwargs) -> HttpResponse:
        context = {}
        path = pathlib.Path(settings.CSV_PATH)
        csv_files = list(path.glob('*.csv'))
        if 'start_recording' in request.GET:
            cache.set('write_ask_bid_to_csv', True)
            logger.info('Recording started')
            return redirect(reverse('admin:csv_list'))
        elif 'stop_recording' in request.GET:
            cache.set('write_ask_bid_to_csv', False)
            logger.info('Recording stopped')
            return redirect(reverse('admin:csv_list'))
        elif 'delete_all' in request.GET:
            for file in csv_files:
                if file.is_file():
                    file.unlink()
                    logger.info(f'File {file} deleted')
            return redirect(reverse('admin:csv_list'))
        elif 'filename' in kwargs:
            filename = kwargs['filename']
            if filename:
                file_path = path.joinpath(filename)
                if file_path.is_file():
                    with open(file_path, 'r') as f:
                        response = HttpResponse(
                            f.read(), content_type='text/csv', charset='utf-8-sig')
                        response['Content-Disposition'] = f'attachment; filename="{filename}"'
                        return response
        context['files'] = [i.name for i in csv_files]
        context['recording'] = cache.get('write_ask_bid_to_csv', False)
        return render(request, 'admin/csv_list.html', context)


@admin.register(Position)
class PositionAdmin(admin.ModelAdmin):
    list_display = (
        'id', 'is_open', 'strategy', '_position_side', 'mode', 'symbol',
        '_trade_ids', '_pos', '_amount', '_duration', '_datetime'
    )
    fieldsets = (
        (None, {'fields': ('id', 'is_open', 'strategy', 'symbol', 'mode', 'account')}),
        (None, {'fields': ('trade_ids',)}),
        (None, {'fields': ('updated_at', 'created_at')}),
        (
            'Data',
            {
                'fields': (
                    ('_position_data', '_sl_tp_data', '_ask_bid_data'),
                )
            }
        ),
        ('Bills', {'fields': ('_bills',)})
    )
    search_fields = ('strategy__name', 'symbol__symbol', 'trade_ids')
    list_display_links = ('id', 'strategy')
    readonly_fields = (
        'id', 'updated_at', 'created_at', '_position_data', '_sl_tp_data',
        '_ask_bid_data', '_pos', '_amount', 'strategy', 'symbol', 'account',
        'mode', '_bills'
    )
    list_filter = (
        'is_open', 'mode', PositionSideFilter, PositionStrategyFilter,
        PositionSymbolFilter
    )
    actions = ['export_csv_action', 'toggle_open_close']
    list_per_page = 500
    ordering = ('-id',)

    class Media:
        css = {
            'all': ('binance_okx/css/inline.css',)
        }

    def get_queryset(self, request) -> QuerySet:
        qs = super().get_queryset(request)
        return qs.order_by('id')

    def get_form(self, request, obj=None, **kwargs):
        form = super().get_form(request, obj, **kwargs)
        form.base_fields['trade_ids'].widget.attrs.update({
            'rows': 2,
            'cols': 100,
            # 'style': 'width: 500px; height: 100px;'
        })
        return form

    # def has_delete_permission(self, request, obj=None):
    #     return False

    def has_add_permission(self, request):
        return False

    # def has_change_permission(self, request, obj=None):
    #     return False
    # http://172.22.249.237:8000/Qgr476gLAiCa/binance_okx/bill/1942454611814744065/change/

    @admin.display(description='Bills')
    def _bills(self, obj) -> str:
        from django.utils.safestring import mark_safe
        rows = [
            '<table class="bills">',
            '<tr>',
            '<th>Sub type</th>',
            '<th>Datetime</th>',
            '<th>Bill ID</th>',
            '<th>Trade ID</th>',
            '<th>Order ID</th>',
            '<th>Symbol</th>',
            '<th>Mode</th>',
            '<th>Price</th>',
            '<th>Size</th>',
            '<th>Amount</th>',
            '<th>PnL</th>',
            '</tr>'
        ]
        for bill in obj.bills:
            rows.append(
                '<tr>'
                f'<td>{bill.data["subType"]}</td>'
                f'<td>{bill.data["ts"]}</td>'
                f'<td><a href={reverse("admin:binance_okx_bill_change", args=[bill.bill_id])}>'
                f'{bill.bill_id}</a></td>'
                f'<td>{bill.trade_id}</td>'
                f'<td>{bill.order_id}</td>'
                f'<td>{bill.symbol}</td>'
                f'<td>{bill.mode}</td>'
                f'<td>{bill.data["px"]}</td>'
                f'<td>{bill.data["sz"]}</td>'
                f'<td>{bill.amount_usdt}</td>'
                f'<td>{bill.data["pnl"]}</td>'
                '</tr>'
            )
        rows.append('</table>')
        return mark_safe(''.join(rows))

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

    @admin.display(description='Trade IDs')
    def _trade_ids(self, obj) -> str:
        if not obj.trade_ids:
            return ''
        return ', '.join(map(lambda x: str(x), obj.trade_ids))

    @admin.display(description='Position size')
    def _pos(self, obj) -> str:
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

    @admin.display(description='uTime', ordering='position_data__uTime')
    def _datetime(self, obj) -> str:
        return obj.position_data.get('uTime', '')

    @admin.display(description='Duration, s')
    def _duration(self, obj) -> int:
        duration = 0
        open_time = timezone.datetime.strptime(
            obj.position_data['cTime'], '%d-%m-%Y %H:%M:%S.%f')
        if obj.is_open:
            open_time = open_time.astimezone(timezone.get_current_timezone())
            duration = (timezone.localtime() - open_time).total_seconds()
        else:
            if obj.position_data['uTime']:
                close_time = timezone.datetime.strptime(
                    obj.position_data['uTime'], '%d-%m-%Y %H:%M:%S.%f')
                duration = (close_time - open_time).total_seconds()
        return f'{duration:.3f}'

    @admin.action(description='Toggle open/close')
    def toggle_open_close(self, request, queryset):
        for position in queryset:
            position.is_open = not position.is_open
            position.save()
        self.message_user(request, f'Positions toggled: {queryset.count()}')

    @admin.action(description='Export CSV')
    def export_csv_action(self, request, queryset):
        f = StringIO()
        writer = csv.writer(f, delimiter=';')
        headers = [
            'ІД Позиції', 'Монета', 'Дата', 'Чаc', 'Тип (trade/emulate)',
            'Позиція (шорт/лонг)', 'Тип (open/close)',
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
        for position in queryset.order_by('id'):
            for bill in position.bills:
                bill_data = Namespace(**bill.data)
                ask_bid_data = Namespace(**bill.position['ask_bid'])
                position_data = Namespace(**bill.position['data'])
                duration = round((
                    timezone.datetime.strptime(bill_data.ts, '%d-%m-%Y %H:%M:%S.%f') -
                    timezone.datetime.strptime(position_data.cTime, '%d-%m-%Y %H:%M:%S.%f')
                ).total_seconds() * 1000)
                is_open = 'open' in bill_data.subType.lower()
                usdt = bill.amount_usdt
                if is_open:
                    row = [
                        bill.position['id'],
                        bill.symbol.symbol,
                        ask_bid_data.date_time_last_prices.split(' ')[0],
                        ask_bid_data.date_time_last_prices.split(' ')[1],
                        bill.mode,
                        position_data.posSide,
                        bill_data.subType,
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
                        bill_data.ts.split(' ')[0],
                        bill_data.ts.split(' ')[1],
                        # position_data.cTime.split(' ')[0],
                        # position_data.cTime.split(' ')[1],
                        bill_data.px,
                        None if is_open else bill_data.ts.split(' ')[1],
                        None if is_open else duration,
                        bill_data.fee,
                        None if is_open else f'{bill_data.pnl:.5f}'
                    ]
                else:
                    row = [
                        bill.position['id'],
                        bill.symbol.symbol,
                        '',
                        '',
                        bill.mode,
                        position_data.posSide,
                        bill_data.subType,
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
                        bill_data.px,
                        '' if is_open else bill_data.ts.split(' ')[1],
                        '' if is_open else duration,
                        bill_data.fee,
                        '' if is_open else f'{bill_data.pnl:.5f}'
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


@admin.register(Order)
class OrderAdmin(admin.ModelAdmin):
    list_display = (
        'account', 'order_id', 'trade_id', 'symbol', '_side', '_pos_side',
        '_order_type', '_state', '_notional', '_sz', '_fill_sz', '_datetime'
    )
    search_fields = ('order_id', 'trade_id')
    list_filter = ('account', OrderInstrumentFilter, OrderTypeFilter, OrderStateFilter)
    fields = ('account', 'order_id', 'trade_id', '_data', 'created_at', 'updated_at')
    list_display_links = ('order_id', 'account')
    ordering = ('-order_id',)
    list_per_page = 500
    actions = ['delete_all']

    # def has_delete_permission(self, request, obj=None):
    #     return False

    def has_add_permission(self, request):
        return False

    def has_change_permission(self, request, obj=None):
        return False

    @admin.action(description='Delete all')
    def delete_all(self, request, queryset):
        Order.objects.all().delete()
        self.message_user(request, 'All orders deleted')

    @admin.display(description='Data')
    def _data(self, obj) -> str:
        return get_pretty_dict(obj.data)

    @admin.display(description='Side')
    def _side(self, obj) -> str:
        return obj.data.get('side', '')

    @admin.display(description='Position side')
    def _pos_side(self, obj) -> str:
        return obj.data.get('posSide', '')

    @admin.display(description='Order type')
    def _order_type(self, obj) -> str:
        return obj.data.get('ordType', '')

    @admin.display(description='State')
    def _state(self, obj) -> str:
        return obj.data.get('state', '')

    @admin.display(description='Notional')
    def _notional(self, obj) -> str:
        return obj.data.get('notionalUsd', '')

    @admin.display(description='Sz')
    def _sz(self, obj) -> str:
        return obj.data.get('sz', '')

    @admin.display(description='Fill sz')
    def _fill_sz(self, obj) -> str:
        return obj.data.get('fillSz', '')

    @admin.display(description='Datetime', ordering='data__cTime')
    def _datetime(self, obj) -> str:
        return obj.data.get('cTime', '')


@admin.register(Bill)
class BillAdmin(admin.ModelAdmin):
    list_display = (
        'account', 'bill_id', 'order_id', 'trade_id', 'symbol', 'mode', '_sub_type',
        '_size', '_pnl', '_px', '_amount', '_datetime'
    )
    search_fields = ('bill_id', 'trade_id', 'order_id')
    list_filter = ('account', 'mode', BillInstrumentFilter, BillSubTypeFilter)
    fields = (
        'bill_id', 'account', 'order_id', 'trade_id', 'symbol', 'mode', '_data',
        'created_at', 'updated_at'
    )
    readonly_fields = (
        'bill_id', 'account', 'symbol', '_data', 'created_at', 'updated_at',
        'mode'
    )
    list_display_links = ('bill_id', 'account')
    ordering = ('-data__ts', '-bill_id')
    list_per_page = 500
    actions = ['delete_all']

    # def has_delete_permission(self, request, obj=None):
    #     return False

    def has_add_permission(self, request):
        return False

    # def has_change_permission(self, request, obj=None):
    #     return False

    @admin.action(description='Delete all')
    def delete_all(self, request, queryset):
        Bill.objects.all().delete()
        self.message_user(request, 'All bills deleted')

    @admin.display(description='Data')
    def _data(self, obj) -> str:
        data: dict = sort_data(obj.data, Bill.get_empty_data())
        return get_pretty_text(data)

    @admin.display(description='Sub type')
    def _sub_type(self, obj) -> str:
        return obj.data.get('subType', '')

    @admin.display(description='Size')
    def _size(self, obj) -> str:
        return obj.data.get('sz', '')

    @admin.display(description='Datetime', ordering='data__ts')
    def _datetime(self, obj) -> str:
        return obj.data.get('ts', '')

    @admin.display(description='Px')
    def _px(self, obj) -> str:
        return obj.data.get('px', '')

    @admin.display(description='PnL')
    def _pnl(self, obj) -> str:
        return obj.data.get('pnl', '')

    @admin.display(description='USDT amount')
    def _amount(self, obj) -> str:
        return obj.amount_usdt
