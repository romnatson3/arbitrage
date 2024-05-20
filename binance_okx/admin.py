import logging
from django.contrib import admin
from django.utils import timezone
from django.utils.html import format_html
from django.contrib.auth.admin import UserAdmin as BaseUserAdmin
from django.contrib.auth import get_user_model
from django.db.models import QuerySet
from .forms import CustomUserCreationForm, CustomUserChangeForm
from .models import (
    StatusLog, Account, Candle, OkxSymbol, BinanceSymbol, OkxCandle, BinanceCandle,
    Strategy, Symbol
)
from .misc import get_pretty_dict
from .forms import StrategyForm


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
        'colored_msg', 'strategy', 'symbol', 'create_datetime_format'
    )
    list_display_links = ('colored_msg',)
    list_filter = ('level',)
    list_per_page = 50
    search_fields = ('msg', 'trace')
    fields = (
        'level', 'colored_msg', 'traceback', 'create_datetime_format', 'created_by',
        'strategy', 'symbol'
    )

    @admin.display(description='Message')
    def colored_msg(self, obj):
        if obj.level in [logging.NOTSET, logging.INFO]:
            color = 'green'
        elif obj.level in [logging.WARNING, logging.DEBUG]:
            color = 'orange'
        else:
            color = 'red'
        return format_html(
            '<pre style="color:{color}; white-space: pre-wrap; font-family: monospace; font-size: 1.05em">{msg}</pre>',
            color=color,
            msg=obj.msg
        )

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
        return timezone.localtime(obj.created_at).strftime('%d-%m-%Y %T')

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
    list_display = ('symbol', 'updated_at')
    search_fields = ('symbol',)
    fields = ('symbol', 'okx', 'binance', 'updated_at', 'created_at')
    ordering = ('symbol',)
    readonly_fields = ('symbol', 'okx', 'binance', 'updated_at', 'created_at')


@admin.register(BinanceSymbol)
class BinanceSymbolAdmin(admin.ModelAdmin):
    list_display = ('symbol', 'updated_at')
    fields = ('symbol', 'pretty_data', 'updated_at', 'created_at')
    search_fields = ('symbol',)
    ordering = ('symbol',)

    @admin.display(description='Data')
    def pretty_data(self, obj) -> str:
        return get_pretty_dict(obj.data)

    def has_delete_permission(self, request, obj=None):
        return False

    def has_add_permission(self, request):
        return False

    def has_change_permission(self, request, obj=None):
        return False


@admin.register(OkxSymbol)
class OkxSymbolAdmin(admin.ModelAdmin):
    list_display = ('symbol', 'updated_at')
    fields = ('symbol', 'pretty_data', 'updated_at', 'created_at')
    search_fields = ('symbol',)
    ordering = ('symbol',)

    @admin.display(description='Data')
    def pretty_data(self, obj) -> str:
        return get_pretty_dict(obj.data)

    def has_delete_permission(self, request, obj=None):
        return False

    def has_add_permission(self, request):
        return False

    def has_change_permission(self, request, obj=None):
        return False

    def get_search_results(self, request, queryset, search_term):
        queryset, use_distinct = super().get_search_results(request, queryset, search_term)
        if request.headers.get('x-requested-with') == 'XMLHttpRequest':
            return OkxSymbol.objects.filter(
                symbol__in=BinanceSymbol.objects.values_list('symbol', flat=True),
                symbol__icontains=search_term
            ), use_distinct
        return queryset, use_distinct


class CandleAdmin(admin.ModelAdmin):
    list_display = ('symbol', 'time_frame', 'updated_at')
    search_fields = ('symbol', 'time_frame')
    fields = ('symbol', 'time_frame', 'pretty_data', 'updated_at')
    ordering = ('-created_at',)
    readonly_fields = ('symbol', 'time_frame', 'pretty_data', 'updated_at')

    def has_delete_permission(self, request, obj=None):
        return False

    def has_add_permission(self, request):
        return False

    def has_change_permission(self, request, obj=None):
        return False

    @admin.display(description='Candles data')
    def pretty_data(self, obj) -> str:
        return get_pretty_dict(obj.data)


# @admin.register(BinanceCandle)
# class BinanceCandleAdmin(CandleAdmin):
#     pass


# @admin.register(OkxCandle)
# class OkxCandleAdmin(CandleAdmin):
#     pass


@admin.register(Strategy)
class StrategyAdmin(admin.ModelAdmin):
    list_display = (
        'id', 'name', 'enabled', 'mode', 'position_size', 'target_profit', 'updated_at'
    )
    search_fields = ('name',)
    list_filter = ()
    fieldsets = (
        (None, {'fields': ('id', 'name', 'enabled', 'mode')}),
        (None, {'fields': (('first_account', 'second_account'),)}),
        (None, {'fields': ('symbols',)}),
        (
            None,
            {
                'fields': (
                    'position_size', 'taker_fee', 'maker_fee', 'target_profit', 'stop_loss',
                    'close_position_type', 'time_to_close',
                    ('close_position_parts', 'first_part_size', 'second_part_size'),
                    'time_to_funding', 'only_profit', 'logging'
                )
            }
        ),
        (None, {'fields': ('updated_at', 'created_at')}),
    )
    list_display_links = ('id', 'name')
    readonly_fields = ('id', 'updated_at', 'created_at')
    autocomplete_fields = ('first_account', 'second_account', 'symbols')
    save_on_top = True
    form = StrategyForm

    def get_queryset(self, request) -> QuerySet:
        qs = super().get_queryset(request)
        if request.user.is_superuser:
            return qs
        return qs.filter(created_by=request.user)

    def save_model(self, request, obj: Strategy, form: StrategyForm, change):
        if not obj.pk:
            obj.created_by = request.user
        super().save_model(request, obj, form, change)
