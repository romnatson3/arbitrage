from django.contrib.admin import SimpleListFilter
from .models import Strategy, Position, Bill


class BillInstrumentFilter(SimpleListFilter):
    title = 'Instrument'
    parameter_name = 'instrument'

    def lookups(self, request, model_admin):
        return [
            (instrument, instrument)
            for instrument in Bill.objects.values_list('data__instId', flat=True).order_by('data__instId').distinct()
        ]

    def queryset(self, request, queryset):
        if self.value():
            return queryset.filter(data__instId=self.value())
        return queryset


class BillSubTypeFilter(SimpleListFilter):
    title = 'Sub Type'
    parameter_name = 'sub_type'

    def lookups(self, request, model_admin):
        return [
            (sub_type, sub_type)
            for sub_type in Bill.objects.values_list('data__subType', flat=True).order_by('data__subType').distinct()
        ]

    def queryset(self, request, queryset):
        if self.value():
            return queryset.filter(data__subType=self.value())
        return queryset


class PositionSideFilter(SimpleListFilter):
    title = 'Position Side'
    parameter_name = 'position_side'

    def lookups(self, request, model_admin):
        return (
            ('long', 'Long'),
            ('short', 'Short'),
        )

    def queryset(self, request, queryset):
        if self.value():
            return queryset.filter(position_data__posSide=self.value())
        return queryset


class PositionStrategyFilter(SimpleListFilter):
    title = 'Strategy'
    parameter_name = 'strategy'

    def lookups(self, request, model_admin):
        return [
            (i['id'], i['name'])
            for i in Strategy.objects.values('id', 'name').order_by('name').distinct()
        ]

    def queryset(self, request, queryset):
        if self.value():
            return queryset.filter(strategy__id=self.value())
        return queryset


class PositionSymbolFilter(SimpleListFilter):
    title = 'Symbol'
    parameter_name = 'symbol'

    def lookups(self, request, model_admin):
        return [
            (symbol, symbol)
            for symbol in Position.objects.values_list('symbol', flat=True).order_by('symbol').distinct()
        ]

    def queryset(self, request, queryset):
        if self.value():
            return queryset.filter(symbol=self.value())
        return queryset
