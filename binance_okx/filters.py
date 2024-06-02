from django.contrib.admin import SimpleListFilter
from .models import Strategy, Position


class PositionSideFilter(SimpleListFilter):
    title = 'Position Side'
    parameter_name = 'position_side'

    def lookups(self, request, model_admin):
        return (
            ('long', 'Long'),
            ('short', 'Short'),
        )

    def queryset(self, request, queryset):
        if self.value() == 'long':
            return queryset.filter(position_data__posSide='long')
        if self.value() == 'short':
            return queryset.filter(position_data__posSide='short')
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
