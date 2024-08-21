from django.contrib import admin
from django import forms
from django.contrib.auth.forms import UserCreationForm, UserChangeForm
from django.contrib.admin.widgets import AutocompleteSelectMultiple
from .models import User, Strategy, Symbol, Account


class CustomUserCreationForm(UserCreationForm):
    class Meta:
        model = User
        fields = '__all__'


class CustomUserChangeForm(UserChangeForm):
    class Meta:
        model = User
        fields = '__all__'


class StrategyForm(forms.ModelForm):
    class Meta:
        model = Strategy
        fields = '__all__'

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # first_account = self.fields['first_account']
        # first_account.widget.can_view_related = False
        # first_account.widget.can_add_related = False
        # first_account.widget.can_change_related = False
        # first_account.widget.can_delete_related = False
        second_account = self.fields['second_account']
        second_account.widget.can_view_related = False
        second_account.widget.can_add_related = False
        second_account.widget.can_change_related = False
        second_account.widget.can_delete_related = False

    symbols = forms.ModelMultipleChoiceField(
        queryset=Symbol.objects.all(),
        widget=AutocompleteSelectMultiple(
            Strategy.symbols.field,
            admin.site,
            attrs={'style': 'width: 500px'}
        )
    )

    def clean(self):
        cleaned_data: dict = super().clean()
        # first_account: Account = cleaned_data.get('first_account')
        second_account: Account = cleaned_data.get('second_account')
        position_size: float = cleaned_data.get('position_size')
        taker_fee: float = cleaned_data.get('taker_fee')
        maker_fee: float = cleaned_data.get('maker_fee')
        target_profit: float = cleaned_data.get('target_profit')
        close_position_type: str = cleaned_data.get('close_position_type')
        close_position_parts: bool = cleaned_data.get('close_position_parts')
        stop_loss_breakeven: bool = cleaned_data.get('stop_loss_breakeven')
        tp_first_price_percent: float = cleaned_data.get('tp_first_price_percent')
        tp_first_part_percent: float = cleaned_data.get('tp_first_part_percent')
        tp_second_price_percent: float = cleaned_data.get('tp_second_price_percent')
        tp_second_part_percent: float = cleaned_data.get('tp_second_part_percent')
        search_duration: int = cleaned_data.get('search_duration')
        if not second_account:
            raise forms.ValidationError('Account is required')
        # if not first_account or not second_account:
        #     raise forms.ValidationError('First account and second account are required')
        # if first_account.exchange == second_account.exchange:
        #     raise forms.ValidationError('First account and second account must be different exchanges')
        if position_size <= 0:
            self.add_error('position_size', 'Position size must be greater than 0')
        if taker_fee <= 0 and close_position_type == 'market':
            self.add_error('taker_fee', 'Taker fee must be greater than 0')
        if maker_fee <= 0 and close_position_type == 'limit':
            self.add_error('maker_fee', 'Maker fee must be greater than 0')
        if target_profit <= 0:
            self.add_error('target_profit', 'Target profit must be greater than 0')
        if close_position_parts:
            # if tp_first_price_percent <= 0 or tp_first_part_percent <= 0 or tp_second_price_percent <= 0 or tp_second_part_percent <= 0:
            if tp_first_price_percent <= 0 or tp_first_part_percent <= 0 or tp_second_price_percent <= 0:
                self.add_error('tp_first_price_percent', 'Take profit price percent must be greater than 0')
                self.add_error('tp_first_part_percent', 'Take profit part percent must be greater than 0')
                self.add_error('tp_second_price_percent', 'Take profit price percent must be greater than 0')
                # self.add_error('tp_second_part_percent', 'Take profit part percent must be greater than 0')
        if search_duration < 0:
            self.add_error('search_duration', 'Search duration must be greater than 0')
        if search_duration > 60000:
            self.add_error('search_duration', 'Search duration must be less than 60000 ms')
        return cleaned_data
