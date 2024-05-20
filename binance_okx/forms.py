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
        first_account = self.fields['first_account']
        first_account.widget.can_add_related = False
        first_account.widget.can_change_related = False
        first_account.widget.can_delete_related = False
        second_account = self.fields['second_account']
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
        first_account: Account = cleaned_data.get('first_account')
        second_account: Account = cleaned_data.get('second_account')
        position_size: float = cleaned_data.get('position_size')
        taker_fee: float = cleaned_data.get('taker_fee')
        maker_fee: float = cleaned_data.get('maker_fee')
        target_profit: float = cleaned_data.get('target_profit')
        close_position_type: str = cleaned_data.get('close_position_type')
        if not first_account or not second_account:
            raise forms.ValidationError('First account and second account are required')
        if first_account.exchange == second_account.exchange:
            raise forms.ValidationError('First account and second account must be different exchanges')
        if position_size <= 0:
            self.add_error('position_size', 'Position size must be greater than 0')
        if taker_fee <= 0 and close_position_type == 'market':
            self.add_error('taker_fee', 'Taker fee must be greater than 0')
        if maker_fee <= 0 and close_position_type == 'limit':
            self.add_error('maker_fee', 'Maker fee must be greater than 0')
        if target_profit <= 0:
            self.add_error('target_profit', 'Target profit must be greater than 0')
        return cleaned_data
