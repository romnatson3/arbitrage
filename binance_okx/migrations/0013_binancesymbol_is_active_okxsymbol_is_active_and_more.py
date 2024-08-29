# Generated by Django 5.0.4 on 2024-08-29 07:41

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('binance_okx', '0012_rename_target_profit_strategy_take_profit'),
    ]

    operations = [
        migrations.AddField(
            model_name='binancesymbol',
            name='is_active',
            field=models.BooleanField(default=True, help_text='Is active'),
        ),
        migrations.AddField(
            model_name='okxsymbol',
            name='is_active',
            field=models.BooleanField(default=True, help_text='Is active'),
        ),
        migrations.AlterField(
            model_name='strategy',
            name='take_profit',
            field=models.FloatField(default=0.0, help_text='Take profit, %', verbose_name='Take profit'),
        ),
    ]
