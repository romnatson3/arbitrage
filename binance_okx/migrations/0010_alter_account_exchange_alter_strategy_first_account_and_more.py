# Generated by Django 5.0.4 on 2024-07-03 14:50

import django.db.models.deletion
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('binance_okx', '0009_statuslog_position'),
    ]

    operations = [
        migrations.AlterField(
            model_name='account',
            name='exchange',
            field=models.CharField(choices=[('okx', 'OKX')], default='okx', help_text='Exchange', verbose_name='exchange'),
        ),
        migrations.AlterField(
            model_name='strategy',
            name='first_account',
            field=models.ForeignKey(help_text='Binance account', null=True, on_delete=django.db.models.deletion.CASCADE, related_name='strategies_set', to='binance_okx.account'),
        ),
        migrations.AlterField(
            model_name='strategy',
            name='search_duration',
            field=models.IntegerField(default=0, help_text='Search duration, milliseconds', verbose_name='Search duration'),
        ),
        migrations.AlterField(
            model_name='strategy',
            name='second_account',
            field=models.ForeignKey(help_text='OKX account', on_delete=django.db.models.deletion.CASCADE, related_name='strategies', to='binance_okx.account', verbose_name='OKX account'),
        ),
    ]
