# Generated by Django 5.0.4 on 2024-05-28 17:46

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('binance_okx', '0002_remove_strategy_logging_alter_execution_bill_id_and_more'),
    ]

    operations = [
        migrations.AddIndex(
            model_name='execution',
            index=models.Index(fields=['bill_id'], name='binance_okx_bill_id_a59d50_idx'),
        ),
        migrations.AddIndex(
            model_name='execution',
            index=models.Index(fields=['trade_id'], name='binance_okx_trade_i_275d69_idx'),
        ),
    ]
