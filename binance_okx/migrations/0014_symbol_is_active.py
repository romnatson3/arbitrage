# Generated by Django 5.0.4 on 2024-08-29 09:22

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('binance_okx', '0013_binancesymbol_is_active_okxsymbol_is_active_and_more'),
    ]

    operations = [
        migrations.AddField(
            model_name='symbol',
            name='is_active',
            field=models.BooleanField(default=True, help_text='Is active'),
        ),
    ]
