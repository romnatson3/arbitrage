from django.apps import AppConfig


class BinanceOkxConfig(AppConfig):
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'binance_okx'
    verbose_name = 'Binance - OKX'

    def ready(self):
        import binance_okx.signals
