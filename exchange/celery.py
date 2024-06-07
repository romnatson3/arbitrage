from __future__ import absolute_import, unicode_literals
import logging
import os
import re
from celery import Celery
from celery.app.log import TaskFormatter as CeleryTaskFormatter
from celery.signals import after_setup_task_logger, after_setup_logger
from celery._state import get_current_task
from celery.schedules import crontab


os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'exchange.settings')

app = Celery('exchange')
app.config_from_object('django.conf:settings', namespace='CELERY')
app.autodiscover_tasks()


app.conf.update(
    task_default_queue='default',
    task_routes={
        'binance_okx.tasks.clean_db_log': {'queue': 'default'},
        'binance_okx.tasks.update_symbols': {'queue': 'default'},
        'binance_okx.tasks.update_ask_bid_price': {'queue': 'default'},
        'binance_okx.tasks.update_binance_ask_bid_price': {'queue': 'default'},
        'binance_okx.tasks.update_okx_ask_bid_price': {'queue': 'default'},
        'binance_okx.tasks.update_okx_market_price': {'queue': 'default'},
        'binance_okx.tasks.check_if_position_is_closed': {'queue': 'position'},
        'binance_okx.tasks.run_websocket_orders': {'queue': 'websocket'},
        'binance_okx.tasks.run_strategy': {'queue': 'strategy'},
        'binance_okx.tasks.trade_strategy_for_symbol': {'queue': 'trade'},
        'binance_okx.tasks.emulate_strategy_for_symbol': {'queue': 'emulate'}
    },
    beat_schedule={
        'run_websocket_orders': {
            'task': 'binance_okx.tasks.run_websocket_orders',
            'schedule': crontab(minute='*/5'),
        },
        'check_if_position_is_closed': {
            'task': 'binance_okx.tasks.check_if_position_is_closed',
            'schedule': 1,
        },
        'okx_market_price': {
            'task': 'binance_okx.tasks.update_okx_market_price',
            'schedule': 1,
        },
        'update_ask_bid_price': {
            'task': 'binance_okx.tasks.update_ask_bid_price',
            'schedule': 5,
        },
        'update_symbols': {
            'task': 'binance_okx.tasks.update_symbols',
            'schedule': crontab(minute=0, hour=0),
        },
        'clean_db_log': {
            'task': 'binance_okx.tasks.clean_db_log',
            'schedule': crontab(minute=0, hour=0),
            'args': (5,)
        },
    }
)


class TaskFormatter(CeleryTaskFormatter):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        green = '\033[32m'
        reset = '\033[0m'
        self.success_fmt = green + self._fmt + reset

    def format(self, record):
        task = get_current_task()
        if task and task.request:
            short_task_id = task.request.id.split('-')[0]
            record.__dict__.update(short_task_id=short_task_id)
        else:
            record.__dict__.setdefault('short_task_id', '--------')
        record.__dict__.setdefault('created_by', '')
        record.__dict__.setdefault('strategy', '')
        record.__dict__.setdefault('symbol', '')
        if record.levelno == logging.INFO and re.search(r'success', record.msg.lower()):
            formatter = CeleryTaskFormatter(self.success_fmt)
            return formatter.format(record)
        return super().format(record)


@after_setup_logger.connect
@after_setup_task_logger.connect
def setup_task_logger(logger, *args, **kwargs):
    for handler in logger.handlers:
        handler.setFormatter(TaskFormatter(
            '[%(asctime)s] %(short_task_id)s %(levelname)-7s %(name)-17s [%(created_by)s] [%(strategy)s] [%(symbol)s] %(message)s'
        ))
