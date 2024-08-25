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
        'binance_okx.tasks.update_bills': {'queue': 'default'},
        'binance_okx.tasks.run_strategy': {'queue': 'default'},
        'binance_okx.tasks.open_or_increase_position': {'queue': 'positions'},
        'binance_okx.tasks.create_or_update_position': {'queue': 'positions'},
        'binance_okx.tasks.create_execution': {'queue': 'positions'},
        'binance_okx.tasks.check_position_close_time': {'queue': 'positions'},
        'binance_okx.handlers.orders_handler': {'queue': 'positions'},
        'binance_okx.tasks.check_condition': {'queue': 'check_condition'},
        'binance_okx.handlers.closing_trade_position_by_market': {'queue': 'market'},
        'binance_okx.handlers.closing_emulate_position_by_market': {'queue': 'market'},
        'binance_okx.tasks.run_websocket_okx_orders': {'queue': 'websocket_okx_orders'},
        'binance_okx.tasks.run_websocket_okx_positions': {'queue': 'websocket_okx_positions'},
        'binance_okx.tasks.run_websocket_okx_ask_bid': {'queue': 'websocket_okx_ask_bid'},
        'binance_okx.tasks.run_websocket_binance_ask_bid': {'queue': 'websocket_binance_ask_bid'},
        'binance_okx.tasks.run_websocket_okx_market_price': {'queue': 'websocket_okx_market_price'},
    },
    beat_schedule={
        'run_websocket_okx_positions': {
            'task': 'binance_okx.tasks.run_websocket_okx_positions',
            'schedule': crontab(minute='*/1'),
        },
        'run_websocket_okx_ask_bid': {
            'task': 'binance_okx.tasks.run_websocket_okx_ask_bid',
            'schedule': crontab(minute='*/1'),
        },
        'run_websocket_binance_ask_bid': {
            'task': 'binance_okx.tasks.run_websocket_binance_ask_bid',
            'schedule': crontab(minute='*/1'),
        },
        'run_websocket_okx_orders': {
            'task': 'binance_okx.tasks.run_websocket_okx_orders',
            'schedule': crontab(minute='*/1'),
        },
        'run_websocket_okx_market_price': {
            'task': 'binance_okx.tasks.run_websocket_okx_market_price',
            'schedule': crontab(minute='*/1'),
        },
        'update_symbols': {
            'task': 'binance_okx.tasks.update_symbols',
            'schedule': crontab(minute=0, hour=0),
        },
        'update_bills': {
            'task': 'binance_okx.tasks.update_bills',
            'schedule': 1,
        },
        'clean_db_log': {
            'task': 'binance_okx.tasks.clean_db_log',
            'schedule': crontab(minute=0, hour='*'),
            'args': (3,)
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
        record.__dict__.setdefault('position', '')
        if record.levelno == logging.INFO and re.search(r'success', record.msg.lower()):
            formatter = CeleryTaskFormatter(self.success_fmt)
            return formatter.format(record)
        return super().format(record)


@after_setup_logger.connect
@after_setup_task_logger.connect
def setup_task_logger(logger, *args, **kwargs):
    for handler in logger.handlers:
        tf = TaskFormatter(
            '[%(asctime)s.%(msecs)03d] %(short_task_id)s %(levelname)-7s %(name)-17s '
            '[%(created_by)s] [%(strategy)s] [%(symbol)s] [%(position)s] %(message)s',
        )
        tf.datefmt = '%d-%m-%Y %H:%M:%S'
        handler.setFormatter(tf)
