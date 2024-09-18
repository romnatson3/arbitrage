from __future__ import absolute_import, unicode_literals
import logging
import os
import re
from celery import Celery
from celery.app.log import TaskFormatter as CeleryTaskFormatter
from celery.signals import after_setup_task_logger, after_setup_logger
from celery._state import get_current_task
from celery.schedules import crontab
from django.conf import settings


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
        'binance_okx.handlers.closing_trade_position_market_parts': {'queue': 'market'},
        'binance_okx.handlers.closing_emulate_position_market_stop_loss': {'queue': 'market'},
        'binance_okx.handlers.closing_emulate_position_market_take_profit': {'queue': 'market'},
        'binance_okx.handlers.closing_emulate_position_market_parts': {'queue': 'market'},
        'binance_okx.handlers.closing_position_by_limit': {'queue': 'market'},
        'binance_okx.handlers.closing_emulate_position_by_limit': {'queue': 'market'},
        'binance_okx.tasks.run_websocket_okx_orders': {'queue': 'websocket_okx_orders'},
        'binance_okx.tasks.run_websocket_okx_positions': {'queue': 'websocket_okx_positions'},
        'binance_okx.tasks.run_websocket_okx_ask_bid': {'queue': 'websocket_okx_ask_bid'},
        'binance_okx.tasks.run_websocket_binance_ask_bid': {'queue': 'websocket_binance_ask_bid'},
        'binance_okx.tasks.run_websocket_okx_market_price': {'queue': 'websocket_okx_market_price'},
        'binance_okx.tasks.run_websocket_okx_last_price': {'queue': 'websocket_okx_last_price'},
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
        'run_websocket_okx_last_price': {
            'task': 'binance_okx.tasks.run_websocket_okx_last_price',
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
        trace_gray = '\033[90m'
        debug_cyan = '\033[36m'
        info_green = '\033[32m'
        warning_yellow = '\033[33m'
        critical_purple = '\033[35m'
        error_red = '\033[31m'
        reset = '\033[0m'
        self.trace_fmt = trace_gray + self._fmt + reset
        self.debug_fmt = debug_cyan + self._fmt + reset
        self.info_fmt = info_green + self._fmt + reset
        self.warning_fmt = warning_yellow + self._fmt + reset
        self.critical_fmt = critical_purple + self._fmt + reset
        self.error_fmt = error_red + self._fmt + reset

    def format(self, record):
        formatter = CeleryTaskFormatter(self._fmt)
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
        if record.levelno == settings.TRACE_LEVEL_NUM:
            record.levelname = 'TRACE'
            formatter = CeleryTaskFormatter(self.trace_fmt)
        if record.levelno == logging.DEBUG:
            formatter = CeleryTaskFormatter(self.debug_fmt)
        if record.levelno == logging.INFO:
            formatter = CeleryTaskFormatter(self.info_fmt)
        if record.levelno == logging.WARNING:
            formatter = CeleryTaskFormatter(self.warning_fmt)
        if record.levelno == logging.CRITICAL:
            formatter = CeleryTaskFormatter(self.critical_fmt)
        if record.levelno == logging.ERROR:
            formatter = CeleryTaskFormatter(self.error_fmt)
        formatter.datefmt = '%d-%m-%Y %H:%M:%S'
        return formatter.format(record)


@after_setup_logger.connect
@after_setup_task_logger.connect
def setup_task_logger(logger, *args, **kwargs):
    for handler in logger.handlers:
        tf = TaskFormatter(
            '[%(asctime)s.%(msecs)03d] %(short_task_id)s %(levelname)-7s '
            '[%(created_by)s] [%(strategy)s] [%(symbol)s] [%(position)s] %(message)s',
        )
        # tf.datefmt = '%d-%m-%Y %H:%M:%S'
        handler.setFormatter(tf)
