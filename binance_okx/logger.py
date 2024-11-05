import logging
from django.conf import settings


class CustomFormatter(logging.Formatter):
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
        formatter = logging.Formatter(self._fmt)
        record.__dict__.setdefault('strategy', '----------')
        record.__dict__.setdefault('symbol', '---------------')
        record.__dict__.setdefault('position', '-----')
        record.symbol = f'{str(record.symbol):<15.15}'
        if record.levelno == settings.TRACE_LEVEL_NUM:
            record.levelname = 'TRACE'
            formatter = logging.Formatter(self.trace_fmt)
        if record.levelno == logging.DEBUG:
            formatter = logging.Formatter(self.debug_fmt)
        if record.levelno == logging.INFO:
            formatter = logging.Formatter(self.info_fmt)
        if record.levelno == logging.WARNING:
            formatter = logging.Formatter(self.warning_fmt)
        if record.levelno == logging.CRITICAL:
            formatter = logging.Formatter(self.critical_fmt)
        if record.levelno == logging.ERROR:
            formatter = logging.Formatter(self.error_fmt)
        formatter.datefmt = '%d-%m-%Y %H:%M:%S'
        return formatter.format(record)


class DatabaseLogHandler(logging.Handler):
    def emit(self, record):
        from .models import StatusLog
        formatter = logging.Formatter()
        trace = None
        if record.exc_info:
            trace = formatter.formatException(record.exc_info)
        msg = record.getMessage()
        strategy = record.__dict__.get('strategy')
        symbol = record.__dict__.get('symbol')
        position = record.__dict__.get('position')
        kwargs = {
            'logger_name': record.name,
            'level': record.levelno,
            'msg': msg,
            'trace': trace,
            'strategy': strategy,
            'symbol': symbol,
            'position': position
        }
        if strategy:
            StatusLog.objects.create(**kwargs)
