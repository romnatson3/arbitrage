import logging


class CustomFormatter(logging.Formatter):
    def format(self, record):
        formatter = logging.Formatter(self._fmt)
        record.__dict__.setdefault('created_by', '')
        record.__dict__.setdefault('strategy', '')
        record.__dict__.setdefault('symbol', '')
        record.__dict__.setdefault('position', '')
        return formatter.format(record)


class DatabaseLogHandler(logging.Handler):
    def emit(self, record):
        from .models import StatusLog
        formatter = logging.Formatter()
        trace = None
        if record.exc_info:
            trace = formatter.formatException(record.exc_info)
        msg = record.getMessage()
        created_by = record.__dict__.get('created_by')
        strategy = record.__dict__.get('strategy')
        symbol = record.__dict__.get('symbol')
        position = record.__dict__.get('position')
        kwargs = {
            'logger_name': record.name,
            'level': record.levelno,
            'msg': msg,
            'trace': trace,
            'created_by': created_by,
            'strategy': strategy,
            'symbol': symbol,
            'position': position
        }
        if strategy:
            StatusLog.objects.create(**kwargs)
