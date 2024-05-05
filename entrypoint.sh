python manage.py migrate
python manage.py createsuperuser --noinput
python manage.py users_handler
python manage.py bybit_futures_symbols_fill
python manage.py bybit_spot_symbols_fill
python manage.py collectstatic --no-input --clear
gunicorn trade.wsgi:application --workers=2 --log-level=info --bind 0.0.0.0:8000
