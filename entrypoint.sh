python manage.py migrate
python manage.py createsuperuser --noinput
python manage.py users_handler
python manage.py update_symbols
python manage.py collectstatic --no-input --clear
gunicorn exchange.wsgi:application --workers=2 --log-level=info --bind 0.0.0.0:8000
