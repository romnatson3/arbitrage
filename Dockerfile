FROM python:3.12

WORKDIR /app
COPY . /app

RUN pip install --upgrade pip && pip install -r requirements.txt

RUN chmod +x /app/entrypoint.sh

RUN mkdir -p /opt/csv/

ENTRYPOINT ["/app/entrypoint.sh"]
