FROM python:3.12

WORKDIR /app
COPY . /app

RUN pip install --upgrade pip && pip install -r requirements.txt

RUN chmod +x /app/entrypoint.sh

ENTRYPOINT ["/app/entrypoint.sh"]
