FROM python:3.12

WORKDIR /app

COPY requirements.txt .

RUN pip install -r requirements.txt --no-cache-dir

COPY ../app/kafka_streams.py .

CMD ["faust", "-A", "kafka_streams", "worker", "-l", "INFO"]
