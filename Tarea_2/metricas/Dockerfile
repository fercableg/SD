FROM python:3.11-slim

WORKDIR /app

RUN pip install --no-cache-dir numpy polars redis

COPY metricas.py .

CMD ["python", "metricas.py"]