FROM python:3.11

WORKDIR /app

RUN pip install fastapi uvicorn polars

COPY generadorRespuestas.py .
COPY dataset /app/dataset

EXPOSE 8000

CMD ["uvicorn", "generadorRespuestas:app", "--host", "0.0.0.0", "--port", "8000"]