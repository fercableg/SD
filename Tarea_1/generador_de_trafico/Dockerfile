FROM python:3.11-slim

WORKDIR /app

RUN pip install --no-cache-dir uvicorn numpy requests polars fastapi redis 

COPY generadorTrafico.py ./

CMD ["uvicorn", "generadorTrafico:app", "--host", "0.0.0.0", "--port", "8001"]