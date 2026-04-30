FROM python:3.11-slim
WORKDIR /app
RUN pip install --no-cache-dir numpy requests aiohttp
COPY generadorTrafico.py .
CMD ["python", "generadorTrafico.py"]