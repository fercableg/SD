FROM python:3.11

WORKDIR /app

RUN apt-get update && apt-get install -y build-essential 
    
RUN pip install fastapi numpy redis requests polars uvicorn 

COPY . .
