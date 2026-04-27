import redis
import requests
import json
import time
from fastapi import FastAPI

app = FastAPI()
db = redis.Redis(host='redis-db', port=6379, decode_responses=True)

GENERADOR_URL = "http://generador_de_respuestas:8001"

# Como Redis almacena por key-value y se pide que en la figura de la tarea se desarrolle así, queda el cache como:

def generarLlavesCache(query):

    tipoDeQuery = query["queryTipo"]
    confianzaDeQuery = query.get("minimaConfianza", 0.0)

    if tipoDeQuery == "Q1":
        key = f"count:{query['zonaId']}:confidence={confianzaDeQuery}"

    elif tipoDeQuery == "Q2":
        key = f"area:{query['zonaId']}:confidence={confianzaDeQuery}"

    elif tipoDeQuery == "Q3":
        key = f"density:{query['zonaId']}:confidence={confianzaDeQuery}"

    elif tipoDeQuery == "Q4":
        key = f"compare:density:{query['zonaId_1']}:{query['zonaId_2']}:confidence={confianzaDeQuery}"

    elif tipoDeQuery == "Q5":
        bins = query.get("bins", 5)
        key = f"confidence_distribution:{query['zone_id']}:bins={bins}"

    else:
        key = f"unknown:{tipoDeQuery}"

    return key

@app.post("/query")

def generarCache(query: dict):
    queryKey = generarLlavesCache(query)
    startTime = time.time()
    cacheValue = db.get(queryKey)

    if cacheValue is not None:
        # HIT! 
        finalTime = time.time()
        latencia = round((finalTime - startTime) * 1000, 2)

        resultado = json.loads(cacheValue)

        # guardar la metrica del hit
        metricas("HIT", query["query_type"], cacheValue, latencia, 0)

        return {
            "source": "cache",
            "cache_key": cacheValue,
            "result": resultado,
            "latency_ms": latencia
        }

    else:
        # MISS :(
        finalTime = time.time()

        #Esto lo manda al generador de respuestas por medio del protocolo HTTP
        respuesta = requests.post(f"{GENERADOR_URL}/query", json=query)
        datosRespuesta = respuesta.json()

        #Tiempo despues de ir al generador de respuesta
        tiempoGeneradorRespuesta = time.time()
        latenciaGeneradorRespuesta = round((tiempoGeneradorRespuesta - finalTime) * 1000, 2)

        resultado = datosRespuesta["result"]

        #Redis con TTL
        ttl = calcularTTL(query["query_type"])
        db.setex(cacheValue, ttl, json.dumps(resultado))

        finalTime = time.time()
        latencia = round((finalTime - startTime) * 1000, 2)

        # Guardar metrica del miss
        metricas("MISS", query["query_type"], cacheValue, latencia, latenciaGeneradorRespuesta)

        return {
            "source": "database",
            "cache_key": cacheValue,
            "result": resultado,
            "latency_ms": latencia
        }

def calcularTTL(query_type):
    ttls = {
        "Query_1": 30,
        "Query_2": 30,
        "Query_3": 30,
        "Query_4": 30,
        "Query_5": 30
    }
    return ttls.get(query_type, 30)


def metricas(evento, query_type, cacheValue, latencia_ms, latencia_db_ms):
    evento = {
        "timestamp": time.time(),
        "event": evento,
        "query_type": query_type,
        "cache_key": cacheValue,
        "latency_total_ms": latencia_ms,
        "latency_db_ms": latencia_db_ms
    }
    db.rpush("metricas", json.dumps(evento))