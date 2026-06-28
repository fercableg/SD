import redis
import requests
import json
import time
from datetime import datetime
from fastapi import FastAPI, Body
from kafka import KafkaProducer

app = FastAPI()
db = redis.Redis(host='redis-db', port=6379, decode_responses=True)

GENERADOR_URL = "http://generador_de_respuestas:8001"

# Kafka producer for metrics
try:
    kafka_producer = KafkaProducer(
        bootstrap_servers=['kafka:9092'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
except Exception as e:
    print(f"Failed to initialize Kafka producer: {e}")
    kafka_producer = None

def enviar_metrica_kafka(metrica):
    """Envía una métrica al topic Kafka metrics-topic"""
    if kafka_producer is not None:
        try:
            kafka_producer.send('metrics-topic', metrica)
        except Exception as e:
            print(f"Error sending metric to Kafka: {e}")

def generarLlavesCache(query):
    tipo = query.get("tipo", "unknown")
    confianza = query.get("confianza", 0.0)
    zona = query.get("provincia", "unknown")

    if tipo == "Q1":
        return f"count:{zona}:conf={confianza}"
    elif tipo == "Q2":
        return f"area:{zona}:conf={confianza}"
    elif tipo == "Q3":
        return f"density:{zona}:conf={confianza}"
    elif tipo == "Q4":
        return f"compare:{zona}:{query.get('provincia2')}:conf={confianza}"
    elif tipo == "Q5":
        return f"conf_dist:{zona}:bins=5"
    else:
        return f"unknown:{tipo}"

def calcularTTL(query_type):
    ttls = {
        "Q1": 1800,
        "Q2": 1800,
        "Q3": 500,
        "Q4": 500,
        "Q5": 500
    }
    return ttls.get(query_type, 500)

def registrarMetrica(evento, query_type, cache_key, latencia_ms, latencia_db_ms):
    registro = {
        "timestamp": time.time(),
        "event": evento,
        "query_type": query_type,
        "cache_key": cache_key,
        "latency_total_ms": latencia_ms,
        "latency_db_ms": latencia_db_ms
    }
    db.rpush("metricas", json.dumps(registro))

@app.post("/query")
def generarCache(query: dict = Body(...)):
    tipo = query.get("tipo")
    startTime = time.time()

    # Use the proper key builder
    queryKey = generarLlavesCache(query)
    cacheValue = db.get(queryKey)

    if cacheValue is not None:
        # --- HIT ---
        latencia = round((time.time() - startTime) * 1000, 2)
        resultado = json.loads(cacheValue)
        registrarMetrica("HIT", tipo, queryKey, latencia, 0)

        # Prepare and send metric to Kafka
        metrica_kafka = {
            "timestamp": datetime.utcfromtimestamp(time.time()).isoformat() + 'Z',
            "query_type": tipo,
            "latency_ms": latencia,
            "cache_hit": True,
            "retry_count": None,  # Not available in cache service
            "status": "success",
            "zone_id": query.get("provincia", "unknown")
        }
        enviar_metrica_kafka(metrica_kafka)

        return {"source": "cache", "result": resultado, "latency_ms": latencia}

    else:
        # --- MISS ---
        respuesta = requests.post(f"{GENERADOR_URL}/query", json=query)
        datosRespuesta = respuesta.json()

        if "result" not in datosRespuesta:
            return {"error": "Error en el generador de respuestas", "detalle": datosRespuesta}

        resultado = datosRespuesta["result"]
        tiempoGeneradorRespuesta = round((time.time() - startTime) * 1000, 2)

        ttl = calcularTTL(tipo)
        db.setex(queryKey, ttl, json.dumps(resultado))

        latencia_total = round((time.time() - startTime) * 1000, 2)
        registrarMetrica("MISS", tipo, queryKey, latencia_total, tiempoGeneradorRespuesta)

        # Prepare and send metric to Kafka
        metrica_kafka = {
            "timestamp": datetime.utcfromtimestamp(time.time()).isoformat() + 'Z',
            "query_type": tipo,
            "latency_ms": latencia_total,
            "cache_hit": False,
            "retry_count": None,  # Not available in cache service
            "status": "success",
            "zone_id": query.get("provincia", "unknown")
        }
        enviar_metrica_kafka(metrica_kafka)

        return {"source": "database", "result": resultado, "latency_ms": latencia_total}