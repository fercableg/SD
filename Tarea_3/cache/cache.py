import redis
import requests
import json
import time
from datetime import datetime, timezone
from fastapi import FastAPI, Body
from kafka import KafkaProducer

app = FastAPI()
db = redis.Redis(host='redis-db', port=6379, decode_responses=True)

GENERADOR_URL = "http://generador_de_respuestas:8001"
METRICS_TOPIC = "metrics-topic"

try:
    kafka_producer = KafkaProducer(
        bootstrap_servers=['kafka:9092'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
except Exception as e:
    print(f"Error en hacer el prodcuto Kafka {e}")
    kafka_producer = None


def construir_metrica(query_type, latencia_ms, cache_hit, status, zone_id, retry_count=None):
    return {
        "timestamp": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "query_type": query_type,
        "latency_ms": latencia_ms,
        "cache_hit": cache_hit,
        "retry_count": retry_count,
        "status": status,
        "zone_id": zone_id
    }


def enviar_metrica_kafka(evento):
    if kafka_producer is None:
        print("Kafka producer no disponible, métrica perdida.")
        return
    try:
        kafka_producer.send(METRICS_TOPIC, evento)
    except Exception as e:
        print(f"Error enviando métrica a Kafka: {e}")


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


@app.post("/query")
def generarCache(query: dict = Body(...)):
    tipo = query.get("tipo")
    zona = query.get("provincia", "unknown")
    startTime = time.time()

    queryKey = generarLlavesCache(query)
    cacheValue = db.get(queryKey)

    if cacheValue is not None:
        # --- HIT ---
        latencia = round((time.time() - startTime) * 1000, 2)
        resultado = json.loads(cacheValue)

        evento = construir_metrica(
            query_type=tipo,
            latencia_ms=latencia,
            cache_hit=True,
            status="success",
            zone_id=zona,
            retry_count=None
        )
        enviar_metrica_kafka(evento)

        return {"source": "cache", "result": resultado, "latency_ms": latencia}

    else:
        # --- MISS ---
        try:
            respuesta = requests.post(f"{GENERADOR_URL}/query", json=query, timeout=10)
            datosRespuesta = respuesta.json()
        except requests.exceptions.RequestException as e:
            latencia_total = round((time.time() - startTime) * 1000, 2)
            evento = construir_metrica(
                query_type=tipo,
                latencia_ms=latencia_total,
                cache_hit=False,
                status="error",
                zone_id=zona,
                retry_count=None
            )
            enviar_metrica_kafka(evento)
            return {"error": "Generador de respuestas no disponible", "detalle": str(e)}

        if "result" not in datosRespuesta:
            latencia_total = round((time.time() - startTime) * 1000, 2)
            evento = construir_metrica(
                query_type=tipo,
                latencia_ms=latencia_total,
                cache_hit=False,
                status="error",
                zone_id=zona,
                retry_count=None
            )
            enviar_metrica_kafka(evento)
            return {"error": "Error en el generador de respuestas", "detalle": datosRespuesta}

        resultado = datosRespuesta["result"]
        ttl = calcularTTL(tipo)
        db.setex(queryKey, ttl, json.dumps(resultado))

        latencia_total = round((time.time() - startTime) * 1000, 2)

        evento = construir_metrica(
            query_type=tipo,
            latencia_ms=latencia_total,
            cache_hit=False,
            status="success",
            zone_id=zona,
            retry_count=None
        )
        enviar_metrica_kafka(evento)

        return {"source": "database", "result": resultado, "latency_ms": latencia_total}