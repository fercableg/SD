import asyncio
import json
import os
import time
import aiohttp
import redis
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from datetime import datetime

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC_PRINCIPAL = os.getenv("TOPIC_PRINCIPAL", "consultas-principal")
TOPIC_REINTENTO = os.getenv("TOPIC_REINTENTO", "consultas-reintento")
CACHE_URL = os.getenv("CACHE_URL", "http://cache:8000")
GROUP_ID = os.getenv("GROUP_ID", "grupo-consumidores")

redis_client = redis.Redis(host='redis-db', port=6379, decode_responses=True)

async def conexion_kafka(consumer, producer, timeout=60):
    start = asyncio.get_event_loop().time()
    while True:
        try:
            await consumer.start()
            await producer.start()

            print("Conexión del consumidor principal con Kafka lista")

            return

        except Exception as error:
            if asyncio.get_event_loop().time() - start > timeout:
                raise
            print(f"Intentando conexión con Kafka, {error}")
            await asyncio.sleep(2)

async def enviar_metrica_kafka(producer, metrica):
    """Envía una métrica al topic Kafka metrics-topic usando el productor dado"""
    try:
        await producer.send('metrics-topic', metrica)
    except Exception as e:
        print(f"Error sending metric to Kafka: {e}")

async def procesar_consulta(session, consulta):
    payload = {
        "tipo": consulta.get("tipo"),
        "provincia": consulta.get("provincia"),
        "confianza": consulta.get("confianza")
    }

    if "provincia2" in consulta:
        payload["provincia2"] = consulta["provincia2"]

    async with session.post(f"{CACHE_URL}/query", json=payload, timeout=10) as respuesta:
        if respuesta.status < 300:
            datos = await respuesta.json()
            return datos
        raise Exception(f"Cache: {respuesta.status}")

async def consumidor():
    consumer = AIOKafkaConsumer(
        TOPIC_PRINCIPAL,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=GROUP_ID,
        enable_auto_commit=False,
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )

    producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    await conexion_kafka(consumer, producer) # Conexion con Kafka

    async with aiohttp.ClientSession() as session:
        try:
            async for mensaje in consumer:
                consulta = mensaje.value
                id_unico = consulta.get('id_unico', '-')
                intentos = consulta.get('intentos', 0)

                start = time.time()

                print(f"Querie con {id_unico} y numero de intentos {intentos}")

                try:
                    datos_respuesta = await procesar_consulta(session, consulta)
                    latencia = (time.time() - start) * 1000

                    if "error" in datos_respuesta:
                        raise Exception(f"Cache devolvió error: {datos_respuesta.get('detalle')}")

                    source = datos_respuesta.get("source")  # "cache" o "database"
                    cache_hit = True if source == "cache" else False if source == "database" else None

                    print(f"Querie {id_unico} consumida correctamente (source={source})")

                    # Store metric in Redis (existing behavior)
                    redis_client.rpush(
                        "metricas", json.dumps({
                            "type": "success",
                            "consulta_id": id_unico,
                            "intentos": intentos,
                            "timestamp": time.time(),
                            "latency_ms": latencia
                        }))

                    # Send metric to Kafka
                    metrica_kafka = {
                        "timestamp": datetime.utcfromtimestamp(time.time()).isoformat() + 'Z',
                        "query_type": consulta.get("tipo", "unknown"),
                        "latency_ms": round(latencia, 2),
                        "cache_hit": cache_hit,
                        "retry_count": intentos,  # Number of attempts made so far (including this one?)
                        "status": "success",
                        "zone_id": consulta.get("provincia", "unknown")
                    }
                    await enviar_metrica_kafka(producer, metrica_kafka)

                    await consumer.commit()

                except Exception as error:
                    latencia = (time.time() - start) * 1000

                    print(f"Error al enviar la querie: {error}")

                    consulta['intentos'] = intentos + 1

                    await producer.send(TOPIC_REINTENTO, consulta)

                    # Store metric in Redis (existing behavior)
                    redis_client.rpush(
                        "metricas", json.dumps({
                            "type": "retry_sent",
                            "consulta_id": id_unico,
                            "intentos": intentos+1,
                            "timestamp": time.time(),
                            "latency_ms": latencia
                        }))

                    # Send metric to Kafka
                    metrica_kafka = {
                        "timestamp": datetime.utcfromtimestamp(time.time()).isoformat() + 'Z',
                        "query_type": consulta.get("tipo", "unknown"),
                        "latency_ms": round(latencia, 2),
                        "retry_count": intentos+1,  # Number of attempts made so far (including this one?)
                        "status": "retry_sent",
                        "zone_id": consulta.get("provincia", "unknown")
                    }
                    await enviar_metrica_kafka(producer, metrica_kafka)

                    await consumer.commit()
        finally:
            await consumer.stop()
            await producer.stop()

if __name__ == "__main__":
    asyncio.run(consumidor())