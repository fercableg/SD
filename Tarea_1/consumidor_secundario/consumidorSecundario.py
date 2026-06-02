import asyncio
import json
import os
import time
import aiohttp
import redis
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

# Configuración desde variables de entorno
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC_REINTENTO = os.getenv("TOPIC_REINTENTO", "consultas-reintento")
TOPIC_DLQ = os.getenv("TOPIC_DLQ", "consultas-dlq")
CACHE_URL = os.getenv("CACHE_URL", "http://cache:8000")
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "3"))
RETRY_DELAY = int(os.getenv("RETRY_DELAY", "0.1"))
GROUP_ID = os.getenv("GROUP_ID", "grupo-reintentos")

# Cliente Redis (global)
redis_client = redis.Redis(host='redis-db', port=6379, decode_responses=True)

async def conectar_kafka(consumer, producer, timeout=60):
    """Reintenta conectar a Kafka hasta que esté disponible o se agote el tiempo."""
    start_time = asyncio.get_event_loop().time()
    while True:
        try:
            await consumer.start()
            await producer.start()
            print("✅ Conectado a Kafka exitosamente")
            return
        except Exception as e:
            elapsed = asyncio.get_event_loop().time() - start_time
            if elapsed > timeout:
                raise Exception(f"No se pudo conectar a Kafka después de {timeout}s: {e}")
            print(f"⏳ Esperando a Kafka... ({int(elapsed)}s) Error: {e}")
            await asyncio.sleep(2)

async def procesar_consulta(session, consulta):
    payload = {
        "tipo": consulta.get("tipo"),
        "provincia": consulta.get("provincia"),
        "confianza": consulta.get("confianza")
    }
    if "provincia2" in consulta:
        payload["provincia2"] = consulta["provincia2"]
    async with session.post(f"{CACHE_URL}/query", json=payload, timeout=10) as response:
        if response.status < 300:
            return True
        raise Exception(f"Cache status {response.status}")

async def consumir_reintentos():
    consumer = AIOKafkaConsumer(
        TOPIC_REINTENTO,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=GROUP_ID,
        enable_auto_commit=False,
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )
    producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    await conectar_kafka(consumer, producer)

    async with aiohttp.ClientSession() as session:
        try:
            async for msg in consumer:
                consulta = msg.value
                id_unico = consulta.get('id_unico', '?')
                intentos = consulta.get('intentos', 0)
                start_time = time.time()
                print(f"[Reintento] Recibida: {id_unico} intentos={intentos}")

                await asyncio.sleep(RETRY_DELAY)

                try:
                    exito = await procesar_consulta(session, consulta)
                    latency_ms = (time.time() - start_time) * 1000
                    if exito:
                        print(f"✅ Reintento exitoso: {id_unico}")
                        # Evento de recuperación exitosa
                        event = {
                            "type": "recovery_success",
                            "consulta_id": id_unico,
                            "intentos": intentos,
                            "timestamp": time.time(),
                            "latency_ms": latency_ms
                        }
                        redis_client.rpush("metricas", json.dumps(event))
                        await consumer.commit()
                    else:
                        raise Exception("Falló")
                except Exception as e:
                    latency_ms = (time.time() - start_time) * 1000
                    print(f"❌ Error en reintento: {e}")
                    nuevo_intentos = intentos + 1
                    consulta['intentos'] = nuevo_intentos
                    if nuevo_intentos <= MAX_RETRIES:
                        print(f"↻ Reintento {nuevo_intentos} para {id_unico}")
                        await producer.send(TOPIC_REINTENTO, consulta)
                        # Evento de reintento enviado
                        event = {
                            "type": "retry_sent",
                            "consulta_id": id_unico,
                            "intentos": nuevo_intentos,
                            "timestamp": time.time(),
                            "latency_ms": latency_ms
                        }
                        redis_client.rpush("metricas", json.dumps(event))
                    else:
                        print(f"💀 DLQ: {id_unico}")
                        await producer.send(TOPIC_DLQ, consulta)
                        # Evento de DLQ
                        event = {
                            "type": "dlq",
                            "consulta_id": id_unico,
                            "intentos": nuevo_intentos,
                            "timestamp": time.time(),
                            "latency_ms": latency_ms
                        }
                        redis_client.rpush("metricas", json.dumps(event))
                    await consumer.commit()
        finally:
            await consumer.stop()
            await producer.stop()

if __name__ == "__main__":
    asyncio.run(consumir_reintentos())