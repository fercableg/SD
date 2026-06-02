import asyncio
import json
import os
import time
import aiohttp
import redis
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC_PRINCIPAL = os.getenv("TOPIC_PRINCIPAL", "consultas-principal")
TOPIC_REINTENTO = os.getenv("TOPIC_REINTENTO", "consultas-reintento")
CACHE_URL = os.getenv("CACHE_URL", "http://cache:8000")
GROUP_ID = os.getenv("GROUP_ID", "grupo-consumidores")

redis_client = redis.Redis(host='redis-db', port=6379, decode_responses=True)

async def conectar_kafka(consumer, producer, timeout=60):
    start = asyncio.get_event_loop().time()
    while True:
        try:
            await consumer.start()
            await producer.start()
            print("[OK] Connected to Kafka")
            return
        except Exception as e:
            if asyncio.get_event_loop().time() - start > timeout:
                raise
            print(f"[WAIT] Waiting for Kafka... {e}")
            await asyncio.sleep(2)

async def procesar_consulta(session, consulta):
    payload = {
        "tipo": consulta.get("tipo"),
        "provincia": consulta.get("provincia"),
        "confianza": consulta.get("confianza")
    }
    if "provincia2" in consulta:
        payload["provincia2"] = consulta["provincia2"]
    async with session.post(f"{CACHE_URL}/query", json=payload, timeout=10) as resp:
        if resp.status < 300:
            return True
        raise Exception(f"Cache status {resp.status}")

async def consumir():
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
    await conectar_kafka(consumer, producer)

    async with aiohttp.ClientSession() as session:
        try:
            async for msg in consumer:
                consulta = msg.value
                id_unico = consulta.get('id_unico', '?')
                intentos = consulta.get('intentos', 0)
                start = time.time()
                print(f"[MAIN] Received {id_unico} attempts={intentos}")

                try:
                    ok = await procesar_consulta(session, consulta)
                    lat = (time.time() - start) * 1000
                    if ok:
                        print(f"[OK] Success for {id_unico}")
                        redis_client.rpush("metricas", json.dumps({
                            "type": "success", "consulta_id": id_unico, "intentos": intentos,
                            "timestamp": time.time(), "latency_ms": lat
                        }))
                        await consumer.commit()
                except Exception as e:
                    lat = (time.time() - start) * 1000
                    print(f"[ERROR] {e} -> sending to retry topic")
                    consulta['intentos'] = intentos + 1
                    await producer.send(TOPIC_REINTENTO, consulta)
                    redis_client.rpush("metricas", json.dumps({
                        "type": "retry_sent", "consulta_id": id_unico, "intentos": intentos+1,
                        "timestamp": time.time(), "latency_ms": lat
                    }))
                    await consumer.commit()   # Confirm the original message
        finally:
            await consumer.stop()
            await producer.stop()

if __name__ == "__main__":
    asyncio.run(consumir())