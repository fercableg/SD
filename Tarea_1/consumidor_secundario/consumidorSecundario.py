import asyncio
import json
import os
import aiohttp
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC_REINTENTO = os.getenv("TOPIC_REINTENTO", "consultas-reintento")
TOPIC_DLQ = os.getenv("TOPIC_DLQ", "consultas-dlq")
CACHE_URL = os.getenv("CACHE_URL", "http://cache:8000")
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "3"))
RETRY_DELAY = int(os.getenv("RETRY_DELAY", "10")) 
GROUP_ID = os.getenv("GROUP_ID", "grupo-reintentos")

async def procesar_consulta(session, consulta):
    # misma función que en el principal
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
        TOPIC_REINTENTO,  # solo reintentos
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=GROUP_ID,
        enable_auto_commit=False,
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )
    producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    await consumer.start()
    await producer.start()
    async with aiohttp.ClientSession() as session:
        try:
            async for msg in consumer:
                consulta = msg.value
                id_unico = consulta.get('id_unico', '?')
                intentos = consulta.get('intentos', 0)
                print(f"[Reintento] Recibida: {id_unico} intentos={intentos}")
                
                # Espera deliberada antes de procesar
                await asyncio.sleep(RETRY_DELAY)
                
                try:
                    exito = await procesar_consulta(session, consulta)
                    if exito:
                        print(f"✅ Reintento exitoso: {id_unico}")
                        await consumer.commit()
                    else:
                        raise Exception("Falló")
                except Exception as e:
                    print(f"❌ Error en reintento: {e}")
                    nuevo_intentos = intentos + 1
                    consulta['intentos'] = nuevo_intentos
                    if nuevo_intentos <= MAX_RETRIES:
                        print(f"↻ Reintento {nuevo_intentos} para {id_unico}")
                        await producer.send(TOPIC_REINTENTO, consulta)  # vuelve a reintento
                    else:
                        print(f"💀 DLQ: {id_unico}")
                        await producer.send(TOPIC_DLQ, consulta)
                    await consumer.commit()
        finally:
            await consumer.stop()
            await producer.stop()

if __name__ == "__main__":
    asyncio.run(consumir_reintentos())