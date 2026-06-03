import asyncio
import json
import os
import time
import aiohttp
import redis
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC_REINTENTO = os.getenv("TOPIC_REINTENTO", "consultas-reintento")
TOPIC_DLQ = os.getenv("TOPIC_DLQ", "consultas-dlq")
CACHE_URL = os.getenv("CACHE_URL", "http://cache:8000")
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "3"))
RETRY_DELAY = int(os.getenv("RETRY_DELAY", "0.1"))
GROUP_ID = os.getenv("GROUP_ID", "grupo-reintentos")

redis_client = redis.Redis(host='redis-db', port=6379, decode_responses=True)

async def conexion_kafka(consumer, producer, timeout=60):
    start = asyncio.get_event_loop().time()
    while True:
        try:
            await consumer.start()
            await producer.start()
            
            print("Conexión del consumidor de reintentos con Kafka lista")

            return
        
        except Exception as error:
            if asyncio.get_event_loop().time() - start > timeout:
                raise
            print(f"Intentando conexión con Kafka, {error}")
            await asyncio.sleep(2)

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
            return True
        raise Exception(f"Cache: {respuesta.status}")

async def consumidor_reintentos():
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

    await conexion_kafka(consumer, producer) # Conexion con Kafka

    async with aiohttp.ClientSession() as session:
        try:
            async for mensaje in consumer:
                consulta = mensaje.value
                id_unico = consulta.get('id_unico', '-')
                intentos = consulta.get('intentos', 0)
                
                start = time.time()

                print(f"Querie con {id_unico} y numero de intentos {intentos}")
                
                await asyncio.sleep(RETRY_DELAY) #Itentando ver si le quedan intentos a la consulta

                try:
                    exito = await procesar_consulta(session, consulta)
                    latencia = (time.time() - start) * 1000
                    if exito:
                        print(f"Querie {id_unico} consumida correctamente")

                        redis_client.rpush(
                            "metricas", json.dumps({
                                "type": "recovery_success", 
                                "consulta_id": id_unico, 
                                "intentos": intentos,
                                "timestamp": time.time(), 
                                "latency_ms": latencia
                            }))

                        await consumer.commit()

                except Exception as error:
                    latencia = (time.time() - start) * 1000

                    print(f"Reintentos fallidos con {intentos} intentos, con querie {id_unico}: {error}")
                    
                    nuevo_intentos = intentos + 1
                    consulta['intentos'] = nuevo_intentos

                    if nuevo_intentos <= MAX_RETRIES:
                        print(f"Mandando la consulta nuevamente, con reintento: {nuevo_intentos})")

                        await producer.send(TOPIC_REINTENTO, consulta)

                        redis_client.rpush(
                            "metricas", json.dumps({
                                "type": "retry_sent", 
                                "consulta_id": id_unico, 
                                "intentos": nuevo_intentos,
                                "timestamp": time.time(), 
                                "latency_ms": latencia
                            }))
                    else:
                        print(f"Enviando querie a DLQ con id: {id_unico}, numeros de intentos realizados {MAX_RETRIES}")
                       
                        await producer.send(TOPIC_DLQ, consulta)

                        redis_client.rpush(
                            "metricas", json.dumps({
                                "type": "dlq", 
                                "consulta_id": id_unico, 
                                "intentos": nuevo_intentos,
                                "timestamp": time.time(), 
                                "latency_ms": latencia
                            }))
                        
                    await consumer.commit()
        finally:
            await consumer.stop()
            await producer.stop()

if __name__ == "__main__":
    asyncio.run(consumidor_reintentos())