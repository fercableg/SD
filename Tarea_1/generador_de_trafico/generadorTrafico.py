import numpy as np
import asyncio
import os
import json
import time
import uuid
from aiokafka import AIOKafkaProducer
from aiokafka.admin import AIOKafkaAdminClient, NewTopic

# Configuración
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "consultas-principal")
TOTAL = int(os.getenv("TOTAL_MENSAJES", "1000"))        # CAMBIADO A 1000
CONCURRENCIA = int(os.getenv("CONCURRENCIA", "1000"))
DISTRIBUCION = int(os.getenv("DISTRIBUCION", "1"))  # 1=Uniforme, 2=Zipf

def comuna(numero):
    match numero:
        case 1: return "Providencia"
        case 2: return "Las Condes"
        case 3: return "Maipú"
        case 4: return "Santiago Centro"
        case 5: return "Pudahuel"

def generar_query():
    alpha = 2.0
    def zipf():
        while True:
            x = np.random.zipf(alpha)
            if 1 <= x <= 5:
                return x
    if DISTRIBUCION == 1:
        peticion = np.random.randint(1, 6)
    else:
        peticion = zipf()
    numero_provincia = np.random.randint(1, 6)
    provincia = comuna(numero_provincia)
    confianza = np.random.randint(1, 10) / 10
    provincia2 = None
    if peticion == 4:
        provincia2 = comuna(np.random.randint(1, 6))
    return peticion, provincia, confianza, provincia2

async def crear_topic_si_no_existe():
    admin = AIOKafkaAdminClient(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)

    await admin.start()

    try:
        topics = await admin.list_topics()
        if KAFKA_TOPIC not in topics:
            await admin.create_topics([NewTopic(name=KAFKA_TOPIC, num_partitions=3, replication_factor=1)])
            print(f"[Kafka] Topic '{KAFKA_TOPIC}' created.")
        else:
            print(f"[Kafka] Topic '{KAFKA_TOPIC}' already exists.")
    except Exception as e:
        print(f"[Warning] {e}")
    finally:
        await admin.close()

async def publicar(producer):
    peticion, provincia, confianza, provincia2 = generar_query()
    mensaje = {
        "id_unico": str(uuid.uuid4()),
        "timestamp_creacion": time.time(),
        "intentos": 0,
        "tipo": f"Q{peticion}",
        "provincia": provincia,
        "confianza": confianza
    }
    if provincia2:
        mensaje["provincia2"] = provincia2
    await producer.send(KAFKA_TOPIC, mensaje, key=mensaje["id_unico"])

async def main():
    await crear_topic_si_no_existe()
    
    print(f"Burst mode: sending {TOTAL} messages with concurrency {CONCURRENCIA}")
    
    producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        key_serializer=lambda k: str(k).encode('utf-8'),
        max_batch_size=16384,
        linger_ms=5
    )

    await producer.start()

    semaforo = asyncio.Semaphore(CONCURRENCIA)
    queries_enviadas = 0
    start = time.time()

    async def enviar():
        nonlocal queries_enviadas

        async with semaforo:
            await publicar(producer)

            queries_enviadas += 1

            if queries_enviadas % 1000 == 0:
                tiempo_transcurrido = time.time() - start

                if tiempo_transcurrido > 0:
                    rate_de_queries = queries_enviadas / tiempo_transcurrido 
                else:
                    rate_de_queries = 0

            print(f"El total de queries enviadas son: {queries_enviadas}/{TOTAL}. Promedio de queries: {rate_de_queries:.0f} queries/s)")

    tareas = [enviar() for _ in range(TOTAL)]
    
    await asyncio.gather(*tareas)

    tiempo_transcurrido = time.time() - start

    print(f"\n {TOTAL} de queries enviadas en {tiempo_transcurrido:.2f}s. Promedio de queries: {TOTAL/tiempo_transcurrido:.0f} queries/s")

    await producer.stop()

if __name__ == "__main__":
    asyncio.run(main())