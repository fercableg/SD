import numpy as np
import asyncio
import os
import json
import time
import uuid
from aiokafka import AIOKafkaProducer
from aiokafka.admin import AIOKafkaAdminClient, NewTopic

# ========== CONFIGURACIÓN RÁPIDA ==========
CANTIDAD_CONSULTAS = 500      # Cambia aquí el número de consultas
DISTRIBUCION = 1              # 1 = Uniforme, 2 = Zipf
# ==========================================

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "consultas-principal")

def comuna(numero):
    match numero:
        case 1: return "Providencia"
        case 2: return "Las Condes"
        case 3: return "Maipú"
        case 4: return "Santiago Centro"
        case 5: return "Pudahuel"

def generarQuery(opcion):
    alpha = 2.0
    def zipf():
        while True:
            x = np.random.zipf(alpha)
            if 1 <= x <= 5:
                return x
    if opcion == 1:
        peticion = np.random.randint(1, 6)
    else:
        peticion = zipf()
    numeroProvincia = np.random.randint(1, 6)
    provincia = comuna(numeroProvincia)
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
            #Creamos el topic de kafka con 3 particiones para que pueda manejar de mejor forma los recursos
            await admin.create_topics([NewTopic(name=KAFKA_TOPIC, num_partitions=3, replication_factor=1)])
            print(f"  [Kafka] Tópico '{KAFKA_TOPIC}' creado.")
        else:
            print(f"  [Kafka] Tópico '{KAFKA_TOPIC}' ya existe.")
    except Exception as e:
        print(f"  [Advertencia] {e}")
    finally:
        await admin.close()

async def publicar_query(producer, opcion):
    peticion, provincia, confianza, provincia2 = generarQuery(opcion)
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

    clave_mensaje = mensaje["id_unico"]
    await producer.send_and_wait(KAFKA_TOPIC, mensaje, clave_mensaje)

async def enviar_N_queries(nQuerys, opcion, max_concurrent=1000, report_every=100):
    await crear_topic_si_no_existe()
    print(f"Iniciando envío de {nQuerys} consultas...")
    tiempo_inicial = time.time()

    #Acá configuramos como mandamos el mensaje a kafka, se puede dejar solo con value, pero agregamos el key para mantener el orden
    producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        key_serializer=lambda k: str(k).encode('utf-8')
    )
    await producer.start()

    semaphore = asyncio.Semaphore(max_concurrent)
    enviadas = 0

    async def enviar_con_limite():
        nonlocal enviadas
        async with semaphore:
            await publicar_query(producer, opcion)
            enviadas += 1
            if enviadas % report_every == 0:
                print(f"  Progreso: {enviadas}/{nQuerys} consultas enviadas")

    # Procesar en lotes
    batch_size = 5000
    for inicio in range(0, nQuerys, batch_size):
        fin = min(inicio + batch_size, nQuerys)
        tareas = [enviar_con_limite() for _ in range(fin - inicio)]
        await asyncio.gather(*tareas)

    await producer.stop()
    tiempo_final = time.time()
    total = round(tiempo_final - tiempo_inicial, 2)
    throughput = round(nQuerys / total, 2)
    print(f"\n✅ {nQuerys} consultas publicadas en {total} seg → {throughput} consultas/seg")

if __name__ == "__main__":
    asyncio.run(enviar_N_queries(CANTIDAD_CONSULTAS, DISTRIBUCION))