import numpy as np
import asyncio
import os
import json
import time
import uuid
import argparse
from aiokafka import AIOKafkaProducer
from aiokafka.admin import AIOKafkaAdminClient, NewTopic

# ========== CONFIGURACIÓN POR DEFECTO ==========
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "consultas-principal")

def comuna(numero):
    match numero:
        case 1: return "Providencia"
        case 2: return "Las Condes"
        case 3: return "Maipú"
        case 4: return "Santiago Centro"
        case 5: return "Pudahuel"

def generar_query(tipo_distribucion):
    alpha = 2.0
    def zipf():
        while True:
            x = np.random.zipf(alpha)
            if 1 <= x <= 5:
                return x
    if tipo_distribucion == 1:   # Uniforme
        peticion = np.random.randint(1, 6)
    else:                         # Zipf
        peticion = zipf()
    numero_provincia = np.random.randint(1, 6)
    provincia = comuna(numero_provincia)
    confianza = np.random.randint(1, 10) / 10
    provincia2 = None
    if peticion == 4:   # Q4 requiere dos provincias
        provincia2 = comuna(np.random.randint(1, 6))
    return peticion, provincia, confianza, provincia2

async def crear_topic_si_no_existe():
    admin = AIOKafkaAdminClient(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)
    await admin.start()
    try:
        topics = await admin.list_topics()
        if KAFKA_TOPIC not in topics:
            await admin.create_topics([NewTopic(name=KAFKA_TOPIC, num_partitions=3, replication_factor=1)])
            print(f"[Kafka] Tópico '{KAFKA_TOPIC}' creado.")
        else:
            print(f"[Kafka] Tópico '{KAFKA_TOPIC}' ya existe.")
    except Exception as e:
        print(f"[Advertencia] {e}")
    finally:
        await admin.close()

async def publicar_query(producer, tipo_distribucion):
    peticion, provincia, confianza, provincia2 = generar_query(tipo_distribucion)
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
    await producer.send(KAFKA_TOPIC, mensaje, key=clave_mensaje)  # send, no send_and_wait

async def modo_burst(total, concurrencia, tipo_distribucion):
    """Envía 'total' mensajes lo más rápido posible, con concurrencia limitada."""
    print(f"Modo BURST: enviando {total} mensajes con concurrencia {concurrencia}...")
    producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        key_serializer=lambda k: str(k).encode('utf-8'),
        max_batch_size=16384,
        linger_ms=5
    )
    await producer.start()
    semaphore = asyncio.Semaphore(concurrencia)
    enviadas = 0
    start_time = time.time()

    async def enviar_una():
        nonlocal enviadas
        async with semaphore:
            await publicar_query(producer, tipo_distribucion)
            enviadas += 1
            if enviadas % 500 == 0:
                elapsed = time.time() - start_time
                rate = enviadas / elapsed if elapsed > 0 else 0
                print(f"  Progreso: {enviadas}/{total} (tasa actual: {rate:.1f} msg/s)")

    tareas = [enviar_una() for _ in range(total)]
    await asyncio.gather(*tareas)
    elapsed = time.time() - start_time
    print(f"\n✅ {total} mensajes enviados en {elapsed:.2f} segundos. Tasa media: {total/elapsed:.1f} msg/s")
    await producer.stop()

async def modo_tasa_constante(tasa, duracion, tipo_distribucion):
    """Envía a una tasa fija (msg/segundo) durante 'duracion' segundos."""
    print(f"Modo RATE: {tasa} msg/s durante {duracion} segundos...")
    producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        key_serializer=lambda k: str(k).encode('utf-8')
    )
    await producer.start()
    intervalo = 1.0 / tasa
    start_time = time.time()
    enviadas = 0
    while time.time() - start_time < duracion:
        await publicar_query(producer, tipo_distribucion)
        enviadas += 1
        if enviadas % 100 == 0:
            print(f"  Enviadas: {enviadas}")
        await asyncio.sleep(intervalo)
    elapsed = time.time() - start_time
    print(f"\n✅ {enviadas} mensajes enviados en {elapsed:.2f} segundos. Tasa real: {enviadas/elapsed:.1f} msg/s")
    await producer.stop()

async def modo_bucle_infinito(tasa, tipo_distribucion):
    """Envía mensajes indefinidamente a una tasa fija."""
    print(f"Modo LOOP: enviando mensajes a {tasa} msg/s (Ctrl+C para detener)...")
    producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        key_serializer=lambda k: str(k).encode('utf-8')
    )
    await producer.start()
    intervalo = 1.0 / tasa
    enviadas = 0
    start_time = time.time()
    try:
        while True:
            await publicar_query(producer, tipo_distribucion)
            enviadas += 1
            if enviadas % 100 == 0:
                elapsed = time.time() - start_time
                print(f"  Enviadas: {enviadas} (tasa real: {enviadas/elapsed:.1f} msg/s)")
            await asyncio.sleep(intervalo)
    except KeyboardInterrupt:
        elapsed = time.time() - start_time
        print(f"\n🛑 Detenido. {enviadas} mensajes enviados en {elapsed:.2f} segundos. Tasa media: {enviadas/elapsed:.1f} msg/s")
    finally:
        await producer.stop()

async def main():
    parser = argparse.ArgumentParser(description="Generador de tráfico para Kafka")
    parser.add_argument("--modo", choices=["burst", "rate", "loop"], default="burst",
                        help="Modo de generación: burst (ráfaga), rate (tasa fija por tiempo), loop (infinito)")
    parser.add_argument("--total", type=int, default=1000,
                        help="Número total de mensajes (para modo burst)")
    parser.add_argument("--tasa", type=float, default=100,
                        help="Mensajes por segundo (para rate y loop)")
    parser.add_argument("--duracion", type=int, default=30,
                        help="Duración en segundos (para modo rate)")
    parser.add_argument("--concurrencia", type=int, default=500,
                        help="Máximo de tareas concurrentes (para burst)")
    parser.add_argument("--distribucion", type=int, choices=[1,2], default=1,
                        help="1=Uniforme, 2=Zipf")
    args = parser.parse_args()

    await crear_topic_si_no_existe()

    if args.modo == "burst":
        await modo_burst(args.total, args.concurrencia, args.distribucion)
    elif args.modo == "rate":
        await modo_tasa_constante(args.tasa, args.duracion, args.distribucion)
    elif args.modo == "loop":
        await modo_bucle_infinito(args.tasa, args.distribucion)

if __name__ == "__main__":
    asyncio.run(main())