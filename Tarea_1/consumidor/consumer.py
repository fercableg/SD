import asyncio
import json
import os
import random
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
import aiohttp

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC_PRINCIPAL = os.getenv("TOPIC_PRINCIPAL", "consultas-principal")
TOPIC_REINTENTO = os.getenv("TOPIC_REINTENTO", "consultas-reintento")
TOPIC_DLQ = os.getenv("TOPIC_DLQ", "consultas-dlq")
CACHE_URL = os.getenv("CACHE_URL", "http://cache:8000")
RESPUESTAS_URL = os.getenv("RESPUESTAS_URL", "http://generador_de_respuestas:8001")
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "3"))
GROUP_ID = os.getenv("GROUP_ID", "grupo-consumidores")

async def procesar_consulta(consulta):
    """Procesa la consulta: primero caché, luego generador de respuestas."""
    # Simulación de fallos aleatorios para pruebas (puedes desactivar cambiando a 0)
    if random.random() < 0.3:
        raise Exception("Fallo temporal simulado")
    # Aquí iría la lógica real con aiohttp llamando a CACHE_URL y RESPUESTAS_URL
    # Por ahora, simulamos éxito
    return True

async def consumir():
    consumer = AIOKafkaConsumer(
        TOPIC_PRINCIPAL, TOPIC_REINTENTO,
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
    try:
        async for msg in consumer:
            consulta = msg.value
            print(f"Recibida: {consulta.get('id_unico', '?')} intentos={consulta.get('intentos',0)}")
            try:
                ok = await procesar_consulta(consulta)
                if ok:
                    print(f"✅ Éxito: {consulta['id_unico']}")
                    await consumer.commit()
                else:
                    raise Exception("Fallo intencional")
            except Exception as e:
                print(f"❌ Error: {e}")
                intentos = consulta.get('intentos', 0) + 1
                consulta['intentos'] = intentos
                if intentos <= MAX_RETRIES:
                    print(f"↻ Reintento {intentos} para {consulta['id_unico']}")
                    await producer.send(TOPIC_REINTENTO, consulta)
                else:
                    print(f"💀 DLQ: {consulta['id_unico']}")
                    await producer.send(TOPIC_DLQ, consulta)
                await consumer.commit()
    finally:
        await consumer.stop()
        await producer.stop()

if __name__ == "__main__":
    asyncio.run(consumir())