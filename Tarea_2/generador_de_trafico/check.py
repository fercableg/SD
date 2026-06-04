import asyncio
from aiokafka import AIOKafkaProducer

async def get_metadata():
    producer = AIOKafkaProducer(bootstrap_servers='localhost:9092')
    await producer.start()
    brokers = producer.client.cluster.brokers()
    print("Brokers anunciados por Kafka:")
    for broker in brokers:
        print(f"  {broker.host}:{broker.port}")
    await producer.stop()

asyncio.run(get_metadata())