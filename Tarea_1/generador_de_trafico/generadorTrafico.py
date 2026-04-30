import numpy as np
import asyncio
import aiohttp
import os
import json
import time

RESPUESTAS_URL = os.getenv("RESPUESTAS_URL", "http://localhost:8000")

def comuna(numero):
    match numero:
        case 1: 
            return "Providencia"
        case 2: 
            return "Las Condes"
        case 3: 
            return "Maipú"
        case 4: 
            return "Santiago Centro"
        case 5: 
            return "Pudahuel"

async def enviar_query(session, peticion, provincia, confianza, provincia2=None):
    payload = {
        "tipo": f"Q{peticion}",
        "provincia": provincia,
        "confianza": confianza
    }
    if provincia2:
        payload["provincia2"] = provincia2
    try:
        async with session.post(f"{RESPUESTAS_URL}/query", json=payload) as response:
            data = await response.json()
            pretty_json = json.dumps(data, indent=2, ensure_ascii=False)
            print(f"  [HTTP] Respuesta {response.status}:\n{pretty_json}")
    except Exception as e:
        print(f"  [ERROR] No se logro enviar: {e}")

def generarQuery(opcion):
    alpha = 2.0

    def zipf():
        while True:
            x = np.random.zipf(alpha)
            if 1 <= x <= 5:
                return x

    if opcion == 1:
        peticion = np.random.randint(1, 6)       # uniforme
    else:
        peticion = zipf()                         # zipf

    numeroProvincia = np.random.randint(1, 6)
    provincia = comuna(numeroProvincia)
    confianza = np.random.randint(1, 10) / 10
    provincia2 = None

    if peticion == 4:
        provincia2 = comuna(np.random.randint(1, 6))

    return peticion, provincia, confianza, provincia2

async def enviar_N_queries(nQuerys, opcion):
    tiempoInicial = time.time()

    # Create all tasks and send them all at once
    async with aiohttp.ClientSession() as session:
        tareas = []
        for _ in range(nQuerys):
            peticion, provincia, confianza, provincia2 = generarQuery(opcion)
            tarea = enviar_query(session, peticion, provincia, confianza, provincia2)
            tareas.append(tarea)

        await asyncio.gather(*tareas)

    tiempoFinal = time.time()
    total = round(tiempoFinal - tiempoInicial, 2)
    throughput = round(nQuerys / total, 2)
    print(f"\n {nQuerys} queries enviadas en {total} segundos, y {throughput} queries/seg")

opcion = int(input("¿Qué distribución? 1. Uniforme  2. Zipf: "))
nQuerys = int(input("¿Cuántas queries quiere enviar? N: "))

asyncio.run(enviar_N_queries(nQuerys, opcion))