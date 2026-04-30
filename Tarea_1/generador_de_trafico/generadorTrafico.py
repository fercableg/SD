import numpy as np
import time
import requests
import os
import json

# URL del servicio de respuestas (puedes cambiarla con variable de entorno)
RESPUESTAS_URL = os.getenv("RESPUESTAS_URL", "http://cache:8000")

def comuna (numero):   
    match numero:      
        case 1:
            provincia = "Providencia"
        case 2:
            provincia = "Las Condes"
        case 3:
            provincia = "Maipú"
        case 4:
            provincia = "Santiago Centro"
        case 5:
            provincia = "Pudahuel"
    return provincia

def enviar_query(peticion, provincia, confianza, provincia2=None):
    payload = {
        "tipo": f"Q{peticion}",
        "provincia": provincia,
        "confianza": confianza
    }
    if provincia2:
        payload["provincia2"] = provincia2
    try:
        response = requests.post(f"{RESPUESTAS_URL}/query", json=payload, timeout=30)
        if response.status_code == 200:
            data = response.json()
            pretty_json = json.dumps(data, indent=2, ensure_ascii=False)
            print(f"  [HTTP] Respuesta {response.status_code}:\n{pretty_json}")
        else:
            print(f"  [HTTP] Error {response.status_code}: {response.text}")
    except Exception as e:
        print(f"  [ERROR] No se pudo enviar: {e}")
"""
Generador de Tráfico: Simula solicitudes de empresas de reparto y logística que consultan zonas de Santia-
go. Genera consultas automáticamente siguiendo dos distribuciones de tasa de arribo, las cuales son la Ley
de potencia (Zipf) y distribución uniforme respectivamente. Cada consulta incluye el tipo de operación
(Q1–Q5), la zona geográfica (bounding box) y los parámetros asociados. Las consultas son completamente
sintéticas: se construyen a partir de las zonas predefinidas y los parámetros del dataset, sin interacción
con una base de datos externa.
"""

opcion = int(input("¿Qué distribución? 1. Uniforme   2. Zipf: "))

if opcion == 1:
    print("Generando peticiones con distribucion uniforme")
    
    while True:
        #Calcular un numero para determinar la query
        peticion = np.random.randint(1, 6)
        #Calcular un numero para determinar la provincia o comuna
        numeroProvincia = np.random.randint(1, 6)
        
        provincia = comuna (numeroProvincia)

        #Crear el decimal del nivel de confianza
        confianza = np.random. randint(1, 10)/10

        #Match para crear las peticiones
        match peticion:
            case 1:     
                print(f"Preguntando Q1, provincia: {provincia}, nivel de confianza: {confianza}")
                enviar_query(1, provincia, confianza)
            case 2:
                print(f"Preguntando Q2, provincia: {provincia}, nivel de confianza: {confianza}")
                enviar_query(2, provincia, confianza)
            case 3:
                print(f"Preguntando Q3, provincia: {provincia}, nivel de confianza: {confianza}")
                enviar_query(3, provincia, confianza)
            case 4:
                #Obtenemos la segunda comuna
                numeroProvincia2 = np.random.randint(1, 6) 
                provincia2 = comuna (numeroProvincia2)
                print(f"Preguntando Q4, provincia: {provincia}, provincia 2 {provincia2} nivel de confianza: {confianza}")
                enviar_query(4, provincia, confianza, provincia2)
            case 5:
                print(f"Preguntando Q5, provincia: {provincia}, nivel de confianza: {confianza}")
                enviar_query(5, provincia, confianza)

        time.sleep(5) 

elif opcion == 2:
   
    print("Generando peticiones: tipo de consulta Zipf, provincia y confianza uniformes")
    #Este alpha nos determina que tan empinada es la grafica zipf, osea que tanto se repiten las consultas
    alpha = 2.0 

    #Función para generar entre 1 y 5 con zipf
    def zipf():
        while True:
            x = np.random.zipf(alpha)
            if 1 <= x <= 5:
                return x

    while True:
        peticion = zipf()
        #Calcular un numero para determinar la provincia o comuna
        numeroProvincia = np.random.randint(1, 6)
        #match para determinar la comuna en base al numero obtenido
        provincia = comuna(numeroProvincia)
        confianza = np.random. randint(1, 10)/10

        #Match para crear las peticiones
        match peticion:
            case 1:     
                print(f"Preguntando Q1, provincia: {provincia}, nivel de confianza: {confianza}")
                enviar_query(1, provincia, confianza)
            case 2:
                print(f"Preguntando Q2, provincia: {provincia}, nivel de confianza: {confianza}")
                enviar_query(2, provincia, confianza)
            case 3:
                print(f"Preguntando Q3, provincia: {provincia}, nivel de confianza: {confianza}")
                enviar_query(3, provincia, confianza)
            case 4:
                #Obtenemos la segunda comuna
                numeroProvincia2 = np.random.randint(1, 6)  
                provincia2 = comuna (numeroProvincia2)
                print(f"Preguntando Q4, provincia: {provincia}, provincia 2 {provincia2} nivel de confianza: {confianza}")
                enviar_query(4, provincia, confianza, provincia2)
            case 5:
                print(f"Preguntando Q5, provincia: {provincia}, nivel de confianza: {confianza}")
                enviar_query(5, provincia, confianza)

        time.sleep(5)