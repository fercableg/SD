from fastapi import FastAPI, Query
import polars as pl
import time
from typing import Optional          
from pydantic import BaseModel       

app = FastAPI()

df = pl.read_csv("dataset/967_buildings.csv")
print("Columnas:", df.columns)
print("Total filas:", df.height)

def zonaGeografica(longitud, latitud):
    # Providencia
    if latitud >= -33.445 and latitud <= -33.420 and longitud >= -70.640 and longitud <= -70.600:
        return "Providencia"
    # Las Condes
    elif latitud >= -33.420 and latitud <= -33.390 and longitud >= -70.600 and longitud <= -70.550:
        return "Las Condes"
    # Maipú
    elif latitud >= -33.530 and latitud <= -33.490 and longitud >= -70.790 and longitud <= -70.740:
        return "Maipú"  
    # Santiago Centro
    elif latitud >= -33.460 and latitud <= -33.430 and longitud >= -70.670 and longitud <= -70.630:
        return "Santiago Centro"
    # Pudahuel
    elif latitud >= -33.470 and latitud <= -33.430 and longitud >= -70.810 and longitud <= -70.760:
        return "Pudahuel"
    else:
        return "Desconocida"
    
#aca se calculo el area por cada comuna pero se definieron de modo de acortar procesos
def areaZonaGeografica(zona):
    # Providencia
    if zona == "Providencia" :
        return 10.28
    # Las Condes
    elif zona == "Las Condes":
        return 15.43
    # Maipú
    elif zona == "Maipú":
        return 20.54
    # Santiago Centro
    elif zona == "Santiago Centro":
        return 12.33
    # Pudahuel
    elif zona == "Pudahuel":
        return 20.55
    else:
        return  0   

#Conteo de edificios en una zona
def q1Conteo(zona: str, confidence_min: float = 0.0) -> int:
    contador = 0
    for i in range(df.height):
        fila = df.row(i)
        latitud = fila[0]      
        longitud = fila[1]     
        conf = fila[3]         
        zona_actual = zonaGeografica(longitud, latitud)
        if zona_actual == zona and conf >= confidence_min:
            contador += 1
    return contador

#área promedio y área total de edificaciones
def q2Area(zona: str, confidence_min: float = 0.0) -> dict:
    time.sleep(0.5)  
    total_area = 0.0
    contador = 0

    for i in range(df.height):
        fila = df.row(i)
        latitud = fila[0]
        longitud = fila[1]
        conf = fila[3]
        area = fila[2]          

        zona_actual = zonaGeografica(longitud, latitud)
        if zona_actual == zona and conf >= confidence_min:
            total_area += area
            contador += 1

    if contador == 0:
        return {"avg_area": 0.0, "total_area": 0.0, "n": 0}
    else:
        return {
            "avg_area": round(total_area / contador, 2),
            "total_area": round(total_area, 2),
            "n": contador
        }

#Densidad de edificaciones por km²
def q3_density(zona: str, confidence_min: float = 0.0) -> float:
    time.sleep(0.5)  
    count = q1Conteo(zona, confidence_min)
    area_km2 = areaZonaGeografica(zona)
    return round(count / area_km2, 2)

#comparación de densidad entre dos zonas
def q4_compare(zone_a: str, zone_b: str, confidence_min: float = 0.0) -> dict:
    time.sleep(0.5)  
    da = q3_density(zone_a, confidence_min)
    db = q3_density(zone_b, confidence_min)
    winner = zone_a if da > db else zone_b
    return {
        "zone_a": da,
        "zone_b": db,
        "winner": winner
    }

# Distribución de confianza en una zona
def q5_confidence_dist(zona: str, bins: int = 5) -> list:
    conteos = [0] * bins
    for i in range(df.height):
        fila = df.row(i)
        latitud = fila[0]
        longitud = fila[1]
        conf = fila[3]
        
        zona_actual = zonaGeografica(longitud, latitud)
        if zona_actual != zona:
            continue
        if conf < 0 or conf > 1:
            continue
        
        # Calcular el índice del intervalo (bin)
        if conf == 1.0:
            idx = bins - 1
        else:
            idx = int(conf * bins)
        conteos[idx] += 1
    
    # Construir el resultado con los límites de cada intervalo
    resultado = []
    for i in range(bins):
        min_val = i / bins
        max_val = (i + 1) / bins
        if i == bins - 1:
            max_val = 1.0
        resultado.append({
            "bucket": i,
            "min": round(min_val, 4),
            "max": round(max_val, 4),
            "count": conteos[i]
        })
    return resultado

#Aca estan las rutas para la comunicación HTTP
@app.get("/q1")
def endpoint_q1(
    #Establecemos que ambos parametros son olbigatorios en caso de no estar nos mostrara un error
    zona: str = Query(..., description="Nombre de la zona"),
    confianza_min: float = Query(..., description="Confianza mínima")
):
    return {"count": q1Conteo(zona, confianza_min)}

@app.get("/q2")
def endpoint_q2(
    #Establecemos que ambos parametros son olbigatorios en caso de no estar nos mostrara un error
    zona: str = Query(..., description="Nombre de la zona"),
    confianza_min: float = Query(..., description="Confianza mínima")
):
    return q2Area(zona, confianza_min)

@app.get("/q3")
def endpoint_q3(
    #Establecemos que ambos parametros son olbigatorios en caso de no estar nos mostrara un error
    zona: str = Query(..., description="Nombre de la zona"),
    confianza_min: float = Query(..., description="Confianza mínima")
):
    return {"density": q3_density(zona, confianza_min)}

@app.get("/q4")
def endpoint_q4(
    #Establecemos que ambos parametros son olbigatorios en caso de no estar nos mostrara un error
    zone_a: str = Query(..., description="Primera zona"),
    zone_b: str = Query(..., description="Segunda zona"),
    confianza_min: float = Query(..., description="Confianza mínima")
):
    return q4_compare(zone_a, zone_b, confianza_min)

@app.get("/q5")
def endpoint_q5(
    zona: str = Query(..., description="Nombre de la zona"),
    bins: int = Query(..., description="Número de intervalos")
):
    return q5_confidence_dist(zona, bins)

# ========== NUEVO ENDPOINT PARA GENERADOR DE TRÁFICO ==========
# Define el formato de los datos que enviará el generador de tráfico
class ConsultaTrafico(BaseModel):
    tipo: str           # "Q1", "Q2", "Q3", "Q4", "Q5"
    provincia: str
    confianza: float
    provincia2: Optional[str] = None   # solo para Q4

@app.post("/query")
async def recibir_consulta(consulta: ConsultaTrafico):
    """
    Endpoint unificado para que el generador de tráfico envíe consultas.
    Redirige a las funciones específicas según el tipo.
    """
    # Mapeo de tipos a funciones y parámetros
    tipo = consulta.tipo.upper()
    zona = consulta.provincia
    conf = consulta.confianza
    zona2 = consulta.provincia2

    if tipo == "Q1":
        resultado = {"count": q1Conteo(zona, conf)}
    elif tipo == "Q2":
        resultado = q2Area(zona, conf)
    elif tipo == "Q3":
        resultado = {"density": q3_density(zona, conf)}
    elif tipo == "Q4":
        if zona2 is None:
            return {"error": "Q4 requiere provincia2"}
        resultado = q4_compare(zona, zona2, conf)
    elif tipo == "Q5":
        # Para Q5, el generador envía 'confianza' pero la función pide bins.
        # Asumimos que 'confianza' representa el número de bins (o un valor por defecto 5)
        # Como el payload de tráfico no envía 'bins', usamos un valor fijo 5 o redondeamos.
        bins = max(2, int(conf * 10)) if conf > 0 else 5
        resultado = q5_confidence_dist(zona, bins)
    else:
        return {"error": f"Tipo desconocido: {tipo}"}

    return resultado 