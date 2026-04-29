import polars as pl
import time
from fastapi import FastAPI, Body

app = FastAPI()
df = pl.read_csv("967_buildings.csv")
#print("Columnas:", df.columns)
#print("Total filas:", df.height)

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
def q1Conteo(zona, confidence_min):
    contador = 0
    for i in range(df.height):
        fila = df.row(i)
        latitud = fila[0]      # latitude
        longitud = fila[1]     # longitude
        conf = fila[3]         # confidence
        zona_actual = zonaGeografica(longitud, latitud)
        if zona_actual == zona and conf >= confidence_min:
            contador += 1
    return contador

#área promedio y área total de edificaciones
def q2Area(zona, confidence_min):
    time.sleep(0.5)  # simula procesamiento
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

#Densidad de edificaciones por km
def q3_density(zona, confidence_min):
    time.sleep(0.5) 
    count = q1Conteo(zona, confidence_min)
    area_km2 = areaZonaGeografica(zona)
    return round(count / area_km2, 2)

#comparación de densidad entre dos zonas
def q4_compare(zone_a, zone_b, confidence_min):

    time.sleep(0.5)  # simular procesamiento
    da = q3_density(zone_a, confidence_min)
    db = q3_density(zone_b, confidence_min)
    winner = zone_a if da > db else zone_b
    return {
        "zone_a": da,
        "zone_b": db,
        "winner": winner
    }

#Distribución de confianza en una zona
def q5_confidence_dist(zone_id, bins: int = 5):

    scores = []
    for i in range(df.height):
        fila = df.row(i)
        latitud = fila[0]
        longitud = fila[1]
        conf = fila[3]  
        
        zona_actual = zonaGeografica(longitud, latitud)
        if zona_actual != zone_id:
            continue
        
    
    # Construir histograma manualmente
    edges = [i / bins for i in range(bins + 1)]
    counts = [0] * bins
    
    for score in scores:
        # Determinar bin (excepto el caso score == 1.0)
        for b in range(bins):
            if edges[b] <= score < edges[b+1]:
                counts[b] += 1
                break
        else:
            # Si score es exactamente 1.0, cae en el último bin
            if score == 1.0:
                counts[bins-1] += 1
    
    # 3. Construir la lista de resultados
    result = []
    for i in range(bins):
        result.append({
            "bucket": i,
            "min": round(edges[i], 4),
            "max": round(edges[i+1], 4),
            "count": counts[i]
        })
    return result

@app.post("/query")

async def enviar_query(query: dict = Body()):

    tipo = query.get("tipo", "").upper()
    provincia = query.get("provincia")
    confianza = query.get("confianza", 0.0)
    
    if tipo == "Q1":
        result = q1Conteo(provincia, confianza)
        return {"result": result}
    
    elif tipo == "Q2":
        # Call your q2Area function
        result = q2Area(provincia, confianza)
        return {"result": result}

    elif tipo == "Q3":
        result = q3_density(provincia, confianza)
        return {"result": result}

    elif tipo == "Q4":
        provincia2 = query.get("provincia2")
        result = q4_compare(provincia, provincia2, confianza)
        return {"result": result}

    elif tipo == "Q5":
        # Defaulting to 5 bins as per your logic
        result = q5_confidence_dist(provincia, bins=5)
        return {"result": result}

    return {"error": "Invalid Query Type"}

#print("Conteo en Pudahuel (confianza >= 0.5):", q1Conteo("Pudahuel", 0.5))
#print("Áreas en Pudahuel (confianza >= 0.5):", q2Area("", 0.5))
#print("Densidad en Pudahuel:", q3_density("Providencia", 0.5))
#dist = q5_confidence_dist("Pudahuel", bins=5)
#for d in dist:
#    print(f"Bucket {d['bucket']}: {d['min']}-{d['max']} -> {d['count']} edificios")