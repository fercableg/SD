import polars as pl
import time
from fastapi import FastAPI, Body

app = FastAPI()
df = pl.read_csv("967_buildings.csv")

def zonaGeografica(longitud, latitud):
    if latitud >= -33.445 and latitud <= -33.420 and longitud >= -70.640 and longitud <= -70.600:
        return "Providencia"
    elif latitud >= -33.420 and latitud <= -33.390 and longitud >= -70.600 and longitud <= -70.550:
        return "Las Condes"
    elif latitud >= -33.530 and latitud <= -33.490 and longitud >= -70.790 and longitud <= -70.740:
        return "Maipú"
    elif latitud >= -33.460 and latitud <= -33.430 and longitud >= -70.670 and longitud <= -70.630:
        return "Santiago Centro"
    elif latitud >= -33.470 and latitud <= -33.430 and longitud >= -70.810 and longitud <= -70.760:
        return "Pudahuel"
    else:
        return "Desconocida"
    
def areaZonaGeografica(zona):
    if zona == "Providencia":
        return 10.28
    elif zona == "Las Condes":
        return 15.43
    elif zona == "Maipú":
        return 20.54
    elif zona == "Santiago Centro":
        return 12.33
    elif zona == "Pudahuel":
        return 20.55
    else:
        return 0   

def q1Conteo(zona, confidence_min):
    lat = df[:, 0]
    lon = df[:, 1]
    conf = df[:, 3]

    zonas = [zonaGeografica(lon[i], lat[i]) for i in range(df.height)]
    zonas_series = pl.Series(zonas)

    mask = (zonas_series == zona) & (conf >= confidence_min)
    return mask.sum()

def q2Area(zona, confidence_min):
    time.sleep(0.5)

    lat = df[:, 0]
    lon = df[:, 1]
    conf = df[:, 3]
    area = df[:, 2]

    zonas = [zonaGeografica(lon[i], lat[i]) for i in range(df.height)]
    zonas_series = pl.Series(zonas)

    mask = (zonas_series == zona) & (conf >= confidence_min)

    filtered_area = area.filter(mask)

    contador = filtered_area.len()
    total_area = filtered_area.sum()

    if contador == 0:
        return {"avg_area": 0.0, "total_area": 0.0, "n": 0}
    else:
        return {
            "avg_area": round(total_area / contador, 2),
            "total_area": round(total_area, 2),
            "n": contador
        }

def q3_density(zona, confidence_min):
    time.sleep(0.5)
    count = q1Conteo(zona, confidence_min)
    area_km2 = areaZonaGeografica(zona)
    return round(count / area_km2, 2)

def q4_compare(zone_a, zone_b, confidence_min):
    time.sleep(0.5)
    da = q3_density(zone_a, confidence_min)
    db = q3_density(zone_b, confidence_min)
    winner = zone_a if da > db else zone_b
    return {
        "zone_a": da,
        "zone_b": db,
        "winner": winner
    }

def q5_confidence_dist(zone_id, bins: int = 5):

    lat = df[:, 0]
    lon = df[:, 1]
    conf = df[:, 3]

    zonas = [zonaGeografica(lon[i], lat[i]) for i in range(df.height)]
    zonas_series = pl.Series(zonas)

    scores = conf.filter(zonas_series == zone_id).to_list()

    edges = [i / bins for i in range(bins + 1)]
    counts = [0] * bins

    for score in scores:
        for b in range(bins):
            if edges[b] <= score < edges[b+1]:
                counts[b] += 1
                break
        else:
            if score == 1.0:
                counts[bins-1] += 1

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
        result = q5_confidence_dist(provincia, bins=5)
        return {"result": result}

    return {"error": "Invalid Query Type"}