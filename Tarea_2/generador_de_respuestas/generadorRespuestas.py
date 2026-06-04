import polars as pl
from fastapi import FastAPI, Body

app = FastAPI()

# ── Load CSV once ──────────────────────────────────────────
df = pl.read_csv("967_buildings.csv")

# ── Pre-compute zones ONCE at startup ─────────────────────
def zonaGeografica(latitud, longitud):
    if -33.445 <= latitud <= -33.420 and -70.640 <= longitud <= -70.600:
        return "Providencia"
    elif -33.420 <= latitud <= -33.390 and -70.600 <= longitud <= -70.550:
        return "Las Condes"
    elif -33.530 <= latitud <= -33.490 and -70.790 <= longitud <= -70.740:
        return "Maipú"
    elif -33.460 <= latitud <= -33.430 and -70.670 <= longitud <= -70.630:
        return "Santiago Centro"
    elif -33.470 <= latitud <= -33.430 and -70.810 <= longitud <= -70.760:
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

# Runs ONCE at startup
df = df.with_columns(
    pl.struct([df.columns[0], df.columns[1]])
    .map_elements(
        lambda r: zonaGeografica(r[df.columns[0]], r[df.columns[1]]),
        return_dtype=pl.String
    )
    .alias("zona")
)

print(f"Dataset listo: {df.height}, edificios pre-cargados")

# ── Queries ────────────────────────────────────────────────
def q1Conteo(zona, confidence_min):
    return df.filter(
        (pl.col("zona") == zona) &
        (pl.col(df.columns[3]) >= confidence_min)
    ).height

def q2Area(zona, confidence_min):
    filtered = df.filter(
        (pl.col("zona") == zona) &
        (pl.col(df.columns[3]) >= confidence_min)
    )
    if filtered.height == 0:
        return {"avg_area": 0.0, "total_area": 0.0, "n": 0}
    total_area = filtered[df.columns[2]].sum()
    contador = filtered.height
    return {
        "avg_area": round(total_area / contador, 2),
        "total_area": round(total_area, 2),
        "n": contador
    }

def q3_density(zona, confidence_min):
    count = q1Conteo(zona, confidence_min)
    area_km2 = areaZonaGeografica(zona)
    if area_km2 == 0:
        return 0.0
    return round(count / area_km2, 2)

def q4_compare(zone_a, zone_b, confidence_min):
    da = q3_density(zone_a, confidence_min)
    db = q3_density(zone_b, confidence_min)
    winner = zone_a if da > db else zone_b
    return {"zone_a": da, "zone_b": db, "winner": winner}

def q5_confidence_dist(zone_id, bins=5):
    conf = df.filter(pl.col("zona") == zone_id)[df.columns[3]]
    edges = [i / bins for i in range(bins + 1)]
    result = []
    for i in range(bins):
        count = conf.filter(
            (conf >= edges[i]) & (conf < edges[i + 1])
        ).len()
        result.append({
            "bucket": i,
            "min": round(edges[i], 4),
            "max": round(edges[i + 1], 4),
            "count": count
        })
    return result

# ── Endpoint ───────────────────────────────────────────────
@app.post("/query")
async def enviar_query(query: dict = Body()):
    tipo = query.get("tipo", "").upper()
    provincia = query.get("provincia")
    confianza = query.get("confianza", 0.0)

    if tipo == "Q1":
        return {"result": q1Conteo(provincia, confianza)}
    elif tipo == "Q2":
        return {"result": q2Area(provincia, confianza)}
    elif tipo == "Q3":
        return {"result": q3_density(provincia, confianza)}
    elif tipo == "Q4":
        provincia2 = query.get("provincia2")
        return {"result": q4_compare(provincia, provincia2, confianza)}
    elif tipo == "Q5":
        return {"result": q5_confidence_dist(provincia, bins=5)}

    return {"error": "Invalid Query Type"}