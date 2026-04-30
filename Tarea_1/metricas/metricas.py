import redis
import json
import polars as pl

# Para visualizar las metricas justo cuando acabe la simulación del cache, ejecutar esto
# docker exec -it metricas python metricas.py
# De forma local, simplemente usar (en la carpeta)
# python3 metricas.py

db = redis.Redis(host='localhost', port=6379, decode_responses=True)

def procesarMetricas():
    data = db.lrange("metricas", 0, -1)

    if not data:
        print("No se encontraron datos en Redis :(.")
        return

    # Pasar de JSON a Polars
    eventosCache = []
    for item in data:
        loadData = json.loads(item)
        eventosCache.append(loadData)

    # Creacion del Data Frame en Polars
    df = pl.DataFrame(eventosCache)
    totalDF = df.height

    # Revisamos todas las columnas y contabilizamos cuantos hits and misses tenemos
    hits = df.filter(pl.col("event") == "HIT").height
    misses = df.filter(pl.col("event") == "MISS").height

    # Latencias por Percentil 50 y 95
    percentil50 = df["latency_total_ms"].quantile(0.5)
    percentil95 = df["latency_total_ms"].quantile(0.95)

    # Throughput (Cuantas consultas pasan por el cache)
    duracionTotal = df["timestamp"].max() - df["timestamp"].min()
    if duracionTotal == 0:
        throughput = 0
    else:
        throughput = totalDF / duracionTotal

    # Cache Efficiency
    tiempoCache = df.filter(pl.col("event") == "HIT")["latency_total_ms"].mean() or 0
    tiempoDB = df.filter(pl.col("event") == "MISS")["latency_db_ms"].mean() or 0

    if totalDF == 0:
        hitRate = 0
        cacheEfficiency = 0
    else:
        hitRate = hits / totalDF
        cacheEfficiency = (hits * tiempoCache - misses * tiempoDB) / totalDF

    def obtenerEvictions():
        info = db.info("stats")
        evicted = info.get("evicted_keys", 0)
        return evicted    
    
    evictions = obtenerEvictions()
    
    print("--- Metricas por CLI ---")
    print(f"Total de Consultas: {totalDF}")
    print(f"Hit Rate: {hitRate:.2%}")
    print(f"Throughput: {throughput:.2f} queries/segundos")
    print(f"Latencia en el Percentil 50: {percentil50:.2f} ms")
    print(f"Latencia en el Percentil 95: {percentil95:.2f} ms")
    print(f"Cache Efficiency: {cacheEfficiency:.2f}")
    print(f"Total Evictions: {evictions:.2f}")

    # Exportar a un CSV (pal informe xd)
    df.write_csv("metricas.csv")

procesarMetricas()