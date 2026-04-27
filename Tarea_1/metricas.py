import redis
import json
import polars as pl

# Para visualizar las metricas justo cuando acabe la simulación del cache, ejecutar esto
# docker exec -it cache python metricas.py

# De forma local, simplemente usar (en la carpeta)
# python3 metricas.py

db = redis.Redis(host='redis-db', port=6379, decode_responses=True)

def procesarMetricas():
    
    data = db.lrange("metricas", 0, -1)

    if data is not None:
        print("No se encontraron datos en Redis :(.")
   
    else: 
        # Pasar de JSON a Polars
        eventosCache = []

        for eventosCache in data:
            loadData = json.loads(eventosCache)
            eventosCache.append(loadData)

        #Creacion del Data Frame en Polars
        df = pl.DataFrame(eventosCache)
        totalDF = df.height

        #Revisamos todas las columnes y contabilizamos cuantos hits and misses tenemos
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

        print("--- Analisis de las metricas ---")
        print(f"Total de Consultas: {totalDF}")
        print(f"Hit Rate: {hitRate:.2%}")
        print(f"Throughput: {throughput:.2f} queries/sec")
        print(f"Latencia en el Percentil 50: {percentil50:.2f} ms")
        print(f"Latencia en el Percentil 95: {percentil95:.2f} ms")
        print(f"Cache Efficiency: {cacheEfficiency:.2f}")

        # Exportar a un CSV (pal informe xd) 

        df.write_csv("metricas_experimento.csv")
        print("\nArchivo 'metricas_experimento.csv' generado exitosamente.")

