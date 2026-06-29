
# Tarea 1

## Despliegue del sistema

Primero, descarga o clona este repositorio en una carpeta accesible desde la consola:

```bash
docker-compose up --build  
```

![Inicio del sistema](https://github.com/user-attachments/assets/a153c702-dbb3-458c-ab47-f5adbca30d79)

Una vez levantado el sistema, abre una nueva terminal para ejecutar el contenedor generador de tráfico:

```bash
sudo docker start -ai generador_de_trafico
```

Si el generador **no funciona correctamente**, presiona **Enter** y ejecuta el comando nuevamente.

---

## Generación de consultas

Al ejecutar el generador de tráfico, podrás seleccionar el tipo de distribución:

1. Distribución Uniforme  
2. Distribución Zipf  

Luego, se solicitará ingresar la cantidad de consultas a enviar. Puedes ingresar el número que estimes conveniente.

![Generador](https://github.com/user-attachments/assets/2229ca26-1dbd-4d01-939f-a595e94a2451)

---

## Estadísticas en tiempo real del caché

Redis permite visualizar estadísticas en tiempo real mediante el siguiente comando:

```bash
sudo docker exec -it redis-db redis-cli --stat
```

> Esto **no reemplaza** al contenedor de métricas.

Estas estadísticas permiten observar el comportamiento del caché en tiempo real:

- **keys:** Llaves almacenadas en el caché  
- **Mem:** Memoria utilizada  
- **Clients:** Clientes conectados  
- **Blocked:** Clientes bloqueados  
- **Requests:** Solicitudes realizadas  
- **Connections:** Conexiones totales  

![Stats Redis](https://github.com/user-attachments/assets/c62d53e8-5f70-4783-82e5-cf14479de37b)

---

## Conexión a Redis (Interfaz gráfica)

1. Abre tu navegador y dirígete a:

```
http://localhost:5540
```

2. Acepta los términos y condiciones.  
3. Haz clic en **"Add Redis Database"**.  
4. Luego selecciona **"Connection Settings"**.  

Configura los siguientes parámetros:

- **Database Alias:** `redis-db`  
- **Host:** `redis-db`  

![Configuración Redis](https://github.com/user-attachments/assets/48899719-69fd-4fc6-9712-60cbaf96a612)

Una vez creada la conexión, podrás acceder a las métricas y visualizar información relevante del caché, como:

- Evento  
- Tipo de consulta  
- Llave del caché, etc.  

![Métricas](https://github.com/user-attachments/assets/883c50d9-cce0-4567-8563-5355234053ca)

---

## Contenedor de métricas

Para utilizar el sistema de métricas, primero debes crear y activar un entorno virtual:

```bash
# Crear entorno virtual
python3 -m venv venv

# Activar entorno virtual
source venv/bin/activate

# Instalar dependencias
pip install redis
pip install polars
```

Luego, ejecuta el script:

```bash
python3 metricas.py
```

![Ejecución métricas](https://github.com/user-attachments/assets/0b98ff31-7254-4b3c-a406-c52ce56a1e42)

---

Con esto, se da por finalizada la explicación del funcionamiento de la **Tarea 1**.

---

# Tarea 2

## Despliegue del sistema

Primero despliega el sistema a nivel local, desde una consola ejecuta:

```bash
git clone https://github.com/fercableg/SD.git
```

Desde la carpeta `Tarea_2`, levanta todos los servicios:

```bash
cd Tarea_2
docker-compose up --build
```

Esto iniciará automáticamente los siguientes contenedores:
- `redis-db` — Base de datos caché
- `kafka` — Broker de mensajería
- `init-kafka` — Inicialización de tópicos
- `generador_de_respuestas` — Procesador de consultas
- `cache` — Sistema de caché
- `consumidor` — Consumidor principal de Kafka
- `consumidor_reintento` — Consumidor del tópico de reintentos
- `kafka-ui` — Interfaz gráfica de Kafka

> [!TIP]
> Para verificar si la conexión con Kafka se ha ejecutado correctamente, abre dos terminales diferentes y ejecuta los siguientes comandos para observar los logs en tiempo real:
> 
> ```bash
> docker-compose logs -f consumidor
> docker-compose logs -f consumidor_reintento
> ```
---

## Generación de consultas

Una vez levantado el sistema, en una nueva terminal ejecuta el generador de tráfico:

```bash
docker-compose run --rm -e TOTAL_MENSAJES=N -e CONCURRENCIA=M generador_de_trafico python generadorTrafico.py
```
Donde "N" corresponde a la cantidad de consultas a enviar y "M" corresponde a los milisegundos en los cuales se envian estas consultas (Se recomienda colocar multiplos de 1000 para trabajar el segundos).

El generador publicará las consultas directamente en el tópico `consultas-principal` de Kafka.
A diferencia de la Tarea 1, el generador **no se comunica directamente con el caché** — Kafka
actúa como intermediario.

---

## Aumentar consumidores

Para aumentar el número de consumidores procesando consultas en paralelo, ejecuta el siguente comando:

```bash
docker-compose up --scale consumidor=K
```

Donde "K" es el numero de consumidores máximos que soporta el sistema. Por ejemplo, si colocas "K=4", habrán solo cuatro consumidores funcionando correctamente. 

---

## Simulación de falla temporal

Para simular una caída del Generador de Respuestas mientras el sistema está en funcionamiento, ejecuta:

```bash
docker stop generador_de_respuestas
```

Para volver a levantar el servicio, ejecuta:

```bash
docker start generador_de_respuestas
```

---

## Monitoreo con Kafka UI

En un navegador a conveniencia, dirígete a:

http://localhost:8080/

Desde aquí puedes visualizar en tiempo real:
- **Backlog** de mensajes por tópico.
- Mensajes en `consultas-reintento` y `consultas-dlq`.
- Consumidores activos y Throughput de mensajes por segundo.

---

## Evaluación de métricas

Crea un entorno virtual de Python3 e instala las dependencias necesarias para la ejecución correspondiente:

```bash
python3 -m venv venv
source venv/bin/activate
pip install redis polars kafka-python
```

Luego para visualizar la mayoría de las métricas:

```bash
cd metricas
python3 metricas.py
```

Donde al ejecutar el comando, se guarda un archivo .csv con todo los parametros que se muestran a continuación:

| Métrica | Descripción |
|---|---|
| Hit Rate | Porcentaje de consultas respondidas desde caché |
| Throughput | Consultas procesadas por segundo |
| Latencia p50/p95 | Percentiles de latencia en tiempo de respuesta |
| Cache Efficiency | Eficiencia comparada entre hits y misses en el caché |
| Total Evictions | Keys eliminadas por política de remoción |
| Total de Reintentos | Cantidad total de consultas derivadas al flujo de *fallback* tras un fallo inicial |
| Retry Rate | Porcentaje de consultas reenviadas a los tópicos de reintento |
| Recovery Rate | Porcentaje de consultas recuperadas exitosamente tras fallos temporales |
| DLQ Rate | Porcentaje de consultas enviadas a la Dead Letter Queue |

Para visualizar el resto de las metricas, como el Backlog y el Recovery time, en otra consola ejecuta:

```bash
cd Metricas2plano
python3 metricasKafka.py
```

Ahora ingresa tu contraseña, luego se desplegara un monitoreo dentro del sistema el cual registra como "Lag", junto con los topicos de los consumidores principa y de reintento. Para finalizar este proceso, ejecuta en tu teclado "Ctrl+C", donde se guarda un archivo .csv con todo el monitoreo realizado: 

| Métrica | Descripción |
|---|---|
| Backlog Size (Peak) | Cantidad máxima de mensajes pendientes (Lag) acumulados en la cola de Kafka durante el experimento |
| Recovery Time | Tiempo transcurrido (en segundos) necesario para procesar y vaciar por completo la cola tras un pico o falla |
---

> [!WARNING]
> Una vez hayas realizado el monitoreo correspondiente, debes eliminar este archivo, ya que si ejecutas nuevamente este comando este funcionara correctamente, el cual lo puedes hacer de la siguente forma:
>
> ```bash
> cd Metricas2plano
> rm lag_metricas.csv
> ```

Con esto, se da por finalizada la explicación del funcionamiento de la **Tarea 2**.

# Tarea 3
 
## Despliegue del sistema
 
Desde la carpeta `Tarea_3`, levanta todos los servicios con:
 
```bash
docker-compose up --build
```

Esto iniciará, además de los contenedores ya conocidos de la Tarea 2, los siguientes:
 
- `kafka-ui` — Interfaz gráfica de Kafka en `http://localhost:8080`.
- `elasticsearch` — Almacén de métricas agregadas en `http://localhost:9200`.
- `kibana` — Visualización de dashboards en `http://localhost:5601`.
- `spark` — Job de Spark Structured Streaming que consume `metrics-topic`.
---
 
## Generación de consultas y escenarios de prueba
 
La generación de tráfico funciona igual que en la Tarea 2:
 
```bash
docker-compose run --rm -e TOTAL_MENSAJES=N -e CONCURRENCIA=M generador_de_trafico python generadorTrafico.py
```

> [!TIP]
> El tópico `consultas-principal` se crea con 3 particiones. Si se escalan a más de 3 consumidores, los consumidores adicionales quedarán sin partición asignada y permanecerán inactivos.
 
---
 
## Monitoreo de servicios vía logs
 
Para verificar en tiempo real que cada servicio está procesando correctamente, abre una terminal por servicio:
 
```bash
docker logs -f cache
docker logs -f consumidor
docker logs -f consumidor_reintento
docker logs -f spark
```

---

## Pipeline de métricas: de la consulta a Kafka
 
Cada evento de métrica se publica en el tópico `metrics-topic` con el siguiente formato JSON:
 
```json
{
  "timestamp": "YYYY-MM-DDT12:00:00.000000Z",
  "query_type": "QI",
  "latency_ms": XX.X,
  "cache_hit": bool,
  "retry_count": Y,
  "status": "value",
  "zone_id": "name_comuna"
}
```
 
El funcionamiento en el tópico `metrics-topic`:
 
- Al momento de una consulta cargarse en `cache.py`, se publica un evento por cada resolución de consulta.
- En `consumer.py` se publica un evento por cada consulta procesada desde `consultas-principal`.
- Y en `consumidorSecundario.py` se publican eventos correspondiente al número de intento.
> [!WARNING]
> Como esta diseñado la arquitectura, `cache.py` y `consumer.py` pueden publicar un evento, cada uno por la misma consulta exitosa, que produce un conteo duplicado en `total_attempts`, y en el throughput agregado por Spark.

---

 
Con esto, se da por finalizada la explicación del funcionamiento de la **Tarea 3**.
 
---
 
# Integrantes
- Fernando Cabrera  
- Cristopher Vásquez  

