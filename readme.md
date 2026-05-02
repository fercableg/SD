
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

# 🧪 Tarea 2

_(Pendiente)_

---

# 🧪 Tarea 3

_(Pendiente)_

---

# 👥 Integrantes

- Fernando Cabrera  
- Cristopher Vásquez  
