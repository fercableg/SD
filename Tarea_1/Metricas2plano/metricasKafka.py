#!/usr/bin/env python3
import subprocess
import time
import csv
from datetime import datetime

# Configuración
INTERVALO = 2          # 2 segundos entre muestras
ARCHIVO_CSV = "lag_metricas.csv"
GRUPOS = ["grupo-consumidores", "grupo-reintentos"]

def get_lag(group):
    """Obtiene el lag total de un grupo de consumidores."""

    cmd = f"sudo docker exec kafka kafka-consumer-groups --bootstrap-server localhost:9092 --group {group} --describe 2>/dev/null"
    resultado = subprocess.run(cmd, shell=True, capture_output=True, text=True)

    if resultado.returncode != 0 or not resultado.stdout.strip():
        return -1
    
    lineas = resultado.stdout.strip().split('\n')[1:]  # saltar cabecera
    total = 0

    for linea in lineas:
        total_partes = linea.split()
        if len(total_partes) >= 6:
            try:
                total += int(total_partes[5])   # la columna LAG
            except ValueError:
                pass

    return total

def guardar_csv(timestamp, lag_principal, lag_reintentos):
    """Añade una fila al archivo CSV."""

    with open(ARCHIVO_CSV, mode='a', newline='') as f:
        escritor_csv = csv.writer(f)
        escritor_csv.writerow([timestamp, lag_principal, lag_reintentos])

def iniciar_monitoreo():
    # Crear archivo CSV con cabecera si no existe

    try:
        with open(ARCHIVO_CSV, mode='x', newline='') as f:
            escritor_csv = csv.writer(f)
            escritor_csv.writerow(["timestamp", "lag_principal", "lag_reintentos"])
    except FileExistsError:
        pass  # ya existe, se agregarán filas al final

    print(f"Monitoreando lag cada {INTERVALO} segundos. Presiona Ctrl+C para detener el lag.")

    try:
        while True:
            tiempo_inicial = time.time()
            lag_principal = get_lag("grupo-consumidores")
            lag_reintentos = get_lag("grupo-reintentos")
            hora_legible = datetime.fromtimestamp(ts).strftime("%H:%M:%S")

            print(f"[{hora_legible}]  Principal: {lag_principal}  |  Reintentos: {lag_reintentos}")
            guardar_csv(tiempo_inicial, lag_principal, lag_reintentos)

            time.sleep(INTERVALO)
    except KeyboardInterrupt:
        print("\nMonitoreo detenido. Datos guardados en el archivo", ARCHIVO_CSV)

if __name__ == "__main__":
    iniciar_monitoreo()