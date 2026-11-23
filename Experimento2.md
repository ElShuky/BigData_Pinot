# Apache Pinot — Experimento 2: Rendimiento y Escalabilidad en Ingesta Streaming (Motor REALTIME)

Repositorio reproducible del **Grupo 7** (*Extracción y Gestión de Datos Masivos, 2026-2*) para evaluar el comportamiento del motor **REALTIME** de **Apache Pinot 1.2.0** conectado a **Apache Kafka 3.8.0**, midiendo *freshness lag*, latencia de consultas analíticas `GROUP BY` concurrentes, fragmentación de segmentos y consumo de RAM al variar la tasa de eventos/s y el umbral de *flush* (`realtime.segment.flush.threshold.rows`).

---

## Estructura del repositorio

```text
BigData_Pinot/
├── docker-compose.yml                         # Clúster Pinot (ZooKeeper, Kafka, Controller, Broker, Server)
└── workspace/
    ├── exp2_stress_producer.py                # Productor directo a Kafka (500, 2.500 y 10.000 ev/s)
    ├── exp2_monitor.py                        # Monitor en vivo (Freshness Lag, SQL GROUP BY, Segmentos y RAM)
    └── config/
        └── exp2/
            ├── security_events_schema.json    # Esquema de la tabla security_events
            ├── exp2_table_10k.json            # Config REALTIME con flush.threshold.rows = 10.000
            ├── exp2_table_100k.json           # Config REALTIME con flush.threshold.rows = 100.000
            └── exp2_table_500k.json           # Config REALTIME con flush.threshold.rows = 500.000
```

---

## Requisitos previos

- **Docker** y **Docker Compose (V2)**
- **Python 3** con la librería `kafka-python-ng` (`pip install kafka-python-ng`)
- Memoria RAM libre recomendada: **~3–4 GiB** (`pinot-server` configurado con `-Xmx1g` / límite `2 GiB`)

---

## Paso a paso para reproducir el Experimento 2

Ejecuta todos los comandos desde la raíz del proyecto (`BigData_Pinot/`). Se requieren **dos terminales abiertas** en paralelo para ejecutar el productor de carga y el monitor en tiempo real.

### 1. Levantar el clúster desde limpio (ZooKeeper, Kafka y Pinot)

Levanta primero ZooKeeper y Kafka para evitar *timeouts* de inicialización en Helix, y luego inicia el resto del clúster:

```bash
docker compose down -v
docker compose up -d zookeeper kafka
sleep 15
docker compose up -d
```

Espera ~30 segundos y verifica que los 5 contenedores estén activos (`Up`) y que Controller (`9000`) y Broker (`8099`) respondan `OK`:

```bash
docker compose ps
curl -s http://localhost:9000/health
curl -s http://localhost:8099/health
```

---

### 2. Crear el tópico en Kafka y registrar el esquema en Pinot

1. **Crear el tópico `network-events` (1 partición, replicación 1):**
   ```bash
   docker exec pinot-kafka /opt/kafka/bin/kafka-topics.sh --create --topic network-events --bootstrap-server kafka:9092 --partitions 1 --replication-factor 1
   ```

2. **Registrar el esquema `security_events`:**
   ```bash
   curl -X POST -H "Content-Type: application/json" -d @workspace/config/exp2/security_events_schema.json http://localhost:9000/schemas
   ```

3. **Generar las variantes de configuración de tabla (`10k`, `100k` y `500k` filas de flush):**  
   *(Asegurando `stream.kafka.consumer.prop.auto.offset.reset: "largest"`, exigido por Pinot 1.2.0).*
   ```powershell
   $fixed = (Get-Content -Raw workspace/config/exp2/exp2_table_100k.json) -replace '"latest"', '"largest"'
   [System.IO.File]::WriteAllText("$PWD\workspace\config\exp2\exp2_table_100k.json", $fixed, (New-Object System.Text.UTF8Encoding $false))
   [System.IO.File]::WriteAllText("$PWD\workspace\config\exp2\exp2_table_10k.json", ($fixed -replace '"100000"', '"10000"'), (New-Object System.Text.UTF8Encoding $false))
   [System.IO.File]::WriteAllText("$PWD\workspace\config\exp2\exp2_table_500k.json", ($fixed -replace '"100000"', '"500000"'), (New-Object System.Text.UTF8Encoding $false))
   ```

---

### 3. Ejecutar las 5 corridas experimentales (C1 a C5)

En cada corrida se sigue exactamente el mismo protocolo en dos terminales:
- **Terminal A:** Elimina la tabla anterior (si existe) y registra la configuración JSON correspondiente.
- **Terminal B:** Inicia el monitor (`python workspace/exp2_monitor.py`), que realiza 10 muestreos cada 2 segundos ejecutando una consulta `GROUP BY` concurrente.
- **Terminal A (inmediatamente):** Lanza el productor (`python workspace/exp2_stress_producer.py <tasa_ev_s> <segundos>`).

#### Corrida C1 — Línea base (`500 ev/s` | Flush: `100.000` filas)
- **Terminal A:**
  ```bash
  curl -X POST -H "Content-Type: application/json" -d @workspace/config/exp2/exp2_table_100k.json http://localhost:9000/tables
  ```
- **Terminal B:**
  ```bash
  python workspace/exp2_monitor.py
  ```
- **Terminal A:**
  ```bash
  python workspace/exp2_stress_producer.py 500 25
  ```

#### Corrida C2 — Carga media (`2.500 ev/s` | Flush: `100.000` filas)
- **Terminal A (resetear tabla):**
  ```bash
  curl -X DELETE "http://localhost:9000/tables/security_events?type=realtime"
  curl -X POST -H "Content-Type: application/json" -d @workspace/config/exp2/exp2_table_100k.json http://localhost:9000/tables
  ```
- **Terminal B:**
  ```bash
  python workspace/exp2_monitor.py
  ```
- **Terminal A:**
  ```bash
  python workspace/exp2_stress_producer.py 2500 25
  ```

#### Corrida C3 — Estrés estándar (`10.000 ev/s` | Flush: `100.000` filas)
- **Terminal A (resetear tabla):**
  ```bash
  curl -X DELETE "http://localhost:9000/tables/security_events?type=realtime"
  curl -X POST -H "Content-Type: application/json" -d @workspace/config/exp2/exp2_table_100k.json http://localhost:9000/tables
  ```
- **Terminal B:**
  ```bash
  python workspace/exp2_monitor.py
  ```
- **Terminal A:**
  ```bash
  python workspace/exp2_stress_producer.py 10000 25
  ```
> *Resultado esperado:* al cruzar 100.000 y 200.000 filas se disparan 2 commits de segmento y la latencia SQL salta transitoriamente de ~7 ms a ~19 ms

#### Corrida C4 — Estrés con flush agresivo (`10.000 ev/s` | Flush: `10.000` filas)
- **Terminal A (resetear tabla):**
  ```bash
  curl -X DELETE "http://localhost:9000/tables/security_events?type=realtime"
  curl -X POST -H "Content-Type: application/json" -d @workspace/config/exp2/exp2_table_10k.json http://localhost:9000/tables
  ```
- **Terminal B:**
  ```bash
  python workspace/exp2_monitor.py
  ```
- **Terminal A:**
  ```bash
  python workspace/exp2_stress_producer.py 10000 25
  ```
 > *Resultado esperado:* ~1 cierre por segundo, generando 25 segmentos `[24 GOOD/ONLINE + 1 CONSUMING]` y mayor incremento de RAM).

#### Corrida C5 — Estrés con flush holgado (`10.000 ev/s` | Flush: `500.000` filas)
- **Terminal A (resetear tabla):**
  ```bash
  curl -X DELETE "http://localhost:9000/tables/security_events?type=realtime"
  curl -X POST -H "Content-Type: application/json" -d @workspace/config/exp2/exp2_table_500k.json http://localhost:9000/tables
  ```
- **Terminal B:**
  ```bash
  python workspace/exp2_monitor.py
  ```
- **Terminal A:**
  ```bash
  python workspace/exp2_stress_producer.py 10000 30
  ```
  > *Resultado esperado:* 0 cierres de segmento; las ~266.300 filas quedan en **1 único segmento `CONSUMING`** y la latencia `GROUP BY` escala linealmente de 5 ms a 12 ms.

---

### 4. Inspección visual en la Consola Web de Pinot

Mientras corre la inyección (especialmente en **C3** o **C4**), verifica los resultados en **[http://localhost:9000](http://localhost:9000)**:

- **Gestor de Tablas (`#/tables` → `security_events_REALTIME`):** Muestra el crecimiento en vivo de `Total Docs`, el tamaño compacto (`Reported Size`), el parámetro `realtime.segment.flush.threshold.rows` y la lista de segmentos cerrados en estado **`GOOD`** junto al segmento activo en memoria.
- **Consola SQL (`#/query`):** Ejecuta la consulta analítica concurrente:
  ```sql
  SELECT
    event_type,
    severity,
    count(*) AS total_eventos,
    sum(bytes_transferred) AS total_bytes,
    avg(bytes_transferred) AS promedio_bytes,
    max(timestamp_ms) AS ultimo_ts
  FROM security_events
  GROUP BY event_type, severity
  ORDER BY total_eventos DESC;
  ```
  En **`QUERY RESPONSE STATS`** revisa los valores de `timeUsedMs`, `numDocsScanned`, `numSegmentsProcessed` y **`numConsumingSegmentsQueried: 1`** (confirmando la consulta híbrida simultánea sobre segmentos en disco y en RAM).

---

### 5. Limpieza (opcional)

```bash
curl -s -X DELETE "http://localhost:9000/tables/security_events?type=realtime"
curl -s -X DELETE "http://localhost:9000/schemas/security_events"
docker compose down -v
```
