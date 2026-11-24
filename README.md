# Apache Pinot — Repositorio Unificado de Experimentos (Grupo 7)

Repositorio reproducible del **Grupo 7** (*Extracción y Gestión de Datos Masivos, 2026-2*) para ejecutar **ambos experimentos desde una sola carpeta madre (`BigData_Pinot/`)** sobre un único clúster de **Apache Pinot 1.2.0 + Apache Kafka 3.8.0**, sin conflictos entre tablas ni configuraciones:

* **Experimento 1 (`security_events_exp1`, tabla `OFFLINE`):** Impacto de índices secundarios (*Forward*, *Inverted*, *Bloom Filter*) y pre-agregación (*Star-Tree*).
* **Experimento 2 (`security_events`, tabla `REALTIME`):** Rendimiento y escalabilidad en ingesta streaming desde Kafka (*Freshness Lag*, `GROUP BY` concurrente, *flush threshold* y RAM).

---

## Estructura unificada del proyecto

```text
BigData_Pinot/
├── docker-compose.yml                         # Clúster común (ZooKeeper, Kafka, Controller, Broker, Server)
├── README.md                                  # Esta guía unificada (Exp 1 + Exp 2)
├── Experimento2.md                            # Documentación detallada original del Experimento 2
├── exp1_consultas.txt                         # Set fijo de 3 consultas SQL del Experimento 1
└── workspace/
    ├── generate_dataset.py                    # [Exp 1] Generador de dataset JSONL (5M–10M eventos)
    ├── exp2_stress_producer.py                # [Exp 2] Productor Kafka (500, 2.500 y 10.000 ev/s)
    ├── exp2_monitor.py                        # [Exp 2] Monitor en vivo (Freshness, SQL, Segmentos, RAM)
    └── config/
        ├── exp1/
        │   ├── schema.json                    # Esquema de security_events_exp1
        │   ├── config_a_forward_1.json        # Config A: Solo Forward Index
        │   ├── config_b_inverted_bloom_2.json # Config B: Inverted Index + Bloom Filter (src_ip)
        │   ├── config_c_startree_3.json       # Config C: Config B + Star-Tree Index
        │   └── ingestion.yaml                 # Job batch de carga de segmentos
        └── exp2/
            ├── security_events_schema.json    # Esquema de security_events (REALTIME)
            ├── exp2_table_10k.json            # Config REALTIME (flush = 10.000 filas)
            ├── exp2_table_100k.json           # Config REALTIME (flush = 100.000 filas)
            └── exp2_table_500k.json           # Config REALTIME (flush = 500.000 filas)
```

---

## Requisitos previos

* **Docker** y **Docker Compose (V2)**
* **Python 3** con `kafka-python-ng` instalado para el Experimento 2 (si usas el entorno virtual `.venv`, actívalo antes de correr los scripts de Exp 2):
  ```bash
  source .venv/bin/activate
  # Si aún no está instalado:
  pip install kafka-python-ng
  ```
* Memoria RAM libre recomendada: **~3–4 GiB**

---

## Paso 0 (Común): Levantar el clúster una sola vez

Ejecuta **todos** los comandos de esta guía parado en la carpeta madre `BigData_Pinot/`.

Levanta primero ZooKeeper y Kafka para evitar *timeouts* de Helix, y luego el resto de Pinot:

```bash
docker compose up -d zookeeper kafka
sleep 15
docker compose up -d
```

Espera ~20–30 segundos y verifica que los 5 contenedores estén `Up` y que Controller (`9000`) y Broker (`8099`) respondan `OK`:

```bash
docker compose ps
curl -s http://localhost:9000/health
curl -s http://localhost:8099/health
```


---

# Experimento 1 — Índices Secundarios y Pre-agregación (`security_events_exp1`)

### 1.1 Generar el dataset común (5M registros / ~1,1 GiB en 20 shards)

```bash
python3 workspace/generate_dataset.py --rows 5000000 --shards 20
```

*(Opcional para 10M registros: `--rows 10000000 --shards 20`).*

### 1.2 Registrar el esquema y la tabla inicial (Config A — Solo Forward Index)

```bash
curl -X POST -H "Content-Type: application/json" -d @workspace/config/exp1/schema.json http://localhost:9000/schemas

curl -X POST -H "Content-Type: application/json" -d @workspace/config/exp1/config_a_forward_1.json http://localhost:9000/tables
```

### 1.3 Ingerir los datos en modo batch (una sola vez)

```bash
docker run --rm --network container:pinot-controller \
  -e JAVA_OPTS="-Xms512m -Xmx2g -XX:+UseG1GC" \
  -v "$PWD/workspace:/workspace" \
  apachepinot/pinot:1.2.0 \
  LaunchDataIngestionJob \
  -jobSpecFile /workspace/config/exp1/ingestion.yaml
```

Verifica en **[http://localhost:9000/#/tables](http://localhost:9000/#/tables)** → `security_events_exp1_OFFLINE` que los **20 segmentos estén `ONLINE`**.

### 1.4 Ejecutar las 3 configuraciones y consultas en la Consola Web

1. **Config A (Solo Forward Index):** anota **Reported Size** en `#/tables`, abre **[http://localhost:9000/#/query](http://localhost:9000/#/query)** y ejecuta las 3 consultas de `exp1_consultas.txt` (3–5 corridas de calentamiento + medición).
2. **Config B (Inverted Index + Bloom Filter):** en `security_events_exp1_OFFLINE` → **Edit Config** → `Ctrl+A` → pega `workspace/config/exp1/config_b_inverted_bloom_2.json` → **Save** → **Reload All Segments**. Espera `20/20` en **Reload Status**, anota el nuevo tamaño y repite las 3 consultas.
3. **Config C (+ Star-Tree Index):** repite **Edit Config** con `workspace/config/exp1/config_c_startree_3.json` → **Save** → **Reload All Segments**. Espera `20/20`, anota el tamaño y repite las 3 consultas.

> **Atajo para la demostración en vivo (0 s de espera en Reload):** deja `security_events_exp1` pre-cargada en **Config C** y usa en `http://localhost:9000/#/query`:
> * Sin Inverted Index en Q2: agrega arriba `SET skipIndexes = 'event_type=inverted&severity=inverted';`
> * Sin Star-Tree en Q3: agrega arriba `SET useStarTree = false;`

---

# Experimento 2 — Ingesta Streaming desde Kafka (`security_events`)

Se ejecuta desde la **misma carpeta `BigData_Pinot/`** usando **dos terminales abiertas** (con `.venv` activado si ahí está `kafka-python-ng`).

### 2.1 Crear el tópico Kafka y registrar el esquema `security_events`

```bash
docker exec pinot-kafka /opt/kafka/bin/kafka-topics.sh \
  --create --if-not-exists --topic network-events \
  --bootstrap-server kafka:9092 \
  --partitions 1 --replication-factor 1

curl -X POST -H "Content-Type: application/json" \
  -d @workspace/config/exp2/security_events_schema.json \
  http://localhost:9000/schemas
```

*(Los 3 archivos `exp2_table_10k.json`, `exp2_table_100k.json` y `exp2_table_500k.json` ya tienen `"auto.offset.reset": "largest"` listo para Pinot 1.2.0).*

### 2.2 Ejecutar las corridas experimentales (C1 a C5)

En cada corrida: **Terminal A** crea/resetea la tabla → **Terminal B** lanza el monitor → **Terminal A** lanza el productor de estrés.

#### C1 — Línea base (`500 ev/s` | Flush: `100.000`)
* **Terminal A (crear tabla):**
  ```bash
  curl -X POST -H "Content-Type: application/json" -d @workspace/config/exp2/exp2_table_100k.json http://localhost:9000/tables
  ```
* **Terminal B (monitor):**
  ```bash
  python3 workspace/exp2_monitor.py
  ```
* **Terminal A (productor):**
  ```bash
  python3 workspace/exp2_stress_producer.py 500 25
  ```

#### C2 — Carga media (`2.500 ev/s` | Flush: `100.000`)
* **Terminal A (resetear tabla):**
  ```bash
  curl -X DELETE "http://localhost:9000/tables/security_events?type=realtime"
  curl -X POST -H "Content-Type: application/json" -d @workspace/config/exp2/exp2_table_100k.json http://localhost:9000/tables
  ```
* **Terminal B:** `python3 workspace/exp2_monitor.py`
* **Terminal A:** `python3 workspace/exp2_stress_producer.py 2500 25`

#### C3 — Estrés estándar (`10.000 ev/s` | Flush: `100.000`)
* **Terminal A (resetear tabla):**
  ```bash
  curl -X DELETE "http://localhost:9000/tables/security_events?type=realtime"
  curl -X POST -H "Content-Type: application/json" -d @workspace/config/exp2/exp2_table_100k.json http://localhost:9000/tables
  ```
* **Terminal B:** `python3 workspace/exp2_monitor.py`
* **Terminal A:** `python3 workspace/exp2_stress_producer.py 10000 25`

#### C4 — Estrés con flush agresivo (`10.000 ev/s` | Flush: `10.000`)
* **Terminal A (resetear tabla):**
  ```bash
  curl -X DELETE "http://localhost:9000/tables/security_events?type=realtime"
  curl -X POST -H "Content-Type: application/json" -d @workspace/config/exp2/exp2_table_10k.json http://localhost:9000/tables
  ```
* **Terminal B:** `python3 workspace/exp2_monitor.py`
* **Terminal A:** `python3 workspace/exp2_stress_producer.py 10000 25`

#### C5 — Estrés con flush holgado (`10.000 ev/s` | Flush: `500.000`)
* **Terminal A (resetear tabla):**
  ```bash
  curl -X DELETE "http://localhost:9000/tables/security_events?type=realtime"
  curl -X POST -H "Content-Type: application/json" -d @workspace/config/exp2/exp2_table_500k.json http://localhost:9000/tables
  ```
* **Terminal B:** `python3 workspace/exp2_monitor.py`
* **Terminal A:** `python3 workspace/exp2_stress_producer.py 10000 30`

### 2.3 Consulta SQL en vivo (Consola Web `#/query`)

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

---

## Limpieza (al finalizar toda la demostración)

* **Borrar solo Experimento 1:**
  ```bash
  curl -s -X DELETE "http://localhost:9000/tables/security_events_exp1?type=offline"
  curl -s -X DELETE "http://localhost:9000/schemas/security_events_exp1"
  ```
* **Borrar solo Experimento 2:**
  ```bash
  curl -s -X DELETE "http://localhost:9000/tables/security_events?type=realtime"
  curl -s -X DELETE "http://localhost:9000/schemas/security_events"
  ```
* **Apagar todo el clúster:**
  ```bash
  docker compose down -v
  ```
