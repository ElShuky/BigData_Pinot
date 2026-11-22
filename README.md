# Apache Pinot — Experimento 1: Índices Secundarios y Pre-agregación (Star-Tree)

Repositorio reproducible del **Grupo 7** (Extracción y Gestión de Datos Masivos, 2026-2) para evaluar el impacto progresivo de **Forward Index**, **Inverted Index + Bloom Filter** y **Star-Tree Index** en **Apache Pinot 1.2.0** mediante la Consola Web del Controller.

---

## Estructura del repositorio

```text
BigData_Pinot/
├── docker-compose.yml                         # Clúster Pinot (ZooKeeper, Kafka, Controller, Broker, Server)
├── exp1_consultas.txt                         # Set fijo de 3 consultas SQL del experimento
└── workspace/
    ├── generate_dataset.py                    # Generador reproducible de eventos de ciberseguridad (JSONL)
    └── config/
        └── exp1/
            ├── schema.json                    # Esquema de la tabla security_events_exp1
            ├── config_a_forward_1.json        # Config A: Solo Forward Index (línea base)
            ├── config_b_inverted_bloom_2.json # Config B: Inverted Index + Bloom Filter (src_ip)
            ├── config_c_startree_3.json       # Config C: Config B + Star-Tree Index pre-agregado
            └── ingestion.yaml                 # Job batch de generación y carga de segmentos
```

---

## Requisitos previos

* **Docker** y **Docker Compose (V2)**
* **Python 3** (solo biblioteca estándar)
* Memoria RAM libre recomendada: **~3–4 GiB**

---

## Paso a paso para reproducir el Experimento 1

Ejecuta todos los comandos desde la raíz de este repositorio (`BigData_Pinot/`).

### 1. Levantar el clúster Apache Pinot

```bash
docker compose up -d
```

Espera ~20–30 segundos a que Helix termine de inicializar y verifica que Controller (`9000`) y Broker (`8099`) respondan `OK`:

```bash
docker compose ps
curl -s http://localhost:9000/health
curl -s http://localhost:8099/health
```

---

### 2. Generar el dataset común de ciberseguridad

Genera **5.000.000 de eventos (~1,1 GiB)** repartidos en 20 shards JSONL dentro de `workspace/rawdata/exp_dataset/` (la IP rara `172.20.45.99` se inserta únicamente en `part_10.json` para evaluar la poda real del Bloom Filter):

```bash
python3 workspace/generate_dataset.py --rows 5000000 --shards 20
```

*(Opcional: para 10M de registros usar `--rows 10000000 --shards 20`).*

---

### 3. Registrar el esquema y crear la tabla inicial (Config A)

```bash
curl -X POST -H "Content-Type: application/json" -d @workspace/config/exp1/schema.json http://localhost:9000/schemas

curl -X POST -H "Content-Type: application/json" -d @workspace/config/exp1/config_a_forward_1.json http://localhost:9000/tables
```

---

### 4. Ingerir los datos en modo batch (una sola vez)

Construye los 20 segmentos a partir del dataset y los sube a Pinot usando un contenedor efímero con `-Xmx2g`:

```bash
docker run --rm --network container:pinot-controller -e JAVA_OPTS="-Xms512m -Xmx2g -XX:+UseG1GC" -v "$PWD/workspace:/workspace" apachepinot/pinot:1.2.0 LaunchDataIngestionJob -jobSpecFile workspace/config/exp1/ingestion.yaml
```

Abre **[http://localhost:9000/#/tables](http://localhost:9000/#/tables)**, selecciona `security_events_exp1_OFFLINE` y verifica que los **20 segmentos estén en estado `ONLINE`**.

---

### 5. Evaluar Config A (Solo Forward Index)

1. En `http://localhost:9000/#/tables` → `security_events_exp1_OFFLINE`, anota el tamaño reportado (**Reported Size**).
2. Abre la Consola SQL en **[http://localhost:9000/#/query](http://localhost:9000/#/query)**.
3. Ejecuta las 3 consultas de `exp1_consultas.txt` (realiza 3–5 ejecuciones iniciales de calentamiento de caché `MMAP` y registra las mediciones posteriores):
   * **Q1 (Bloom Filter):** `WHERE src_ip = '172.20.45.99'`
   * **Q2 (Inverted Index):** `WHERE event_type = 'login_fail' AND severity = 'HIGH' GROUP BY username`
   * **Q3 (Star-Tree):** `GROUP BY event_type, severity` (`COUNT`, `SUM`, `AVG`)
4. Registra: `timeUsedMs`, `numEntriesScannedInFilter`, `numDocsScanned` y `numSegmentsPrunedByValue`.

---

### 6. Activar Config B (Inverted Index + Bloom Filter) y evaluar

1. En **[http://localhost:9000/#/tables](http://localhost:9000/#/tables)** entra en `security_events_exp1_OFFLINE` y haz clic en **Edit Config**.
2. Selecciona todo (`Ctrl + A`), reemplaza el JSON completo con el contenido de `workspace/config/exp1/config_b_inverted_bloom_2.json` y haz clic en **Save**.
3. Haz clic en **Reload All Segments** → **Yes**, y espera a que **Reload Status** muestre los 20 segmentos completados.
4. Anota el nuevo **Reported Size** y repite las 3 consultas de `exp1_consultas.txt` en **[http://localhost:9000/#/query](http://localhost:9000/#/query)**.
   * *Resultado esperado:* en **Q1** `numSegmentsPrunedByValue` sube a `19/20`; en **Q2** `numEntriesScannedInFilter` cae a `0`.

---

### 7. Activar Config C (+ Star-Tree Index) y evaluar

1. En `security_events_exp1_OFFLINE` haz clic en **Edit Config**, reemplaza todo el JSON con `workspace/config/exp1/config_c_startree_3.json` y haz clic en **Save**.
2. Haz clic en **Reload All Segments** → **Yes**, y espera a que **Reload Status** termine de construir el árbol Star-Tree en los 20 segmentos.
3. Anota el **Reported Size** final y repite las 3 consultas de `exp1_consultas.txt` en **[http://localhost:9000/#/query](http://localhost:9000/#/query)**.
   * *Resultado esperado:* en **Q3** `numDocsScanned` cae de `5.000.000` a `~120` nodos pre-agregados y `timeUsedMs` baja a pocos milisegundos.

---

### 8. Limpieza (opcional)

```bash
curl -s -X DELETE "http://localhost:9000/tables/security_events_exp1?type=offline"
curl -s -X DELETE "http://localhost:9000/schemas/security_events_exp1"
docker compose down
```
