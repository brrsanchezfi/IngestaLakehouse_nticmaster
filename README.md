# FarmIA — Motor de Ingesta y Lagos de Datos

Proyecto de data lakehouse para FarmIA en Azure Databricks.
Arquitectura medallion: Landing → Bronze → Silver → Gold.

---

## Estructura del proyecto

```
farmia_project/
│
├── datagen/                        ← Generador de datos sintéticos
│   ├── config.py                   ← Rutas ADLS, credenciales Kafka, volumen
│   ├── generate_batch.py           ← product_catalog, inventory, orders_cdc
│   ├── generate_images.py          ← Imágenes JPEG de campo (Pillow)
│   ├── generate_streaming.py       ← Producer Confluent Kafka
│   └── main.py                     ← Entrypoint con soporte de fechas históricas
│
└── ingestion_engine/               ← Motor de ingesta Landing → Bronze → Silver
    ├── main.py                     ← Entrypoint del motor
    ├── config/
    │   ├── datasets.json           ← Config datasets Bronze (batch + streaming)
    │   └── datasets_silver.json    ← Config datasets Silver
    └── src/
        ├── __init__.py
        ├── metadata.py             ← Enriquecimiento de metadatos Bronze
        ├── ops_logger.py           ← Logging en farmia_ops.ingestion.control
        ├── batch_ingestor.py       ← Autoloader Landing → Bronze
        ├── streaming_ingestor.py   ← Kafka → Bronze
        ├── silver_ingestor.py      ← Bronze → Silver (merge, CDC, streaming)
        └── engine.py               ← Orquestador unificado
```

---

## Setup en Databricks

### 1. Instalar dependencias en el cluster

```
confluent-kafka
pillow
```

### 2. Subir el proyecto

Subir la carpeta completa al workspace de Databricks o clonar desde Git.

### 3. Configurar credenciales Kafka (recomendado: Databricks Secrets)

```bash
databricks secrets create-scope --scope farmia
databricks secrets put --scope farmia --key kafka_bootstrap
databricks secrets put --scope farmia --key kafka_key
databricks secrets put --scope farmia --key kafka_secret
```

---

## Flujo completo de ejecución

### Paso 1 — Generar datos sintéticos

```python
%run ./datagen/main

# Fecha por defecto
run_datagen(dbutils)

# Fecha histórica específica
run_datagen(dbutils, date=datetime(2026, 3, 1))

# Backfill de rango de fechas
run_datagen_range(
    dbutils,
    start=datetime(2026, 3, 1),
    end=datetime(2026, 3, 5),
)

# Solo batch (sin Kafka ni imágenes)
run_datagen(dbutils, include_images=False, include_streaming=False)

# Con CDC incremental (updates + deletes sobre orders)
run_datagen(dbutils, include_cdc_incremental=True)
```

### Paso 2 — Motor de ingesta

```python
%run ./ingestion_engine/main

# Bronze completo
engine.run_batch()
engine.run_streaming(timeout_seconds=180)

# Silver completo
engine.run_silver()

# Validar
engine.validate_bronze()
engine.validate_silver()
```

### Paso 3 — Dataset específico

```python
# Bronze
engine.run_batch("product_catalog")
engine.run_batch("inventory")
engine.run_batch("orders_cdc")
engine.run_batch("field_images")
engine.run_streaming("sensors")
engine.run_streaming("app_events")

# Silver
engine.run_silver("product_current")
engine.run_silver("inventory_current")
engine.run_silver("orders_current")
engine.run_silver("orders_history")
engine.run_silver("sensor_reads")
engine.run_silver("app_events")
```

---

## Modelo semántico

### Bronze (`bronze.farmia.*`)

| Tabla | Fuente | Patrón | Formato |
|---|---|---|---|
| `product_catalog_raw` | Landing | Full load diario | Parquet |
| `inventory_snapshot_raw` | Landing | Snapshot diario | Parquet |
| `orders_cdc_raw` | Landing | CDC I/U/D | JSON |
| `field_images_raw` | Landing | Full load | JPEG (binary) |
| `sensor_events_raw` | Kafka | Streaming | JSON |
| `app_events_raw` | Kafka | Streaming | JSON |

### Silver (`silver.farmia.*`)

| Tabla | Patrón | Descripción |
|---|---|---|
| `product_current` | Full merge (SCD1) | Catálogo actual de productos |
| `inventory_current` | Incremental replace | Stock actual por almacén y SKU |
| `orders_current` | CDC merge + soft delete | Estado actual de pedidos |
| `orders_history` | CDC append deduplicado | Historial completo de eventos |
| `field_images` | Append dedup | Imágenes de campo sin duplicados |
| `sensor_reads` | Streaming + filtros | Lecturas IoT limpias |
| `app_events` | Streaming + dedup | Eventos de app deduplicados |

### Operaciones (`farmia_ops.ingestion.control`)

Registro de cada ejecución: pipeline, dataset, batch_id, filas, status, timestamp.

---

## Reset en desarrollo

```python
# Limpiar un dataset (DROP TABLE + datos físicos + checkpoints)
engine.reset_dataset("product_catalog")

# Limpiar todo
engine.reset_all()
```
