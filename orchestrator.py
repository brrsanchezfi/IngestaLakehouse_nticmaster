# Databricks notebook source
# MAGIC %md
# MAGIC # 🌾 FarmIA — Orquestador de Ingesta
# MAGIC
# MAGIC Flujo completo: Datagen → Landing → Bronze → Silver
# MAGIC
# MAGIC **Antes de empezar:** ajusta `WORKSPACE_PATH` con la ruta real
# MAGIC donde subiste el proyecto en tu workspace de Databricks.

# COMMAND ----------

# MAGIC %md
# MAGIC ## ⚙️ 0 · Setup

# COMMAND ----------

import sys
import os
from datetime import datetime

# ── AJUSTAR ESTA RUTA ────────────────────────────────────────────────────────
WORKSPACE_PATH = "/Workspace/Users/brayangreenblue@gmail.com/farmia_project"
# ─────────────────────────────────────────────────────────────────────────────

DATAGEN_PATH = f"{WORKSPACE_PATH}/datagen"
ENGINE_PATH  = f"{WORKSPACE_PATH}/ingestion_engine/src"

# Añadir al path de Python para poder importar los módulos
for p in [DATAGEN_PATH, ENGINE_PATH]:
    if p not in sys.path:
        sys.path.insert(0, p)

print(f"Datagen path : {DATAGEN_PATH}")
print(f"Engine path  : {ENGINE_PATH}")

# COMMAND ----------

# Importar datagen
from generate_batch     import generate_batch
from generate_images    import generate_field_images
from generate_streaming import StreamingProducer

def run_datagen(
    dbutils,
    date                    = None,
    datasets                = None,
    include_cdc_incremental = False,
    include_images          = True,
    include_streaming       = True,
    streaming_total_sensors = 400,
    streaming_total_app     = 700,
    streaming_interval      = 0.2,
):
    from datetime import datetime as dt
    from config import REFERENCE_DATE
    if date is None:
        date = REFERENCE_DATE

    print(f"[Datagen] fecha={date.date()} images={include_images} streaming={include_streaming}")

    generate_batch(dbutils, date=date, include_cdc_incremental=include_cdc_incremental)

    if include_images:
        generate_field_images(dbutils, date=date)

    if include_streaming:
        sp = StreamingProducer()
        sp._produce_sensors(total=streaming_total_sensors, interval=streaming_interval, date=date)
        sp._produce_app_events(total=streaming_total_app, interval=streaming_interval, date=date)

    print(f"[Datagen] completado para {date.date()}")


def run_datagen_range(
    dbutils,
    start,
    end,
    include_streaming = False,
    **kwargs,
):
    from datetime import timedelta
    current = start
    total   = (end - start).days + 1
    day     = 0
    print(f"[Backfill] {start.date()} → {end.date()} ({total} días)")
    while current <= end:
        day += 1
        print(f"── Día {day}/{total}: {current.date()}")
        run_datagen(dbutils, date=current, include_streaming=include_streaming, **kwargs)
        current += timedelta(days=1)
    print(f"[Backfill] completado: {total} días")


print("✓ Datagen cargado")

# COMMAND ----------

# Importar y configurar motor de ingesta
from engine import IngestionEngine
import json

_BASE = f"{WORKSPACE_PATH}/ingestion_engine"

try:
    KAFKA_CONFIG = {
        "bootstrap.servers": dbutils.secrets.get("farmia", "kafka_bootstrap"),
        "security.protocol": "SASL_SSL",
        "sasl.mechanisms"  : "PLAIN",
        "sasl.username"    : dbutils.secrets.get("farmia", "kafka_key"),
        "sasl.password"    : dbutils.secrets.get("farmia", "kafka_secret"),
    }
except Exception:
    KAFKA_CONFIG = {
    "bootstrap.servers" : "pkc-56d1g.eastus.azure.confluent.cloud:9092",
    "security.protocol" : "SASL_SSL",
    "sasl.mechanisms"   : "PLAIN",
    "sasl.username"     : "A54IM3C3JYQYPY2D",
    "sasl.password"     : "cflth1D3uqSST655nZL9jio1m/4LtZ8L9sh7XU8ByJS536IRjH3BwuXBzPX5lc0A",
}

engine = IngestionEngine(
    spark        = spark,
    config_path  = f"{_BASE}/config/datasets.json",
    kafka_config = KAFKA_CONFIG,
)
engine.load_silver_config(config_path=f"{_BASE}/config/datasets_silver.json")

print("✓ Motor de ingesta cargado")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📅 1 · Parámetros de ejecución

# COMMAND ----------

# ── Fecha ────────────────────────────────────────────────────────────────────
RUN_DATE   = datetime(2026, 4, 1)   # fecha principal

RUN_MODE   = "single"               # "single"   → solo RUN_DATE
                                    # "backfill" → rango START_DATE → END_DATE
START_DATE = datetime(2026, 3, 1)   # solo si RUN_MODE = "backfill"
END_DATE   = datetime(2026, 3, 5)   # solo si RUN_MODE = "backfill"

# ── Flags ────────────────────────────────────────────────────────────────────
RUN_DATAGEN       = True    # generar datos en landing
RUN_BATCH         = True    # Landing → Bronze (Autoloader)
RUN_STREAMING     = True    # Kafka → Bronze
RUN_SILVER        = True    # Bronze → Silver

INCLUDE_IMAGES    = True    # imágenes JPEG de campo
INCLUDE_CDC_INCR  = True    # segunda oleada CDC (updates + deletes)
STREAMING_TIMEOUT = 180     # segundos máx esperando Kafka

print(f"""
╔══════════════════════════════════════════════════╗
║  Modo       : {RUN_MODE:<33}║
║  Fecha      : {str(RUN_DATE.date()):<33}║
║  Datagen    : {'✓' if RUN_DATAGEN else '✗':<33}║
║  Batch      : {'✓' if RUN_BATCH else '✗':<33}║
║  Streaming  : {'✓' if RUN_STREAMING else '✗':<33}║
║  Silver     : {'✓' if RUN_SILVER else '✗':<33}║
╚══════════════════════════════════════════════════╝
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🌱 2 · Datagen — Generar datos sintéticos en Landing

# COMMAND ----------

if RUN_DATAGEN:
    if RUN_MODE == "backfill":
        print(f"Backfill: {START_DATE.date()} → {END_DATE.date()}")
        run_datagen_range(
            dbutils,
            start                   = START_DATE,
            end                     = END_DATE,
            include_images          = INCLUDE_IMAGES,
            include_streaming       = RUN_STREAMING,
            include_cdc_incremental = INCLUDE_CDC_INCR,
        )
    else:
        run_datagen(
            dbutils,
            date                    = RUN_DATE,
            include_images          = INCLUDE_IMAGES,
            include_streaming       = False,   # Kafka se lanza por separado abajo
            include_cdc_incremental = INCLUDE_CDC_INCR,
        )
else:
    print("Datagen omitido")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📡 3 · Kafka Producer — Publicar eventos en Confluent
# MAGIC
# MAGIC Solo en modo `single` — en backfill el producer ya corre dentro del datagen.

# COMMAND ----------

if RUN_STREAMING and RUN_MODE == "single":
    print("Publicando en Kafka...")
    sp = StreamingProducer()
    sp._produce_sensors(total=400, interval=0.1, date=RUN_DATE)
    sp._produce_app_events(total=700, interval=0.1, date=RUN_DATE)
    print("✓ Eventos publicados")
elif not RUN_STREAMING:
    print("Streaming omitido (RUN_STREAMING=False)")
else:
    print("Kafka ya ejecutado en el backfill")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🥉 4 · Bronze — Landing → Bronze (Autoloader + Kafka)

# COMMAND ----------

if RUN_BATCH:
    print("── Batch ──")
    failed = engine.run_batch()
    print(f"✓ Batch completado — fallidos: {failed or 'ninguno'}")
else:
    print("Batch omitido")

# COMMAND ----------

if RUN_STREAMING:
    print("── Streaming ──")
    engine.run_streaming(timeout_seconds=STREAMING_TIMEOUT)
    print("✓ Streaming completado")
else:
    print("Streaming omitido")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🥈 5 · Silver — Bronze → Silver

# COMMAND ----------

if RUN_SILVER:
    print("── Silver ──")
    failed = engine.run_silver()
    print(f"✓ Silver completado — fallidos: {failed or 'ninguno'}")
else:
    print("Silver omitido")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ 6 · Validación

# COMMAND ----------

engine.validate_bronze()

# COMMAND ----------

engine.validate_silver()

# COMMAND ----------

spark.sql("""
    SELECT source_name, status, rows_written, ingest_ts, notes
    FROM lakehouse.ingestion.control
    ORDER BY ingest_ts DESC
    -- LIMIT 30
""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔧 Utilidades — Solo desarrollo

# COMMAND ----------

# Descomenta lo que necesites:

# Reset dataset específico
# engine.reset_dataset("product_catalog")

# Reset completo
# engine.reset_all()

# Ver tabla Bronze
# spark.table("bronze.farmia.product_catalog_raw").display()

# Ver tabla Silver
# spark.table("silver.farmia.product_current").display()

# Historial Delta
# spark.sql("DESCRIBE HISTORY bronze.farmia.orders_cdc_raw LIMIT 10").display()
