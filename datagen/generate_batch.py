# =============================================================================
# datagen/generate_batch.py — Generador de datos batch para FarmIA
# =============================================================================
# Datasets generados:
#   · product_catalog  → Parquet, full load diario
#   · inventory        → Parquet, snapshot diario por almacén
#   · orders_cdc       → JSON, CDC con op_type I/U/D
#
# Naming en landing:
#   landing/farmia/{dataset}/YYYY-MM-DD.parquet  (o .json)
#
# Uso desde notebook:
#   %run ./datagen/generate_batch
#   generate_batch(dbutils)
#   generate_batch(dbutils, date=datetime(2026, 3, 1))   # fecha histórica
#   generate_batch(dbutils, "inventory")                 # solo uno
# =============================================================================

import random
import logging
from datetime import datetime, timedelta

from pyspark.sql import Row, SparkSession
from pyspark.sql import functions as F

from config import LANDING_PATHS, VOLUME, REFERENCE_DATE

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("datagen.batch")

spark = SparkSession.getActiveSession()
random.seed(42)

CATEGORIES = ["Semillas", "Fertilizantes", "Riego", "Herramientas", "Proteccion de cultivos"]
BRANDS     = ["AgroMax", "VerdePlus", "CampoTech", "BioFarm", "TerraNova"]
UNITS      = ["kg", "l", "ud", "saco", "pack"]
CHANNELS   = ["web", "mobile_app", "marketplace"]


# =============================================================================
# Helper de escritura — un único archivo con nombre controlado
# =============================================================================

def _write_single_file(df, base_path: str, filename: str, fmt: str, dbutils) -> None:
    """
    Escribe el DataFrame como UN archivo con nombre controlado en ADLS.
    Estrategia: coalesce(1) → tmp/ → mv al nombre final → rm tmp/
    """
    tmp_path   = f"{base_path}/_tmp"
    final_path = f"{base_path}/{filename}"
    ext        = ".parquet" if fmt == "parquet" else ".json"

    df.coalesce(1).write.mode("overwrite").format(fmt).save(tmp_path)

    tmp_files  = dbutils.fs.ls(tmp_path)
    part_files = [f.path for f in tmp_files if f.name.startswith("part-") and f.name.endswith(ext)]

    if not part_files:
        raise FileNotFoundError(
            f"No se encontró part file en {tmp_path}. "
            f"Archivos: {[f.name for f in tmp_files]}"
        )

    try:
        dbutils.fs.rm(final_path, recurse=False)
    except Exception:
        pass

    dbutils.fs.mv(part_files[0], final_path)
    dbutils.fs.rm(tmp_path, recurse=True)

    log.info(f"[{fmt.upper()}] {final_path}  ({df.count()} filas)")


# =============================================================================
# Generadores por dataset
# =============================================================================

def _product_catalog(dbutils, date: datetime = REFERENCE_DATE) -> None:
    """Full load del catálogo de productos para la fecha indicada."""
    log.info(f"Generando product_catalog para {date.date()}...")
    n    = VOLUME["product_catalog_rows"]
    rows = [
        Row(
            product_id      = f"P{i:04d}",
            sku             = f"SKU-{i:05d}",
            product_name    = f"Producto agricola {i}",
            category        = random.choice(CATEGORIES),
            brand           = random.choice(BRANDS),
            unit_of_measure = random.choice(UNITS),
            price_eur       = float(round(random.uniform(3.5, 120.0), 2)),
            is_active       = True,
            effective_ts    = date,
            extract_ts      = date + timedelta(hours=2),
        )
        for i in range(1, n + 1)
    ]
    df       = spark.createDataFrame(rows)
    filename = f"{date.strftime('%Y-%m-%d')}.parquet"
    _write_single_file(df, LANDING_PATHS["product_catalog"], filename, "parquet", dbutils)


def _inventory(
    dbutils,
    date      : datetime = REFERENCE_DATE,
    days      : int      = VOLUME["inventory_days"],
    warehouses: list     = VOLUME["inventory_warehouses"],
) -> None:
    """Snapshot de inventario — un archivo por día desde `date`."""
    log.info(f"Generando inventory desde {date.date()} ({days} días)...")
    for d in range(days):
        snapshot_ts = date + timedelta(days=d, hours=6)
        extract_ts  = snapshot_ts + timedelta(minutes=15)
        rows = [
            Row(
                warehouse_id    = wh,
                sku             = f"SKU-{i:05d}",
                snapshot_ts     = snapshot_ts,
                extract_ts      = extract_ts,
                available_units = int(100 + (i % 35)),
                reserved_units  = int(10  + (i % 12)),
            )
            for wh in warehouses
            for i in range(1, 81)
        ]
        df       = spark.createDataFrame(rows)
        filename = f"{snapshot_ts.strftime('%Y-%m-%d')}.parquet"
        _write_single_file(df, LANDING_PATHS["inventory"], filename, "parquet", dbutils)


def _orders_cdc(
    dbutils,
    date    : datetime = REFERENCE_DATE,
    n_orders: int      = VOLUME["orders_rows"],
) -> None:
    """CDC de pedidos — inserts para la fecha indicada."""
    log.info(f"Generando orders_cdc (inserts) para {date.date()}...")
    rows = [
        Row(
            order_id        = f"O{i:06d}",
            customer_id     = f"C{(i % 100) + 1:05d}",
            order_status    = "CREATED",
            sales_channel   = random.choice(CHANNELS),
            order_total_eur = float(round(random.uniform(20, 450), 2)),
            item_count      = int((i % 8) + 1),
            op_type         = "I",
            op_ts           = date + timedelta(hours=9, minutes=i * 3),
            sequence_num    = 1,
            is_deleted      = False,
        )
        for i in range(1, n_orders + 1)
    ]
    df       = spark.createDataFrame(rows)
    filename = f"{date.strftime('%Y-%m-%d')}.json"
    _write_single_file(df, LANDING_PATHS["orders_cdc"], filename, "json", dbutils)


def _orders_cdc_incremental(
    dbutils,
    date    : datetime = REFERENCE_DATE,
    n_orders: int      = VOLUME["orders_rows"],
) -> None:
    """CDC de pedidos — updates y deletes para el día siguiente a `date`."""
    update_date = date + timedelta(days=1)
    log.info(f"Generando orders_cdc (updates+deletes) para {update_date.date()}...")
    rows = [
        Row(
            order_id        = f"O{i:06d}",
            customer_id     = f"C{(i % 100) + 1:05d}",
            order_status    = "CANCELLED" if (i % 9 == 0) else random.choice(["DELIVERED", "SHIPPED", "CONFIRMED"]),
            sales_channel   = random.choice(CHANNELS),
            order_total_eur = float(round(random.uniform(40, 450), 2)),
            item_count      = int((i % 8) + 1),
            op_type         = "D" if (i % 9 == 0) else "U",
            op_ts           = update_date + timedelta(hours=10, minutes=i),
            sequence_num    = 2,
            is_deleted      = (i % 9 == 0),
        )
        for i in range(1, n_orders + 1)
    ]
    df       = spark.createDataFrame(rows)
    filename = f"{update_date.strftime('%Y-%m-%d')}.json"
    _write_single_file(df, LANDING_PATHS["orders_cdc"], filename, "json", dbutils)


# =============================================================================
# Entrypoint
# =============================================================================

_GENERATORS = {
    "product_catalog": _product_catalog,
    "inventory"      : _inventory,
    "orders_cdc"     : _orders_cdc,
}


def generate_batch(
    dbutils,
    dataset                : str      = None,
    date                   : datetime = REFERENCE_DATE,
    include_cdc_incremental: bool     = False,
) -> None:
    """
    Genera datos batch en landing con naming por fecha.

    Parámetros:
        dbutils                 : objeto dbutils de Databricks
        dataset                 : dataset específico o None para todos
        date                    : fecha de referencia (permite generar históricos)
        include_cdc_incremental : si True, genera también oleada U/D de orders
    """
    targets = [dataset] if dataset else list(_GENERATORS.keys())
    log.info(f"=== Generación batch: {targets} | fecha: {date.date()} ===")

    for name in targets:
        fn = _GENERATORS.get(name)
        if fn is None:
            log.warning(f"Dataset desconocido: '{name}' — opciones: {list(_GENERATORS.keys())}")
            continue
        try:
            fn(dbutils, date=date)
        except Exception as e:
            log.error(f"[{name}] Error: {e}")
            raise

    if include_cdc_incremental and (dataset is None or dataset == "orders_cdc"):
        _orders_cdc_incremental(dbutils, date=date)

    log.info("=== Generación batch completada ===")
