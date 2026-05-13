# =============================================================================
# datagen/main.py — Entrypoint del generador de datos FarmIA
# =============================================================================
# Uso desde notebook Databricks:
#
#   %run ./datagen/main
#
#   # Generar todo para la fecha por defecto (REFERENCE_DATE)
#   run_datagen(dbutils)
#
#   # Generar para una fecha histórica específica
#   run_datagen(dbutils, date=datetime(2026, 3, 1))
#
#   # Generar un rango de fechas históricas (backfill)
#   run_datagen_range(dbutils, start=datetime(2026, 3, 1), end=datetime(2026, 3, 5))
#
#   # Solo batch (sin Kafka)
#   run_datagen(dbutils, include_streaming=False)
#
#   # Solo un dataset
#   run_datagen(dbutils, datasets=["product_catalog"])
# =============================================================================

import sys
import os
import logging
from datetime import datetime, timedelta

logging.basicConfig(
    level  = logging.INFO,
    format = "%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
log = logging.getLogger("datagen.main")

# Añadir datagen al path para imports relativos
# sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from config              import REFERENCE_DATE, VOLUME
from generate_batch      import generate_batch
from generate_images     import generate_field_images
from generate_streaming  import StreamingProducer


# =============================================================================
# Entrypoint principal
# =============================================================================

def run_datagen(
    dbutils,
    date                   : datetime = REFERENCE_DATE,
    datasets               : list     = None,
    include_cdc_incremental: bool     = False,
    include_images         : bool     = True,
    include_streaming      : bool     = True,
    streaming_total_sensors: int      = VOLUME["sensor_events_total"],
    streaming_total_app    : int      = VOLUME["app_events_total"],
    streaming_interval     : float    = 0.2,
) -> None:
    """
    Ejecuta el datagen completo para una fecha dada.

    Parámetros:
        dbutils                 : objeto dbutils de Databricks (requerido)
        date                    : fecha de referencia para los datos generados
        datasets                : lista de datasets batch a generar. None = todos
                                  Opciones: ["product_catalog", "inventory", "orders_cdc"]
        include_cdc_incremental : genera segunda oleada CDC (updates + deletes)
        include_images          : genera imágenes de campo
        include_streaming       : publica eventos en Kafka (sensors + app_events)
        streaming_total_sensors : número de eventos de sensores a publicar
        streaming_total_app     : número de eventos de app a publicar
        streaming_interval      : pausa entre mensajes Kafka (segundos)
    """
    log.info(f"╔══ Datagen FarmIA ══ fecha: {date.date()} ══╗")

    # 1. Batch
    log.info("── [1/3] Generando datos batch ──")
    generate_batch(
        dbutils,
        dataset                 = None if datasets is None else None,  # se pasan uno a uno si hay lista
        date                    = date,
        include_cdc_incremental = include_cdc_incremental,
    )
    if datasets:
        for ds in datasets:
            generate_batch(dbutils, dataset=ds, date=date,
                           include_cdc_incremental=include_cdc_incremental)

    # 2. Imágenes
    if include_images:
        log.info("── [2/3] Generando imágenes de campo ──")
        generate_field_images(dbutils, date=date)
    else:
        log.info("── [2/3] Imágenes omitidas ──")

    # 3. Streaming
    if include_streaming:
        log.info("── [3/3] Publicando eventos Kafka ──")
        sp = StreamingProducer()
        sp._produce_sensors(
            total    = streaming_total_sensors,
            interval = streaming_interval,
            date     = date,
        )
        sp._produce_app_events(
            total    = streaming_total_app,
            interval = streaming_interval,
            date     = date,
        )
    else:
        log.info("── [3/3] Streaming omitido ──")

    log.info(f"╚══ Datagen completado para {date.date()} ══╝")


def run_datagen_range(
    dbutils,
    start             : datetime,
    end               : datetime,
    include_streaming : bool = False,
    **kwargs,
) -> None:
    """
    Genera datos batch para un rango de fechas (backfill histórico).
    Por defecto el streaming está desactivado en rangos para evitar
    publicar demasiados mensajes en Kafka.

    Parámetros:
        dbutils           : objeto dbutils de Databricks
        start             : fecha de inicio (inclusive)
        end               : fecha de fin (inclusive)
        include_streaming : si True, publica también en Kafka por cada fecha
        **kwargs          : parámetros adicionales pasados a run_datagen
    """
    current = start
    total   = (end - start).days + 1
    log.info(f"╔══ Backfill histórico: {start.date()} → {end.date()} ({total} días) ══╗")

    day = 0
    while current <= end:
        day += 1
        log.info(f"── Día {day}/{total}: {current.date()} ──")
        run_datagen(
            dbutils,
            date              = current,
            include_streaming = include_streaming,
            **kwargs,
        )
        current += timedelta(days=1)

    log.info(f"╚══ Backfill completado: {total} días generados ══╝")


# =============================================================================
# Menú de ayuda al hacer %run
# =============================================================================
print("""
╔══════════════════════════════════════════════════════════════╗
║             Datagen FarmIA — Listo ✓                        ║
╠══════════════════════════════════════════════════════════════╣
║  Generar todo (fecha por defecto)                           ║
║    run_datagen(dbutils)                                     ║
║                                                             ║
║  Generar para fecha histórica                               ║
║    run_datagen(dbutils, date=datetime(2026, 3, 1))          ║
║                                                             ║
║  Backfill de rango de fechas                                ║
║    run_datagen_range(                                       ║
║        dbutils,                                             ║
║        start=datetime(2026, 3, 1),                          ║
║        end=datetime(2026, 3, 5),                            ║
║    )                                                        ║
║                                                             ║
║  Solo batch (sin Kafka ni imágenes)                         ║
║    run_datagen(dbutils,                                     ║
║        include_images=False,                                ║
║        include_streaming=False)                             ║
║                                                             ║
║  Con CDC incremental (updates + deletes)                    ║
║    run_datagen(dbutils, include_cdc_incremental=True)       ║
╚══════════════════════════════════════════════════════════════╝
""")
