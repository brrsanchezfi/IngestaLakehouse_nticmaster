# =============================================================================
# ops_logger.py — Registro de ejecuciones en farmia_ops.ingestion_control
# =============================================================================

import uuid
import logging
import traceback
from datetime import datetime

from pyspark.sql import SparkSession

log = logging.getLogger("ingestion_engine.ops_logger")


class OpsLogger:
    """
    Registra el ciclo de vida de cada ingesta en la tabla de control operativo.
    La tabla destino se construye desde la config: catalog.schema.table
    """

    def __init__(self, spark: SparkSession, ops_config: dict, pipeline_name: str):
        self._spark         = spark
        self._pipeline_name = pipeline_name
        self._table         = (
            f"{ops_config['catalog']}.{ops_config['schema']}.{ops_config['table']}"
        )
        self._ensure_table()

    def _ensure_table(self) -> None:
        """Crea el schema y la tabla de control si no existen."""
        catalog = self._table.split(".")[0]
        schema  = self._table.split(".")[1]
        try:
            self._spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
            self._spark.sql(f"""
                CREATE TABLE IF NOT EXISTS {self._table} (
                    pipeline_name  STRING,
                    source_name    STRING,
                    batch_id       STRING,
                    ingest_ts      TIMESTAMP,
                    rows_read      BIGINT,
                    rows_written   BIGINT,
                    status         STRING,
                    notes          STRING
                ) USING DELTA
            """)
            log.info(f"Tabla ops lista: {self._table}")
        except Exception as e:
            log.warning(f"No se pudo crear tabla ops {self._table}: {e}")

    def _write(self, batch_id, source_name, status, rows_read=0, rows_written=0, notes=""):
        try:
            safe_notes = notes[:500].replace("'", "''")
            self._spark.sql(f"""
                INSERT INTO {self._table} VALUES (
                    '{self._pipeline_name}', '{source_name}', '{batch_id}',
                    current_timestamp(), {rows_read}, {rows_written},
                    '{status}', '{safe_notes}'
                )
            """)
        except Exception as e:
            log.warning(f"OpsLogger no pudo escribir en {self._table}: {e}")

    def start(self, source_name: str) -> str:
        batch_id = str(uuid.uuid4())[:8]
        log.info(f"[{source_name}] Iniciando  batch_id={batch_id}")
        self._write(batch_id, source_name, "STARTED", notes=f"Inicio: {datetime.now().isoformat()}")
        return batch_id

    def success(self, batch_id, source_name, rows_read, rows_written, notes=""):
        log.info(f"[{source_name}] OK  batch_id={batch_id}  rows_written={rows_written}")
        self._write(batch_id, source_name, "SUCCESS", rows_read, rows_written, notes)

    def failure(self, batch_id, source_name, error, rows_read=0):
        tb = traceback.format_exc()[:400]
        log.error(f"[{source_name}] FAILED  batch_id={batch_id}  error={error}")
        self._write(batch_id, source_name, "FAILED", rows_read, 0,
                    f"{type(error).__name__}: {str(error)[:200]} | {tb}")
