# =============================================================================
# silver_ingestor.py — Motor de promoción Bronze → Silver
# =============================================================================
# Implementa los siguientes patrones de carga:
#
#   full_merge        : MERGE por clave de negocio, gana el registro más reciente
#                       según watermark_col. Usado para full loads (product_catalog).
#
#   incremental_replace: MERGE por clave. Reemplaza el estado actual con el
#                       snapshot más reciente. Usado para inventory.
#
#   cdc_merge         : Aplica eventos CDC (I/U/D) sobre current state con
#                       soft delete. Usado para orders_current.
#
#   cdc_history       : Append deduplicado de todos los eventos CDC.
#                       Usado para orders_history.
#
#   append_dedup      : INSERT solo si la clave no existe. Usado para imágenes.
#
#   streaming_append  : Lee Bronze como stream, aplica filtros/dedup y escribe
#                       en Silver. Usado para sensor_reads y app_events.
#
# Incrementalidad via CDF (Change Data Feed):
#   El motor activa CDF en Bronze en la primera ejecución y guarda el último
#   commit version procesado en farmia_ops para lecturas incrementales.
# =============================================================================

import json
import logging
from datetime import datetime
from typing import Optional

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, LongType, DoubleType,
    BooleanType, TimestampType, DateType, BinaryType,
)
from delta.tables import DeltaTable

from ops_logger import OpsLogger

log = logging.getLogger("ingestion_engine.silver")

_TYPE_MAP = {
    "string": StringType(), "integer": IntegerType(), "int": IntegerType(),
    "long": LongType(), "double": DoubleType(), "float": DoubleType(),
    "boolean": BooleanType(), "bool": BooleanType(),
    "timestamp": TimestampType(), "date": DateType(), "binary": BinaryType(),
}


# =============================================================================
# Helpers
# =============================================================================

def _full_table(d: dict) -> str:
    return f"{d['catalog']}.{d['schema']}.{d['table']}"

def _src_table(ds: dict) -> str:
    return _full_table(ds["source"])

def _dst_table(ds: dict) -> str:
    return _full_table(ds["destination"])

def _build_schema(schema_list: list) -> Optional[StructType]:
    if not schema_list:
        return None
    return StructType([
        StructField(f["name"], _TYPE_MAP[f["type"].lower()], nullable=True)
        for f in schema_list
    ])

def _add_silver_metadata(df: DataFrame, created: bool = False) -> DataFrame:
    """Añade _silver_created_at y _silver_modified_at."""
    now = F.current_timestamp()
    if created:
        return df.withColumn("_silver_created_at", now) \
                 .withColumn("_silver_modified_at", now)
    return df.withColumn("_silver_modified_at", now)


# =============================================================================
# Silver Ingestor
# =============================================================================

class SilverIngestor:
    """
    Motor de promoción Bronze → Silver.

    Uso:
        ingestor = SilverIngestor(spark, config, ops)
        ingestor.run_all()
        ingestor.run("product_current")
    """

    def __init__(self, spark: SparkSession, config: dict, ops: OpsLogger):
        self._spark  = spark
        self._config = config
        self._ops    = ops
        self._engine = config["engine"]

        spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")
        spark.conf.set("spark.sql.shuffle.partitions", "8")

    # ------------------------------------------------------------------
    # Gestión de tablas externas Silver
    # ------------------------------------------------------------------

    def _ensure_silver_table(self, ds: dict) -> None:
        """Crea schema y tabla Delta externa Silver si no existe."""
        d           = ds["destination"]
        catalog     = d["catalog"]
        schema_name = d["schema"]
        full_table  = _dst_table(ds)
        ext_path    = d.get("external_path", "")
        part_cols   = ds.get("partition_cols", [])
        spark_schema = _build_schema(ds.get("schema", []))

        self._spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema_name}")

        _sql_types = {
            "StringType": "STRING", "LongType": "LONG", "IntegerType": "INTEGER",
            "DoubleType": "DOUBLE", "BooleanType": "BOOLEAN",
            "TimestampType": "TIMESTAMP", "DateType": "DATE", "BinaryType": "BINARY",
        }

        if spark_schema:
            col_defs = [
                f"{f.name} {_sql_types.get(type(f.dataType).__name__, 'STRING')}"
                for f in spark_schema.fields
            ]
            cols_ddl = ",\n  ".join(col_defs)
            part_clause = f"PARTITIONED BY ({', '.join(part_cols)})" if part_cols else ""
            loc_clause  = f"LOCATION '{ext_path}'" if ext_path else ""

            self._spark.sql(f"""
                CREATE TABLE IF NOT EXISTS {full_table} (
                  {cols_ddl}
                )
                USING DELTA
                {part_clause}
                {loc_clause}
            """)
            log.info(f"Tabla Silver lista: {full_table}")

    # ------------------------------------------------------------------
    # CDF — activar y leer incrementalmente
    # ------------------------------------------------------------------

    def _enable_cdf(self, ds: dict) -> None:
        """Activa CDF en la tabla Bronze fuente si no está habilitado."""
        src = _src_table(ds)
        try:
            props = self._spark.sql(f"DESCRIBE DETAIL {src}") \
                               .select("properties").collect()[0][0]
            if props.get("delta.enableChangeDataFeed", "false") != "true":
                self._spark.sql(
                    f"ALTER TABLE {src} "
                    f"SET TBLPROPERTIES (delta.enableChangeDataFeed = true)"
                )
                log.info(f"CDF activado en: {src}")
            else:
                log.info(f"CDF ya activo en: {src}")
        except Exception as e:
            log.warning(f"No se pudo activar CDF en {src}: {e}")

    def _get_last_version(self, ds: dict) -> Optional[int]:
        """Lee el último commit version procesado desde ops."""
        pipeline = f"silver_{ds['name']}"
        try:
            row = self._spark.sql(f"""
                SELECT MAX(CAST(notes AS LONG)) as last_version
                FROM {_full_table(self._config['engine']['ops'])}
                WHERE pipeline_name = '{pipeline}'
                AND status = 'SUCCESS'
            """).collect()[0]
            return int(row["last_version"]) if row["last_version"] else None
        except Exception:
            return None

    def _save_version(self, ds: dict, version: int, batch_id: str) -> None:
        """Guarda el commit version procesado en ops."""
        self._ops.success(
            batch_id     = batch_id,
            source_name  = ds["name"],
            rows_read    = 0,
            rows_written = 0,
            notes        = str(version),
        )

    def _read_bronze_cdf(self, ds: dict) -> DataFrame:
        """
        Lee Bronze incrementalmente via CDF desde el último version procesado.
        Si es la primera vez, lee toda la tabla completa.
        """
        src          = _src_table(ds)
        last_version = self._get_last_version(ds)

        if last_version is not None:
            log.info(f"[{ds['name']}] CDF desde version {last_version + 1}")
            return (
                self._spark.read
                    .format("delta")
                    .option("readChangeFeed", "true")
                    .option("startingVersion", last_version + 1)
                    .table(src)
                    .filter(F.col("_change_type").isin(["insert", "update_postimage"]))
            )
        else:
            log.info(f"[{ds['name']}] Primera carga — leyendo Bronze completo")
            return self._spark.table(src)

    def _get_current_version(self, ds: dict) -> int:
        """Obtiene el commit version actual de la tabla Bronze."""
        src = _src_table(ds)
        return self._spark.sql(
            f"DESCRIBE HISTORY {src} LIMIT 1"
        ).collect()[0]["version"]

    # ------------------------------------------------------------------
    # Patrones de carga
    # ------------------------------------------------------------------

    def _full_merge(self, ds: dict) -> int:
        """
        MERGE por clave de negocio — gana el registro con watermark_col más reciente.
        Patrón: product_catalog (full load diario).
        """
        src_df       = self._read_bronze_cdf(ds)
        merge_keys   = ds["merge_keys"]
        watermark    = ds.get("watermark_col")
        dst          = _dst_table(ds)

        # Deduplicar por clave + watermark — quedarse con el más reciente
        w = Window.partitionBy(*merge_keys).orderBy(F.col(watermark).desc())
        staged = (
            src_df
                .withColumn("_rn", F.row_number().over(w))
                .filter("_rn = 1")
                .drop("_rn")
                .withColumn("_silver_created_at", F.current_timestamp())
                .withColumn("_silver_modified_at", F.current_timestamp())
        )

        # Seleccionar solo columnas del schema Silver
        silver_cols = [f["name"] for f in ds["schema"]]
        staged = staged.select(*[c for c in silver_cols if c in staged.columns])

        merge_condition = " AND ".join([f"tgt.{k} = src.{k}" for k in merge_keys])

        DeltaTable.forName(self._spark, dst).alias("tgt").merge(
            staged.alias("src"), merge_condition
        ).whenMatchedUpdateAll(
            condition=f"src.{watermark} >= tgt.{watermark}"
        ).whenNotMatchedInsertAll().execute()

        return self._spark.table(dst).count()

    def _incremental_replace(self, ds: dict) -> int:
        """
        MERGE por clave — reemplaza current state con snapshot más reciente.
        Patrón: inventory (snapshot diario).
        """
        src_df     = self._read_bronze_cdf(ds)
        merge_keys = ds["merge_keys"]
        watermark  = ds.get("watermark_col")
        dst        = _dst_table(ds)

        # Quedarse con el snapshot más reciente por clave
        w = Window.partitionBy(*merge_keys).orderBy(F.col(watermark).desc())
        staged = (
            src_df
                .withColumn("_rn", F.row_number().over(w))
                .filter("_rn = 1")
                .drop("_rn")
                .withColumn("_silver_created_at", F.current_timestamp())
                .withColumn("_silver_modified_at", F.current_timestamp())
        )

        silver_cols = [f["name"] for f in ds["schema"]]
        staged = staged.select(*[c for c in silver_cols if c in staged.columns])

        merge_condition = " AND ".join([f"tgt.{k} = src.{k}" for k in merge_keys])

        DeltaTable.forName(self._spark, dst).alias("tgt").merge(
            staged.alias("src"), merge_condition
        ).whenMatchedUpdateAll() \
         .whenNotMatchedInsertAll() \
         .execute()

        return self._spark.table(dst).count()

    def _cdc_merge(self, ds: dict) -> int:
        """
        Aplica eventos CDC (I/U/D) sobre current state.
        Soft delete: marca is_deleted=True en vez de borrar físicamente.
        Patrón: orders_current.
        """
        src_df     = self._read_bronze_cdf(ds)
        merge_keys = ds["merge_keys"]
        seq_col    = ds.get("cdc_sequence_col", "sequence_num")
        op_col     = ds.get("cdc_op_col", "op_type")
        dst        = _dst_table(ds)

        # Deduplicar — quedarse con el evento de mayor sequence por order_id
        w = Window.partitionBy(*merge_keys).orderBy(F.col(seq_col).desc())
        staged = (
            src_df
                .withColumn("_rn", F.row_number().over(w))
                .filter("_rn = 1")
                .drop("_rn")
                .withColumn("_silver_created_at", F.current_timestamp())
                .withColumn("_silver_modified_at", F.current_timestamp())
        )

        silver_cols = [f["name"] for f in ds["schema"]]
        staged = staged.select(*[c for c in silver_cols if c in staged.columns])

        merge_condition = " AND ".join([f"tgt.{k} = src.{k}" for k in merge_keys])

        DeltaTable.forName(self._spark, dst).alias("tgt").merge(
            staged.alias("src"), merge_condition
        ).whenMatchedUpdateAll(
            condition=f"src.{seq_col} >= tgt.{seq_col}"
        ).whenNotMatchedInsert(
            condition=f"src.{op_col} != 'D'",
            values={f.name: f"src.{f.name}" for f in staged.schema.fields}
        ).execute()

        return self._spark.table(dst).count()

    def _cdc_history(self, ds: dict) -> int:
        """
        Append deduplicado de todos los eventos CDC.
        Inserta solo eventos (order_id + sequence_num) que no existen.
        Patrón: orders_history.
        """
        src_df     = self._read_bronze_cdf(ds)
        merge_keys = ds["merge_keys"]
        dst        = _dst_table(ds)

        staged = (
            src_df
                .withColumn("_silver_created_at", F.current_timestamp())
        )

        silver_cols = [f["name"] for f in ds["schema"]]
        staged = staged.select(*[c for c in silver_cols if c in staged.columns])

        merge_condition = " AND ".join([f"tgt.{k} = src.{k}" for k in merge_keys])

        DeltaTable.forName(self._spark, dst).alias("tgt").merge(
            staged.alias("src"), merge_condition
        ).whenNotMatchedInsertAll() \
         .execute()

        return self._spark.table(dst).count()

    def _append_dedup(self, ds: dict) -> int:
        """
        Insert solo si la clave no existe — sin updates.
        Patrón: field_images (dedup por _source_file).
        """
        src_df     = self._read_bronze_cdf(ds)
        merge_keys = ds["merge_keys"]
        dst        = _dst_table(ds)

        staged = src_df.withColumn("_silver_created_at", F.current_timestamp())

        silver_cols = [f["name"] for f in ds["schema"]]
        staged = staged.select(*[c for c in silver_cols if c in staged.columns])

        merge_condition = " AND ".join([f"tgt.{k} = src.{k}" for k in merge_keys])

        DeltaTable.forName(self._spark, dst).alias("tgt").merge(
            staged.alias("src"), merge_condition
        ).whenNotMatchedInsertAll() \
         .execute()

        return self._spark.table(dst).count()

    def _streaming_append(self, ds: dict) -> int:
        """
        Lee Bronze como stream con CDF, aplica filtros de calidad y
        escribe en Silver con deduplicación por watermark.
        Patrón: sensor_reads, app_events.
        """
        src          = _src_table(ds)
        dst          = _dst_table(ds)
        ext_path     = ds["destination"].get("external_path", "")
        merge_keys   = ds["merge_keys"]
        watermark    = ds.get("watermark_col", "event_ts")
        delay        = ds.get("watermark_delay", "10 minutes")
        filters      = ds.get("filters", [])
        checkpoint   = f"{self._engine['checkpoint_root']}/silver/{ds['name']}"
        silver_cols  = [f["name"] for f in ds["schema"]]
        part_cols    = ds.get("partition_cols", [])

        # Leer Bronze como stream
        stream_df = self._spark.readStream.table(src)

        # Aplicar filtros de calidad (rangos válidos)
        for flt in filters:
            col  = flt["col"]
            if col in stream_df.columns:
                if "min" in flt:
                    stream_df = stream_df.filter(F.col(col) >= flt["min"])
                if "max" in flt:
                    stream_df = stream_df.filter(F.col(col) <= flt["max"])

        # Parsear event_ts si viene como string
        if stream_df.schema[watermark].dataType == StringType():
            stream_df = stream_df.withColumn(
                watermark, F.to_timestamp(F.col(watermark))
            )

        # Añadir metadata Silver
        stream_df = (
            stream_df
                .withColumn("_silver_created_at", F.current_timestamp())
                .withWatermark(watermark, delay)
        )

        # Seleccionar columnas Silver disponibles
        available = stream_df.columns
        staged = stream_df.select(*[c for c in silver_cols if c in available])

        # Escribir con dropDuplicates por merge_keys dentro de la ventana watermark
        writer = (
            staged
                .dropDuplicates(merge_keys)
                .writeStream
                .format("delta")
                .option("checkpointLocation", checkpoint)
                .option("mergeSchema", "true")
                .trigger(availableNow=True)
        )

        if part_cols:
            writer = writer.partitionBy(*part_cols)

        # Primera escritura vs append
        try:
            self._spark.sql(f"DESCRIBE TABLE {dst}")
            query = writer.toTable(dst)
        except Exception:
            query = writer.option("path", ext_path).start() if ext_path else writer.start()
            query.awaitTermination()
            if ext_path:
                d = ds["destination"]
                self._spark.sql(f"CREATE TABLE IF NOT EXISTS {dst} USING DELTA LOCATION '{ext_path}'")
            return self._spark.table(dst).count()

        query.awaitTermination()
        return self._spark.table(dst).count()

    # ------------------------------------------------------------------
    # Dispatcher
    # ------------------------------------------------------------------

    _LOADERS = {
        "full_merge"         : "_full_merge",
        "incremental_replace": "_incremental_replace",
        "cdc_merge"          : "_cdc_merge",
        "cdc_history"        : "_cdc_history",
        "append_dedup"       : "_append_dedup",
        "streaming_append"   : "_streaming_append",
    }

    def run(self, dataset_name: str) -> None:
        """Ejecuta la promoción Bronze → Silver de un dataset específico."""
        ds = next(
            (d for d in self._config["datasets"] if d["name"] == dataset_name),
            None,
        )
        if ds is None:
            raise ValueError(f"Dataset Silver '{dataset_name}' no encontrado.")

        load_type = ds.get("load_type")
        loader    = self._LOADERS.get(load_type)
        if loader is None:
            raise ValueError(f"load_type desconocido: '{load_type}'")

        batch_id = self._ops.start(f"silver_{dataset_name}")
        try:
            # Activar CDF en Bronze si no está activo
            if load_type != "streaming_append":
                self._enable_cdf(ds)

            # Crear tabla Silver si no existe
            self._ensure_silver_table(ds)

            # Obtener version actual de Bronze antes de procesar
            current_version = self._get_current_version(ds)

            log.info(f"[{dataset_name}] Ejecutando {load_type} → {_dst_table(ds)}")
            rows = getattr(self, loader)(ds)

            # Guardar version procesada para próxima ejecución incremental
            self._save_version(ds, current_version, batch_id)

            log.info(f"[{dataset_name}] OK — {rows:,} filas en Silver")

        except Exception as e:
            self._ops.failure(batch_id, f"silver_{dataset_name}", e)
            raise

    def run_all(self) -> list:
        """Ejecuta la promoción de todos los datasets Silver."""
        datasets = self._config["datasets"]
        log.info(f"=== Silver ingesta — {len(datasets)} datasets ===")
        failed = []
        for ds in datasets:
            try:
                self.run(ds["name"])
            except Exception as e:
                log.error(f"[{ds['name']}] Error: {e} — continuando")
                failed.append(ds["name"])
        if failed:
            log.warning(f"Completado con errores: {failed}")
        else:
            log.info("Silver completado sin errores ✓")
        return failed
