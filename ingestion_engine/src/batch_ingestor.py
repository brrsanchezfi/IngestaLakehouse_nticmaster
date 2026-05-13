# =============================================================================
# batch_ingestor.py — Ingesta batch con Databricks Autoloader
# =============================================================================
# Escribe tablas EXTERNAS en Bronze — los datos físicos van a external_path
# (ADLS) y Unity Catalog registra la tabla apuntando a esa ubicación.
# =============================================================================

import logging
from typing import Optional

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, LongType, DoubleType,
    BooleanType, TimestampType, DateType, BinaryType,
)

from metadata import get_metadata_fn
from ops_logger import OpsLogger

log = logging.getLogger("ingestion_engine.batch")

_TYPE_MAP = {
    "string": StringType(), "integer": IntegerType(), "int": IntegerType(),
    "long": LongType(), "double": DoubleType(), "float": DoubleType(),
    "boolean": BooleanType(), "bool": BooleanType(),
    "timestamp": TimestampType(), "date": DateType(), "binary": BinaryType(),
}


def _build_schema(schema_list: list) -> Optional[StructType]:
    if not schema_list:
        return None
    return StructType([
        StructField(f["name"], _TYPE_MAP[f["type"].lower()], nullable=True)
        for f in schema_list
    ])


def _full_table_name(ds: dict) -> str:
    d = ds["destination"]
    return f"{d['catalog']}.{d['schema']}.{d['table']}"


def _external_path(ds: dict) -> str:
    return ds["destination"].get("external_path", "")


class BatchIngestor:

    def __init__(self, spark: SparkSession, config: dict, ops: OpsLogger):
        self._spark  = spark
        self._config = config
        self._ops    = ops
        self._engine = config["engine"]
        spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

    def _checkpoint_path(self, name: str) -> str:
        return f"{self._engine['checkpoint_root']}/bronze/{name}"

    def _schema_path(self, name: str) -> str:
        return f"{self._engine['schema_root']}/{name}"

    def _ensure_external_table(self, ds: dict) -> None:
        """
        Crea el schema y la tabla Delta EXTERNA con schema completo explícito.

        Con Unity Catalog + Table ACLs, mergeSchema automático está bloqueado
        en streaming. La única solución es pre-crear la tabla con todas las
        columnas (incluyendo las de metadata) antes del writeStream.

        Si la tabla ya existe, no hace nada (IF NOT EXISTS).
        """
        d              = ds["destination"]
        catalog        = d["catalog"]
        schema_name    = d["schema"]
        table          = d["table"]
        ext_path       = d.get("external_path", "")
        full_table     = f"{catalog}.{schema_name}.{table}"
        fmt            = ds.get("format", "parquet")
        partition_cols = ds.get("partition_cols", [])

        # 1. Crear schema si no existe
        self._spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema_name}")

        # 2. Construir DDL con schema completo — metadata + campos del dataset
        spark_schema = _build_schema(ds.get("schema", []))

        if fmt == "binaryFile":
            # Schema fijo para imágenes
            col_defs = [
                "_ingested_at TIMESTAMP",
                "_ingested_date DATE",
                "_source_file STRING",
                "_source_modified_at TIMESTAMP",
                "_file_size_bytes LONG",
                "_field_id STRING",
                "_capture_date DATE",
                "_image_seq INTEGER",
                "content BINARY",
            ]
        else:
            # Metadata estándar + columnas del dataset
            meta_cols = [
                "_ingested_at TIMESTAMP",
                "_ingested_date DATE",
                "_source_file STRING",
                "_source_modified_at TIMESTAMP",
            ]
            data_cols = []
            if spark_schema:
                _spark_to_sql = {
                    "StringType": "STRING", "LongType": "LONG",
                    "IntegerType": "INTEGER", "DoubleType": "DOUBLE",
                    "FloatType": "FLOAT", "BooleanType": "BOOLEAN",
                    "TimestampType": "TIMESTAMP", "DateType": "DATE",
                    "BinaryType": "BINARY",
                }
                for field in spark_schema.fields:
                    sql_type = _spark_to_sql.get(
                        type(field.dataType).__name__, "STRING"
                    )
                    data_cols.append(f"{field.name} {sql_type}")
            col_defs = meta_cols + data_cols

        cols_ddl = ",".join(col_defs)

        # Particionado
        partition_clause = ""
        if partition_cols:
            partition_clause = f"PARTITIONED BY ({', '.join(partition_cols)})"

        # 3. Crear tabla externa con schema completo
        location_clause = f"LOCATION '{ext_path}'" if ext_path else ""

        # Si existe un Delta log vacío en external_path (de un intento fallido),
        # borrarlo antes de crear — de lo contrario CREATE falla por schema mismatch
        if ext_path:
            delta_log_path = f"{ext_path}/_delta_log"
            try:
                files = self._spark.sparkContext._jvm                     .org.apache.hadoop.fs.FileSystem                     .get(self._spark._jsc.hadoopConfiguration())                     .listStatus(
                        self._spark._jvm.org.apache.hadoop.fs.Path(delta_log_path)
                    )
                # Si el delta log existe pero la tabla UC no, borramos el path
                try:
                    self._spark.sql(f"DESCRIBE TABLE {full_table}")
                    log.info(f"Tabla ya existe en UC: {full_table} — no se recrea")
                    return  # tabla ya existe y está bien — salir
                except Exception:
                    # Tabla no existe en UC pero hay delta log huérfano — borrar
                    log.warning(f"Delta log huérfano detectado en {ext_path} — limpiando")
                    self._spark.sparkContext._jvm                         .org.apache.hadoop.fs.FileSystem                         .get(self._spark._jsc.hadoopConfiguration())                         .delete(
                            self._spark._jvm.org.apache.hadoop.fs.Path(ext_path), True
                        )
            except Exception:
                pass  # delta log no existe — path limpio, continuar

        ddl = f"""
            CREATE TABLE IF NOT EXISTS {full_table} (
              {cols_ddl}
            )
            USING DELTA
            {partition_clause}
            {location_clause}
        """
        self._spark.sql(ddl)
        log.info(f"Tabla externa lista: {full_table} → {ext_path}")

    def _build_read_stream(self, ds: dict) -> DataFrame:
        fmt          = ds["format"]
        landing_path = ds["landing_path"]
        merge_schema = ds.get("merge_schema", True)
        spark_schema = _build_schema(ds.get("schema", []))

        reader = (
            self._spark.readStream
                .format("cloudFiles")
                .option("cloudFiles.format", fmt)
                .option("cloudFiles.schemaLocation", self._schema_path(ds["name"]))
                .option("cloudFiles.inferColumnTypes", "true")
                .option("mergeSchema", str(merge_schema).lower())
        )

        if spark_schema and fmt != "binaryFile":
            reader = reader.schema(spark_schema)

        if fmt == "binaryFile":
            reader = reader.option("pathGlobFilter", "*.jpg")

        return reader.load(landing_path)

    def _run_write_stream(self, ds: dict, stream_df: DataFrame) -> int:
        full_table      = _full_table_name(ds)
        ext_path        = _external_path(ds)
        checkpoint_path = self._checkpoint_path(ds["name"])
        partition_cols  = ds.get("partition_cols", [])

        writer = (
            stream_df.writeStream
                .format("delta")
                .option("checkpointLocation", checkpoint_path)
                .option("mergeSchema", "true")
                .trigger(availableNow=True)
        )

        if partition_cols:
            writer = writer.partitionBy(*partition_cols)

        # Escribir en la ruta externa y registrar en Unity Catalog
        if ext_path:
            # path= escribe físicamente en ADLS; toTable= registra en UC
            query = writer.option("path", ext_path).toTable(full_table)
        else:
            query = writer.toTable(full_table)

        query.awaitTermination()

        try:
            return self._spark.table(full_table).count()
        except Exception:
            return -1

    def run(self, dataset_name: str) -> None:
        ds = next(
            (d for d in self._config["datasets"]
             if d["name"] == dataset_name and d["ingest_type"] == "batch"),
            None,
        )
        if ds is None:
            raise ValueError(f"Dataset batch '{dataset_name}' no encontrado.")

        batch_id = self._ops.start(dataset_name)
        try:
            self._ensure_external_table(ds)

            log.info(f"[{dataset_name}] readStream ← {ds['landing_path']}")
            metadata_fn = get_metadata_fn(ds["ingest_type"], ds["format"])
            stream_df   = self._build_read_stream(ds).transform(metadata_fn)

            log.info(f"[{dataset_name}] writeStream → {_full_table_name(ds)}")
            rows = self._run_write_stream(ds, stream_df)

            self._ops.success(batch_id, dataset_name, rows, rows,
                              notes=f"table={_full_table_name(ds)}")
        except Exception as e:
            self._ops.failure(batch_id, dataset_name, e)
            raise

    def run_all(self) -> list:
        datasets = [d for d in self._config["datasets"] if d["ingest_type"] == "batch"]
        log.info(f"=== Batch ingesta — {len(datasets)} datasets ===")
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
            log.info("Completado sin errores ✓")
        return failed
