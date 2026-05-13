# =============================================================================
# streaming_ingestor.py — Ingesta streaming desde Confluent Kafka → Bronze
# =============================================================================

import logging
from typing import Optional

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, LongType, DoubleType,
    BooleanType, TimestampType,
)

from metadata import add_kafka_metadata
from ops_logger import OpsLogger

log = logging.getLogger("ingestion_engine.streaming")

_TYPE_MAP = {
    "string": StringType(), "integer": IntegerType(), "int": IntegerType(),
    "long": LongType(), "double": DoubleType(), "float": DoubleType(),
    "boolean": BooleanType(), "bool": BooleanType(),
    "timestamp": TimestampType(),
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


class StreamingIngestor:
    """
    Motor de ingesta streaming desde Confluent Kafka → Bronze Delta (tablas externas).
    """

    def __init__(self, spark, config, kafka_config, ops):
        self._spark        = spark
        self._config       = config
        self._kafka_config = kafka_config
        self._ops          = ops
        self._engine       = config["engine"]
        spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

    def _checkpoint_path(self, name: str) -> str:
        return f"{self._engine['checkpoint_root']}/streaming/{name}"

    def _ensure_external_table(self, ds: dict) -> None:
        """
        Para streaming: solo crear el schema en UC.
        La tabla la crea el writeStream en la primera ejecución con el
        schema real de Kafka + campos parseados. No pre-crear la tabla
        porque el schema de Kafka (key, value, topic, partition...) debe
        ser inferido por Spark, no definido manualmente.
        Si la tabla ya existe, el writeStream hace append normalmente.
        """
        d       = ds["destination"]
        catalog = d["catalog"]
        schema  = d["schema"]
        self._spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
        log.info(f"Schema listo: {catalog}.{schema}")

    def _build_read_stream(self, ds: dict) -> DataFrame:
        """Construye readStream desde Confluent Kafka."""
        topic = ds["kafka"]["topic_pattern"]
        kc    = self._kafka_config

        jaas = (
            f"kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule "
            f"required username='{kc['sasl.username']}' "
            f"password='{kc['sasl.password']}';"
        )

        return (
            self._spark.readStream
                .format("kafka")
                .option("kafka.bootstrap.servers",   kc["bootstrap.servers"])
                .option("kafka.security.protocol",   kc.get("security.protocol", "SASL_SSL"))
                .option("kafka.sasl.mechanism",      kc.get("sasl.mechanisms", "PLAIN"))
                .option("kafka.sasl.jaas.config",    jaas)
                .option("subscribe",                 topic)
                .option("startingOffsets",           "earliest")
                .option("failOnDataLoss",            "false")
                .load()
        )

    def _parse_json_value(self, df: DataFrame, ds: dict) -> DataFrame:
        """Parsea _raw_value (JSON string) y expande los campos del evento."""
        spark_schema = _build_schema(ds.get("schema", []))
        if not spark_schema:
            return df

        parsed     = df.withColumn("_event", F.from_json(F.col("_raw_value"), spark_schema))
        event_cols = [F.col(f"_event.{f['name']}") for f in ds["schema"]]

        return parsed.select(
            "_ingested_at", "_ingested_date",
            "_kafka_topic", "_kafka_partition", "_kafka_offset",
            "_kafka_ts", "_kafka_key", "_raw_value",
            *event_cols,
        )

    def _table_exists(self, full_table: str) -> bool:
        try:
            self._spark.sql(f"DESCRIBE TABLE {full_table}")
            return True
        except Exception:
            return False

    def _build_write_stream(self, ds: dict, stream_df: DataFrame):
        """
        Construye y arranca el writeStream → Delta Bronze.

        Estrategia para Unity Catalog + Table ACLs:
        - Si la tabla NO existe: escribir directo al path (Delta crea la tabla
          física) y luego registrarla en UC con CREATE TABLE ... LOCATION.
        - Si la tabla YA existe: usar toTable() normal para append.
        """
        full_table     = _full_table_name(ds)
        ext_path       = ds["destination"].get("external_path", "")
        checkpoint     = self._checkpoint_path(ds["name"])
        partition_cols = ds.get("partition_cols", [])

        writer = (
            stream_df.writeStream
                .format("delta")
                .option("checkpointLocation", checkpoint)
                .option("mergeSchema", "true")
                .trigger(availableNow=True)
        )

        if partition_cols:
            writer = writer.partitionBy(*partition_cols)

        table_exists = self._table_exists(full_table)

        if not table_exists and ext_path:
            # Primera ejecución — escribir al path, Delta crea el log físico
            log.info(f"Primera escritura → path directo: {ext_path}")
            query = writer.option("path", ext_path).start()
            query.awaitTermination()

            # Registrar tabla en Unity Catalog apuntando al path
            d = ds["destination"]
            self._spark.sql(f"CREATE SCHEMA IF NOT EXISTS {d['catalog']}.{d['schema']}")
            self._spark.sql(f"""
                CREATE TABLE IF NOT EXISTS {full_table}
                USING DELTA
                LOCATION '{ext_path}'
            """)
            log.info(f"Tabla registrada en UC: {full_table} → {ext_path}")
            return None  # ya hizo awaitTermination arriba

        # Tabla ya existe — append normal via toTable
        if ext_path:
            return writer.option("path", ext_path).toTable(full_table)
        return writer.toTable(full_table)

    def run(self, dataset_name: str) -> None:
        """Arranca ingesta streaming de un dataset y espera a que termine."""
        ds = next(
            (d for d in self._config["datasets"]
             if d["name"] == dataset_name and d["ingest_type"] == "streaming"),
            None,
        )
        if ds is None:
            raise ValueError(f"Dataset streaming '{dataset_name}' no encontrado.")

        batch_id = self._ops.start(dataset_name)
        try:
            self._ensure_external_table(ds)

            log.info(f"[{dataset_name}] readStream ← Kafka: {ds['kafka']['topic_pattern']}")
            stream_df = (
                self._build_read_stream(ds)
                    .transform(add_kafka_metadata)
                    .transform(lambda df: self._parse_json_value(df, ds))
            )

            log.info(f"[{dataset_name}] writeStream → {_full_table_name(ds)}")
            query = self._build_write_stream(ds, stream_df)
            if query is not None:  # None significa que ya hizo awaitTermination internamente
                query.awaitTermination()

            rows = self._spark.table(_full_table_name(ds)).count()
            self._ops.success(batch_id, dataset_name, rows, rows,
                              notes=f"topic={ds['kafka']['topic_pattern']}")
        except Exception as e:
            self._ops.failure(batch_id, dataset_name, e)
            raise

    def start_all(self) -> list:
        """Arranca todas las queries streaming en paralelo (no bloquea)."""
        datasets = [d for d in self._config["datasets"] if d["ingest_type"] == "streaming"]
        log.info(f"=== Streaming — arrancando {len(datasets)} queries ===")
        queries = []
        for ds in datasets:
            try:
                self._ensure_external_table(ds)
                stream_df = (
                    self._build_read_stream(ds)
                        .transform(add_kafka_metadata)
                        .transform(lambda df, d=ds: self._parse_json_value(df, d))
                )
                query = self._build_write_stream(ds, stream_df)
                if query is None:
                    log.info(f"[{ds['name']}] Primera escritura completada (sin query activa)")
                    continue
                queries.append((ds["name"], query))
                log.info(f"[{ds['name']}] Query arrancada → {_full_table_name(ds)}")
            except Exception as e:
                log.error(f"[{ds['name']}] Error al arrancar: {e}")
        return queries

    def await_all(self, queries: list, timeout_seconds: int = 120) -> None:
        """
        Espera a que todas las queries terminen.
        timeout_seconds: máximo tiempo de espera por query (default 2 min).
        Si el timeout se alcanza, para la query limpiamente.
        """
        for name, query in queries:
            log.info(f"Esperando query [{name}] — timeout={timeout_seconds}s")
            terminated = query.awaitTermination(timeout=timeout_seconds)
            if terminated:
                log.info(f"[{name}] Finalizada ✓")
            else:
                log.warning(f"[{name}] Timeout alcanzado — parando query")
                query.stop()
                log.info(f"[{name}] Parada por timeout")
