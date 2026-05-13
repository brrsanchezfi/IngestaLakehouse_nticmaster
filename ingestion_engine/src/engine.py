# =============================================================================
# engine.py — Orquestador principal del motor de ingesta FarmIA
# =============================================================================
# Carga la configuración desde datasets.json, instancia los ingestores
# y expone una API unificada para ejecutar batch y/o streaming.
# =============================================================================

import json
import logging
import os

from pyspark.sql import SparkSession

from ops_logger         import OpsLogger
from batch_ingestor     import BatchIngestor
from streaming_ingestor import StreamingIngestor
from silver_ingestor    import SilverIngestor

log = logging.getLogger("ingestion_engine")


class IngestionEngine:
    """
    Motor de ingesta unificado para FarmIA.

    Uso típico desde notebook Databricks:

        engine = IngestionEngine(spark, kafka_config=KAFKA_CONFIG)

        # Batch completo
        engine.run_batch()

        # Dataset específico
        engine.run_batch("product_catalog")
        engine.run_batch("field_images")

        # Streaming completo (availableNow — procesa pendientes y para)
        engine.run_streaming()

        # Streaming continuo (no bloquea — devuelve queries activas)
        queries = engine.start_streaming()
        # ... hacer otras cosas ...
        engine.stop_streaming(queries)
    """

    def __init__(
        self,
        spark      : SparkSession,
        config_path: str  = None,
        kafka_config: dict = None,
    ):
        self._spark = spark

        # Ruta por defecto relativa al notebook
        if config_path is None:
            config_path = os.path.join(
                os.path.dirname(os.path.abspath(__file__)),
                "..", "config", "datasets.json"
            )

        self._config      = self._load_config(config_path)
        self._kafka_config = kafka_config or {}

        # Instanciar logger de operaciones
        self._ops = OpsLogger(
            spark        = spark,
            ops_config   = self._config["engine"]["ops"],
            pipeline_name= "ingestion_engine",
        )

        # Instanciar ingestores
        self._batch     = BatchIngestor(spark, self._config, self._ops)
        self._streaming = StreamingIngestor(spark, self._config, self._kafka_config, self._ops)

        # Silver — config separada
        self._silver_config = None
        self._silver        = None

    def _load_config(self, path: str) -> dict:
        with open(path, "r", encoding="utf-8") as f:
            config = json.load(f)
        total    = len(config["datasets"])
        n_batch  = sum(1 for d in config["datasets"] if d["ingest_type"] == "batch")
        n_stream = sum(1 for d in config["datasets"] if d["ingest_type"] == "streaming")
        log.info(f"Config cargada: {total} datasets ({n_batch} batch, {n_stream} streaming)")
        return config

    # ------------------------------------------------------------------
    # API Batch
    # ------------------------------------------------------------------

    def run_batch(self, dataset_name: str = None) -> list:
        """
        Ejecuta ingesta batch completa o de un dataset específico.
        Devuelve lista de datasets que fallaron.

        Parámetros:
            dataset_name : nombre del dataset. Si None, ejecuta todos.
        """
        if dataset_name:
            self._batch.run(dataset_name)
            return []
        return self._batch.run_all()

    # ------------------------------------------------------------------
    # API Streaming
    # ------------------------------------------------------------------

    def run_streaming(self, dataset_name: str = None, timeout_seconds: int = 120) -> None:
        """
        Ejecuta ingesta streaming con trigger(availableNow=True).
        Procesa todos los mensajes pendientes en Kafka y para.
        Bloquea hasta completar o hasta timeout.

        Parámetros:
            dataset_name    : nombre del dataset. Si None, ejecuta todos.
            timeout_seconds : máximo segundos de espera por query (default 120).
        """
        if dataset_name:
            self._streaming.run(dataset_name)
        else:
            queries = self._streaming.start_all()
            self._streaming.await_all(queries, timeout_seconds=timeout_seconds)

    def start_streaming(self, dataset_name: str = None) -> list:
        """
        Arranca queries streaming en modo continuo (no bloquea).
        Devuelve lista de (nombre, StreamingQuery) para monitorizar.

        Útil para dejarlo corriendo en background mientras corres batch.
        """
        if dataset_name:
            ds = next(
                (d for d in self._config["datasets"]
                 if d["name"] == dataset_name and d["ingest_type"] == "streaming"),
                None,
            )
            if ds is None:
                raise ValueError(f"Dataset streaming '{dataset_name}' no encontrado.")
            queries = self._streaming.start_all()
            return [(n, q) for n, q in queries if n == dataset_name]
        return self._streaming.start_all()

    def stop_streaming(self, queries: list) -> None:
        """Para todas las queries streaming activas."""
        for name, query in queries:
            log.info(f"Parando query: {name}")
            query.stop()
            log.info(f"[{name}] Parada ✓")

    # ------------------------------------------------------------------
    # Utilidades
    # ------------------------------------------------------------------

    def validate_bronze(self) -> None:
        """Imprime el conteo de filas de todas las tablas Bronze."""
        log.info("=== Validación Bronze ===")
        for ds in self._config["datasets"]:
            d     = ds["destination"]
            table = f"{d['catalog']}.{d['schema']}.{d['table']}"
            try:
                count = self._spark.table(table).count()
                log.info(f"  {table}: {count:,} filas")
            except Exception as e:
                log.warning(f"  {table}: no existe o error — {e}")

    # ------------------------------------------------------------------
    # API Silver
    # ------------------------------------------------------------------

    def load_silver_config(self, config_path: str = None) -> None:
        """Carga la configuración Silver desde datasets_silver.json."""
        import os
        if config_path is None:
            config_path = os.path.join(
                os.path.dirname(os.path.abspath(__file__)),
                "..", "config", "datasets_silver.json"
            )
        with open(config_path, "r", encoding="utf-8") as f:
            import json
            self._silver_config = json.load(f)

        # Reutilizar ops config del engine principal
        self._silver_config["engine"]["ops"] = self._config["engine"]["ops"]

        self._silver = SilverIngestor(self._spark, self._silver_config, self._ops)
        n = len(self._silver_config["datasets"])
        log.info(f"Config Silver cargada: {n} datasets")

    def run_silver(self, dataset_name: str = None) -> list:
        """
        Ejecuta la promoción Bronze → Silver.

        Parámetros:
            dataset_name : nombre del dataset. Si None, ejecuta todos.

        Uso:
            engine.load_silver_config()
            engine.run_silver()
            engine.run_silver("product_current")
        """
        if self._silver is None:
            raise RuntimeError("Ejecuta engine.load_silver_config() primero.")
        if dataset_name:
            self._silver.run(dataset_name)
            return []
        return self._silver.run_all()

    def validate_silver(self) -> None:
        """Imprime el conteo de filas de todas las tablas Silver."""
        if self._silver_config is None:
            raise RuntimeError("Ejecuta engine.load_silver_config() primero.")
        log.info("=== Validación Silver ===")
        for ds in self._silver_config["datasets"]:
            d     = ds["destination"]
            table = f"{d['catalog']}.{d['schema']}.{d['table']}"
            try:
                count = self._spark.table(table).count()
                log.info(f"  {table}: {count:,} filas")
            except Exception as e:
                log.warning(f"  {table}: no existe o error — {e}")

    # ------------------------------------------------------------------
    # Reset / limpieza
    # ------------------------------------------------------------------

    def reset_dataset(self, dataset_name: str) -> None:
        """
        Limpia completamente un dataset: DROP TABLE, datos físicos y checkpoints.
        Usar en desarrollo cuando hay schema mismatch o delta log huérfano.
        """
        ds = next(
            (d for d in self._config["datasets"] if d["name"] == dataset_name), None
        )
        if ds is None:
            raise ValueError(f"Dataset '{dataset_name}' no encontrado.")

        d          = ds["destination"]
        full_table = f"{d['catalog']}.{d['schema']}.{d['table']}"
        ext_path   = d.get("external_path", "")
        ingest_type = ds.get("ingest_type", "batch")
        chk_prefix  = "streaming" if ingest_type == "streaming" else "bronze"
        checkpoint  = f"{self._engine['checkpoint_root']}/{chk_prefix}/{dataset_name}"
        schema_loc  = f"{self._engine['schema_root']}/{dataset_name}"

        log.info(f"[{dataset_name}] === Reseteando dataset ===")

        # 1. DROP TABLE en Unity Catalog
        try:
            self._spark.sql(f"DROP TABLE IF EXISTS {full_table}")
            log.info(f"  DROP TABLE: {full_table}")
        except Exception as e:
            log.warning(f"  DROP TABLE falló: {e}")

        # 2. Borrar paths físicos en ADLS
        for label, path in [
            ("external_path", ext_path),
            ("checkpoint",    checkpoint),
            ("schema_loc",    schema_loc),
        ]:
            if not path:
                continue
            try:
                self._spark.sql(f"REMOVE '{path}' RECURSIVE")
            except Exception:
                pass
            try:
                dbutils = self._spark._jvm  # fallback JVM
                jfs = self._spark._jsc.hadoopConfiguration()
                fs  = self._spark._jvm.org.apache.hadoop.fs.FileSystem.get(jfs)
                fs.delete(self._spark._jvm.org.apache.hadoop.fs.Path(path), True)
                log.info(f"  Borrado [{label}]: {path}")
            except Exception as e:
                log.warning(f"  No se pudo borrar [{label}] {path}: {e}")

        log.info(f"[{dataset_name}] Reset completado.")

    def reset_all(self) -> None:
        """Reset completo de todos los datasets. Completamente destructivo."""
        log.info("=== Reset completo de todos los datasets ===")
        for ds in self._config["datasets"]:
            self.reset_dataset(ds["name"])
        log.info("=== Reset completo finalizado ===")

