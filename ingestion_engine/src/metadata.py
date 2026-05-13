# =============================================================================
# metadata.py — Enriquecimiento de metadatos para capa Bronze
# =============================================================================

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def add_standard_metadata(df: DataFrame) -> DataFrame:
    """
    Metadatos estándar para formatos estructurados (parquet, json).
    Autoloader expone _metadata con info del archivo de origen.
    """
    return df.select(
        F.current_timestamp().alias("_ingested_at"),
        F.to_date(F.current_timestamp()).alias("_ingested_date"),
        F.col("_metadata.file_path").alias("_source_file"),
        F.col("_metadata.file_modification_time").alias("_source_modified_at"),
        "*",
    )


def add_image_metadata(df: DataFrame) -> DataFrame:
    """
    Metadatos para binaryFile (imágenes de campo).
    Extrae field_id, capture_date e image_seq del nombre del archivo.

    Patrón de nombre esperado: img_{field_id}_{YYYYMMDD}_{seq}.jpg
    Ejemplo              : img_FIELD-NORTE_20260401_001.jpg
    """
    pattern  = r"img_([^_]+(?:_[^_]+)*)_(\d{8})_(\d+)\.jpg"
    filename = F.regexp_extract(F.col("path"), r"([^/]+)$", 1)

    return df.select(
        F.current_timestamp().alias("_ingested_at"),
        F.to_date(F.current_timestamp()).alias("_ingested_date"),
        F.col("path").alias("_source_file"),
        F.col("modificationTime").alias("_source_modified_at"),
        F.col("length").alias("_file_size_bytes"),
        F.regexp_extract(filename, pattern, 1).alias("_field_id"),
        F.to_date(
            F.regexp_extract(filename, pattern, 2), "yyyyMMdd"
        ).alias("_capture_date"),
        F.regexp_extract(filename, pattern, 3).cast("integer").alias("_image_seq"),
        F.col("content"),
    )


def add_kafka_metadata(df: DataFrame) -> DataFrame:
    """
    Metadatos para eventos streaming desde Kafka.
    El valor del mensaje (value) llega como bytes — se castea a string
    para luego parsear el JSON con from_json.
    """
    return df.select(
        F.current_timestamp().alias("_ingested_at"),
        F.to_date(F.current_timestamp()).alias("_ingested_date"),
        F.col("topic").alias("_kafka_topic"),
        F.col("partition").alias("_kafka_partition"),
        F.col("offset").alias("_kafka_offset"),
        F.col("timestamp").alias("_kafka_ts"),
        F.col("key").cast("string").alias("_kafka_key"),
        F.col("value").cast("string").alias("_raw_value"),
    )


def get_metadata_fn(ingest_type: str, fmt: str):
    """
    Devuelve la función de metadata correcta según el tipo de ingesta y formato.
    """
    if ingest_type == "streaming":
        return add_kafka_metadata
    if fmt == "binaryFile":
        return add_image_metadata
    return add_standard_metadata
