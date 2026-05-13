# =============================================================================
# datagen/config.py — Configuración central del datagen FarmIA
# =============================================================================

from datetime import datetime

# -----------------------------------------------------------------------------
# ADLS
# -----------------------------------------------------------------------------
STORAGE            = "lakehouse001"
LANDING_CONTAINER  = "landing"
LAKEHOUSE_CONTAINER = "lakehouse"

LANDING_ROOT = f"abfss://{LANDING_CONTAINER}@{STORAGE}.dfs.core.windows.net/farmia"
ENGINE_ROOT  = f"abfss://{LAKEHOUSE_CONTAINER}@{STORAGE}.dfs.core.windows.net/_engine"

LANDING_PATHS = {
    "product_catalog": f"{LANDING_ROOT}/product_catalog",
    "inventory"      : f"{LANDING_ROOT}/inventory",
    "orders_cdc"     : f"{LANDING_ROOT}/orders_cdc",
    "field_images"   : f"{LANDING_ROOT}/field_images",
    "sensors"        : f"{LANDING_ROOT}/sensors",    # referencia — va por Kafka
    "app_events"     : f"{LANDING_ROOT}/app_events", # referencia — va por Kafka
}

# -----------------------------------------------------------------------------
# Confluent Kafka
# -----------------------------------------------------------------------------
KAFKA_CONFIG = {
    "bootstrap.servers": "pkc-56d1g.eastus.azure.confluent.cloud:9092",
    "security.protocol": "SASL_SSL",
    "sasl.mechanisms"  : "PLAIN",
    "sasl.username"    : "A54IM3C3JYQYPY2D",
    "sasl.password"    : "cflth1D3uqSST655nZL9jio1m/4LtZ8L9sh7XU8ByJS536IRjH3BwuXBzPX5lc0A",
}

KAFKA_TOPICS = {
    "sensors"   : "farmia.sensors",
    "app_events": "farmia.app_events",
}

# -----------------------------------------------------------------------------
# Volumen
# -----------------------------------------------------------------------------
VOLUME = {
    "product_catalog_rows": 120,
    "inventory_days"      : 2,
    "inventory_warehouses": ["MAD-01", "SEV-01", "VAL-01"],
    "orders_rows"         : 180,
    "field_images_count"  : 20,
    "field_ids"           : ["FIELD-NORTE", "FIELD-SUR", "FIELD-ESTE", "FIELD-OESTE"],
    "sensor_events_total" : 400,
    "app_events_total"    : 700,
    "kafka_interval_sec"  : 0.5,
}

# Fecha de referencia por defecto
REFERENCE_DATE = datetime(2026, 4, 1)
