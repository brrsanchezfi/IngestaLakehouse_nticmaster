# =============================================================================
# ingestion_engine/main.py — Entrypoint del motor de ingesta FarmIA
# =============================================================================
# Uso desde notebook Databricks:
#
#   %run ./ingestion_engine/main
#
#   # ── Bronze ──────────────────────────────────────────────
#   engine.run_batch()                        # todos los datasets batch
#   engine.run_batch("product_catalog")       # solo uno
#   engine.run_streaming(timeout_seconds=180) # Kafka → Bronze
#
#   # ── Silver ──────────────────────────────────────────────
#   engine.run_silver()                       # todos los datasets Silver
#   engine.run_silver("product_current")      # solo uno
#
#   # ── Validación ──────────────────────────────────────────
#   engine.validate_bronze()
#   engine.validate_silver()
#
#   # ── Reset (desarrollo) ──────────────────────────────────
#   engine.reset_dataset("product_catalog")
#   engine.reset_all()
# =============================================================================

import sys
import os
import logging

logging.basicConfig(
    level  = logging.INFO,
    format = "%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)

# Añadir src al path para imports
# sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "src"))

from engine import IngestionEngine

# =============================================================================
# Rutas de configuración
# =============================================================================
_BASE_DIR          = os.path.dirname(os.path.abspath(__file__))
BRONZE_CONFIG_PATH = os.path.join(_BASE_DIR, "config", "datasets.json")
SILVER_CONFIG_PATH = os.path.join(_BASE_DIR, "config", "datasets_silver.json")

# =============================================================================
# Credenciales Kafka
# =============================================================================
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
        "bootstrap.servers": "pkc-56d1g.eastus.azure.confluent.cloud:9092",
        "security.protocol": "SASL_SSL",
        "sasl.mechanisms"  : "PLAIN",
        "sasl.username"    : "A54IM3C3JYQYPY2D",
        "sasl.password"    : "cflth1D3uqSST655nZL9jio1m/4LtZ8L9sh7XU8ByJS536IRjH3BwuXBzPX5lc0A",
    }

# =============================================================================
# Instanciar motor
# =============================================================================
engine = IngestionEngine(
    spark        = spark,
    config_path  = BRONZE_CONFIG_PATH,
    kafka_config = KAFKA_CONFIG,
)

engine.load_silver_config(config_path=SILVER_CONFIG_PATH)

print("""
╔══════════════════════════════════════════════════════╗
║       Motor de Ingesta FarmIA — Listo ✓             ║
╠══════════════════════════════════════════════════════╣
║  Bronze                                             ║
║    engine.run_batch()                               ║
║    engine.run_batch("product_catalog")              ║
║    engine.run_streaming(timeout_seconds=180)        ║
║                                                     ║
║  Silver                                             ║
║    engine.run_silver()                              ║
║    engine.run_silver("product_current")             ║
║                                                     ║
║  Validación                                         ║
║    engine.validate_bronze()                         ║
║    engine.validate_silver()                         ║
║                                                     ║
║  Reset (desarrollo)                                 ║
║    engine.reset_dataset("nombre")                   ║
║    engine.reset_all()                               ║
╚══════════════════════════════════════════════════════╝
""")
