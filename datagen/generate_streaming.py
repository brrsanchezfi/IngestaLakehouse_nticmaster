# =============================================================================
# datagen/generate_streaming.py — Producer Confluent Kafka para FarmIA
# =============================================================================
# Publica eventos en:
#   · farmia.sensors    → lecturas IoT de campo
#   · farmia.app_events → eventos de app móvil
#
# IMPORTANTE — ejecutar secuencial en Databricks (no hilos daemon):
#   sp = StreamingProducer()
#   sp._produce_sensors(total=400, interval=0.2)
#   sp._produce_app_events(total=700, interval=0.2)
# =============================================================================

import json
import random
import logging
import threading
import time
from datetime import datetime, timedelta

from confluent_kafka import Producer

from config import KAFKA_CONFIG, KAFKA_TOPICS, VOLUME, REFERENCE_DATE

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("datagen.streaming")

random.seed(42)

FIELD_IDS   = VOLUME["field_ids"]
CHANNELS    = ["android", "ios", "webview"]
EVENT_TYPES = ["session_start", "view_product", "add_to_cart", "checkout", "purchase"]


def _sensor_event(seq: int, base_ts: datetime) -> dict:
    return {
        "sensor_event_id"  : f"SEN-{seq:06d}",
        "field_id"         : random.choice(FIELD_IDS),
        "event_ts"         : (base_ts + timedelta(seconds=seq * 30)).isoformat(),
        "temperature_c"    : round(20 + random.uniform(0, 10), 2),
        "soil_moisture_pct": round(35 + random.uniform(0, 25), 2),
        "ph_level"         : round(6.0 + random.uniform(0, 1), 2),
    }


def _app_event(seq: int, base_ts: datetime) -> dict:
    return {
        "event_id"  : f"APP-{seq:06d}",
        "event_ts"  : (base_ts + timedelta(seconds=seq * 15)).isoformat(),
        "session_id": f"S{(seq % 160) + 1:05d}",
        "customer_id": f"C{(seq % 100) + 1:05d}",
        "event_type": random.choice(EVENT_TYPES),
        "product_id": f"P{(seq % 120) + 1:04d}",
        "channel"   : random.choice(CHANNELS),
    }


def _delivery_report(err, msg):
    if err:
        log.error(f"Error entregando mensaje: {err}")
    else:
        log.debug(f"OK → {msg.topic()} [{msg.partition()}] offset {msg.offset()}")


class StreamingProducer:
    """
    Producer de eventos Kafka para FarmIA.

    Uso en Databricks (secuencial — no hilos daemon):
        sp = StreamingProducer()
        sp._produce_sensors(total=400, interval=0.2)
        sp._produce_app_events(total=700, interval=0.2)
    """

    def __init__(self, kafka_config: dict = KAFKA_CONFIG):
        self._producer   = Producer(kafka_config)
        self._stop_event = threading.Event()
        self._threads    = []

    def _produce_loop(self, topic, event_fn, total, interval, log_every, name, base_ts) -> None:
        log.info(f"Producer {name} → topic: {topic}  total: {total}")
        seq = 0
        for seq in range(1, total + 1):
            if self._stop_event.is_set():
                break
            event = event_fn(seq, base_ts)
            self._producer.produce(
                topic    = topic,
                key      = list(event.values())[1],
                value    = json.dumps(event),
                callback = _delivery_report,
            )
            self._producer.poll(interval)
            if seq % log_every == 0:
                log.info(f"{name}: {seq}/{total}")
                self._producer.flush(timeout=10)

        restantes = self._producer.flush(timeout=60)
        if restantes > 0:
            log.warning(f"{name}: {restantes} mensajes sin confirmar")
        else:
            log.info(f"{name}: {seq} mensajes publicados ✓")

    def _produce_sensors(
        self,
        total   : int      = VOLUME["sensor_events_total"],
        interval: float    = VOLUME["kafka_interval_sec"],
        date    : datetime = REFERENCE_DATE,
    ) -> None:
        base_ts = date + timedelta(hours=8)
        self._produce_loop(
            topic     = KAFKA_TOPICS["sensors"],
            event_fn  = _sensor_event,
            total     = total,
            interval  = interval,
            log_every = 50,
            name      = "sensors",
            base_ts   = base_ts,
        )

    def _produce_app_events(
        self,
        total   : int      = VOLUME["app_events_total"],
        interval: float    = VOLUME["kafka_interval_sec"],
        date    : datetime = REFERENCE_DATE,
    ) -> None:
        base_ts = date + timedelta(hours=9)
        self._produce_loop(
            topic     = KAFKA_TOPICS["app_events"],
            event_fn  = _app_event,
            total     = total,
            interval  = interval,
            log_every = 100,
            name      = "app_events",
            base_ts   = base_ts,
        )

    def stop_all(self) -> None:
        self._stop_event.set()
        for t in self._threads:
            t.join(timeout=10)
        self._threads.clear()
