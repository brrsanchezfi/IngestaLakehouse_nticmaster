# =============================================================================
# datagen/generate_images.py — Generador de imágenes sintéticas de campo
# =============================================================================
# Genera JPEGs simulando capturas de sensores de campo y los sube a ADLS.
# Nombre de archivo: img_{field_id}_{YYYYMMDD}_{seq:03d}.jpg
#
# Uso desde notebook:
#   %run ./datagen/generate_images
#   generate_field_images(dbutils)
#   generate_field_images(dbutils, date=datetime(2026, 3, 1))  # histórico
# =============================================================================

import io
import random
import logging
from datetime import datetime, timedelta

from PIL import Image, ImageDraw, ImageFont

from config import LANDING_PATHS, VOLUME, REFERENCE_DATE

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("datagen.images")

random.seed(42)

FIELD_COLORS = {
    "FIELD-NORTE": (34,  139,  34),
    "FIELD-SUR"  : (139, 115,  85),
    "FIELD-ESTE" : (85,  139,  85),
    "FIELD-OESTE": (210, 180, 140),
}
IMAGE_SIZE = (640, 480)


def _make_field_image(field_id, capture_ts, temperature_c, soil_moisture_pct, seq) -> bytes:
    base_color = FIELD_COLORS.get(field_id, (100, 150, 100))
    r, g, b    = base_color
    noise      = lambda x: max(0, min(255, x + random.randint(-15, 15)))
    bg         = (noise(r), noise(g), noise(b))

    img  = Image.new("RGB", IMAGE_SIZE, color=bg)
    draw = ImageDraw.Draw(img)
    draw.rectangle([(0, 0), (IMAGE_SIZE[0], 60)], fill=(20, 20, 20))

    label = (
        f"FarmIA · {field_id}  |  "
        f"{capture_ts.strftime('%Y-%m-%d %H:%M')}  |  "
        f"Temp: {temperature_c:.1f}°C  |  "
        f"Humedad: {soil_moisture_pct:.1f}%  |  "
        f"#{seq:03d}"
    )
    try:
        font = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf", 14)
    except Exception:
        font = ImageFont.load_default()

    draw.text((10, 20), label, fill=(255, 255, 255), font=font)

    dot_color = (255, 220, 50)
    for col in range(5):
        for row in range(4):
            cx = 80 + col * 120
            cy = 120 + row * 90
            draw.ellipse([(cx - 6, cy - 6), (cx + 6, cy + 6)], fill=dot_color)

    buffer = io.BytesIO()
    img.save(buffer, format="JPEG", quality=85)
    return buffer.getvalue()


def generate_field_images(
    dbutils,
    date           : datetime = REFERENCE_DATE,
    count_per_field: int      = VOLUME["field_images_count"],
    field_ids      : list     = VOLUME["field_ids"],
) -> None:
    """
    Genera imágenes sintéticas de campo y las sube a ADLS.

    Parámetros:
        dbutils         : objeto dbutils de Databricks
        date            : fecha de referencia para los timestamps (permite históricos)
        count_per_field : número de imágenes por campo
        field_ids       : lista de campos a generar
    """
    base_path = LANDING_PATHS["field_images"]
    total     = 0
    log.info(f"Generando imágenes sintéticas | fecha: {date.date()} → {base_path}")

    for field_id in field_ids:
        for seq in range(1, count_per_field + 1):
            capture_ts        = date + timedelta(hours=8, minutes=(seq - 1) * 15)
            temperature_c     = round(18 + random.uniform(0, 12), 1)
            soil_moisture_pct = round(20 + random.uniform(0, 60), 1)

            img_bytes = _make_field_image(field_id, capture_ts, temperature_c, soil_moisture_pct, seq)
            filename  = f"img_{field_id}_{date.strftime('%Y%m%d')}_{seq:03d}.jpg"
            adls_path = f"{base_path}/{filename}"

            dbutils.fs.put(adls_path, img_bytes.decode("latin-1"), overwrite=True)
            total += 1

    log.info(f"Imágenes generadas: {total}  ({len(field_ids)} campos × {count_per_field})")
