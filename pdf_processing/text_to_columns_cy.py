import os
import re
import logging
import argparse
from abc import ABC, abstractmethod
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.types import MapType, StringType

# ============================================================
# 🪵 Configuración de Logger
# ============================================================
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("CY_Extractor_Job")

EXTRACTION_FIELDS = [
    "broker_name", "broker_phone", "broker_fax", "broker_address",
    "broker_city", "broker_state", "broker_zipcode", "broker_email",
    "loadConfirmationNumber", "totalCarrierPay",
    "carrier_name", "carrier_mc", "carrier_address", "carrier_city",
    "carrier_state", "carrier_zipcode", "carrier_phone",
    "carrier_fax", "carrier_contact",
    *[f"{p}_{i}" for p in ["pickup_customer", "pickup_address", "pickup_city", "pickup_state", "pickup_zipcode", "pickup_start_datetime", "pickup_end_datetime"] for i in range(1, 4)],
    *[f"{p}_{i}" for p in ["delivery_customer", "delivery_address", "delivery_city", "delivery_state", "delivery_zipcode", "delivery_start_datetime", "delivery_end_datetime"] for i in range(1, 4)],
    "processed_at"
]

class BaseExtractor(ABC):
    @abstractmethod
    def extract(self, text: str) -> dict: pass

class CYExtractor(BaseExtractor):
    def _normalize(self, text: str) -> str:
        if not text: return ""
        text = text.replace("**", "").replace("__", "") # Limpia el markdown
        text = re.sub(r"\r\n?|\f", "\n", text)
        return "\n".join(ln.strip() for ln in text.splitlines() if ln.strip())

    def _combine_dt(self, date_str: str, time_str: str) -> str:
        if not date_str or not time_str: return ""
        date_str = re.sub(r"^[A-Za-z]{3}\s+", "", date_str.strip())
        for fmt in ["%m/%d/%Y %H:%M", "%m/%d/%Y %I:%M %p", "%m/%d/%Y %H:%M:%S"]:
            try: return datetime.strptime(f"{date_str} {time_str}", fmt).strftime("%Y-%m-%dT%H:%M:%S")
            except ValueError: continue
        return ""

    def _extract_stop_blocks(self, text: str, stop_type: str) -> list:
        label = r"Pick\s*Up" if stop_type.lower() == "pickup" else "Delivery"
        target_re   = re.compile(rf"(?:#+\s*)?Stop\s*\d+\s*:\s*{label}", re.I)
        any_stop_re = re.compile(r"(?:#+\s*)?Stop\s*\d+\s*:|#+\s*Agreement|#+\s*Charges", re.I)
        all_stops   = list(any_stop_re.finditer(text))

        blocks = []
        for m in target_re.finditer(text):
            nxt = next((s for s in all_stops if s.start() > m.start()), None)
            end = nxt.start() if nxt else len(text)
            blocks.append(text[m.end():end].strip())
        return blocks

    def extract(self, text: str) -> dict:
        data = {f: "" for f in EXTRACTION_FIELDS}
        if not text: return data

        text = self._normalize(text)

        if m := re.search(r"Load\s+(\d{6,})", text, re.I):
            data["loadConfirmationNumber"] = m.group(1).strip()

        if m := re.search(r"(?:Total\s+)?USD\s*\$\s*([\d,]+\.\d{2})", text, re.I):
            data["totalCarrierPay"] = m.group(1).replace(",", "").strip()

        data.update({
            "broker_name": "Coyote Logistics, LLC", "broker_phone": "877-626-9683",
            "broker_email": "CarrierInvoices@coyote.com", "broker_address": "960 Northpoint Parkway Suite 150",
            "broker_city": "Alpharetta", "broker_state": "GA", "broker_zipcode": "30005"
        })
        if m := re.search(r"Fax:\s*(\+?1?\s*\(?\d{3}\)?\s*\d{3}\s*\d{4})", text, re.I):
            data["broker_fax"] = m.group(1).strip()

        if m := re.search(r"\[Carrier Legal Name -\s*(.*?)\]", text, re.I):
            data["carrier_name"] = m.group(1).strip()
        if m := re.search(r"\[Carrier USDOT -\s*(\d+)\]", text, re.I):
            data["carrier_mc"] = m.group(1).strip()

        data["carrier_contact"] = "REDACTED_PRIVACY_POLICY"
        data["carrier_phone"]   = "REDACTED_PRIVACY_POLICY"
        data["carrier_fax"]     = "REDACTED_PRIVACY_POLICY"

        def parse_stop(block: str, i: int, prefix: str):
            # YA NO CORTAMOS EL BLOQUE. Usamos las regex exactas sobre todas las líneas.
            
            # 1. Cliente: Línea que empieza con Facility y NO es "Facility Notes"
            if m := re.search(r"^Facility\s+(?!Notes\b)(.+)$", block, re.M | re.I):
                data[f"{prefix}_customer_{i}"] = m.group(1).strip()

            # 2. Dirección: Atrapa el bloque multilínea entre Address y Contact/Phone/Scheduled
            addr_block_m = re.search(r"^Address\s+([\s\S]+?)\n(?:Contact|Phone|Scheduled|Appointment|Driver)", block, re.M | re.I)
            if addr_block_m:
                raw_addr = addr_block_m.group(1).strip()
                # Corta la dirección en 4 grupos: Calle, Ciudad, Estado, Zip
                parts_m = re.search(r"([\s\S]+?)\n([^\n,]+)(?:,|\n)\s*([A-Z]{2})\s*\n?(\d{5}(?:-\d{4})?)$", raw_addr, re.I)
                if parts_m:
                    data[f"{prefix}_address_{i}"] = parts_m.group(1).replace("\n", " ").strip()
                    data[f"{prefix}_city_{i}"]    = parts_m.group(2).strip()
                    data[f"{prefix}_state_{i}"]   = parts_m.group(3).strip()
                    data[f"{prefix}_zipcode_{i}"] = parts_m.group(4)[:5].strip()

            # 3. Fechas
            date_m = re.search(r"(?:Appointment\s+)?Scheduled For\n(?:[A-Za-z]{3}\s*)?(\d{2}/\d{2}/\d{4})", block, re.I)
            time_m = re.search(r"\n(?:from|at)\s*(\d{2}:\d{2})(?:\s*-\s*(\d{2}:\d{2}))?", block, re.I)
            
            if date_m and time_m:
                fecha = date_m.group(1)
                data[f"{prefix}_start_datetime_{i}"] = self._combine_dt(fecha, time_m.group(1))
                data[f"{prefix}_end_datetime_{i}"]   = self._combine_dt(fecha, time_m.group(2) or time_m.group(1))

        pickups = self._extract_stop_blocks(text, "pickup")
        for i, block in enumerate(pickups[:3], start=1):
            parse_stop(block, i, "pickup")

        deliveries = self._extract_stop_blocks(text, "delivery")
        for i, block in enumerate(deliveries[:3], start=1):
            parse_stop(block, i, "delivery")

        data["processed_at"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
        return data

def extract_fields_udf():
    def _extract(text: str):
        return CYExtractor().extract(text or "")
    return udf(_extract, MapType(StringType(), StringType()))

def main(p):
    spark = SparkSession.builder.appName("TruckR_CY_Extraction").getOrCreate()
    logger.info(f"Iniciando extracción para: {p['source_path']}")

    # Volvemos al lector estándar que descubrimos que SÍ funciona bien en Unity Catalog
    df = (
        spark.read
        .option("wholetext", "true")
        .option("encoding", "UTF-8")
        .text(os.path.join(p["source_path"], "*.txt"))
        .select(
            col("_metadata.file_path").alias("source_file"),
            col("value").alias("text")
        )
    )

    extract_udf = extract_fields_udf()
    df = df.withColumn("extracted", extract_udf(col("text")))

    for field in EXTRACTION_FIELDS:
        df = df.withColumn(field, col("extracted").getItem(field))

    df = df.drop("text", "extracted")

    df.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable(p["target_table"])
    logger.info("✅ Extracción CY completada exitosamente.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--source_path", required=True)
    parser.add_argument("--target_table", required=True)
    args = parser.parse_args()
    main({"source_path": args.source_path, "target_table": args.target_table})