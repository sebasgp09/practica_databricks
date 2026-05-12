import argparse
import logging
from pyspark.sql import SparkSession, functions as F, types as T

# ------------------------------------------------------------------------------
# Logger Configuration
# ------------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("CY_Validator")

# ------------------------------------------------------------------------------
# Parse Arguments
# ------------------------------------------------------------------------------
parser = argparse.ArgumentParser(description="CY Extractor Validator")
parser.add_argument(
    "--source_table",
    required=True,
    help="Spark table name for validation (e.g., logistics.silver.cy_load_confirmations)"
)
args = parser.parse_args()
source_table = args.source_table

# ------------------------------------------------------------------------------
# Spark Session
# ------------------------------------------------------------------------------
spark = SparkSession.builder.appName("CY_Extractor_Validator").getOrCreate()

# ------------------------------------------------------------------------------
# Schema Definition
# ------------------------------------------------------------------------------
fields = [
    "broker_name", "broker_phone", "broker_fax", "broker_address",
    "broker_city", "broker_state", "broker_zipcode", "broker_email",
    "loadConfirmationNumber", "totalCarrierPay", "carrier_name", "carrier_mc",
    "carrier_phone", "carrier_fax", "carrier_contact", "pickup_customer_1",
    "pickup_address_1", "pickup_city_1", "pickup_state_1", "pickup_zipcode_1",
    "pickup_start_datetime_1", "pickup_end_datetime_1", "delivery_customer_1",
    "delivery_address_1", "delivery_city_1", "delivery_state_1", "delivery_zipcode_1",
    "delivery_start_datetime_1", "delivery_end_datetime_1"
]
schema = T.StructType([T.StructField(f, T.StringType()) for f in fields])

# ------------------------------------------------------------------------------
# Ground Truth Record (Coyote Logistics Example)
# ------------------------------------------------------------------------------
truth_record = {
    "broker_name": "Coyote Logistics, LLC",
    "broker_phone": "877-626-9683",
    "broker_fax": "+1 (847) 810 4891",
    "broker_address": "960 Northpoint Parkway Suite 150",
    "broker_city": "Alpharetta",
    "broker_state": "GA",
    "broker_zipcode": "30005",
    "broker_email": "CarrierInvoices@coyote.com",
    "loadConfirmationNumber": "28861101",
    "totalCarrierPay": "600.00",
    "carrier_name": "GTT Freight Corp",
    "carrier_mc": "3723304",
    "carrier_phone": "REDACTED_PRIVACY_POLICY",
    "carrier_fax": "REDACTED_PRIVACY_POLICY",
    "carrier_contact": "REDACTED_PRIVACY_POLICY",
    "pickup_customer_1": "United Sugars",
    "pickup_address_1": "450 SONORA DRIVE GATE D",
    "pickup_city_1": "Clewiston",
    "pickup_state_1": "FL",
    "pickup_zipcode_1": "33440",
    "pickup_start_datetime_1": "2023-03-29T08:00:00",
    "pickup_end_datetime_1": "2023-03-29T13:00:00",
    "delivery_customer_1": "Batory Foods",
    "delivery_address_1": "885 DOUGLAS HILLS RD",
    "delivery_city_1": "Lithia Springs",
    "delivery_state_1": "GA",
    "delivery_zipcode_1": "30122",
    "delivery_start_datetime_1": "2023-03-30T09:30:00",
    "delivery_end_datetime_1": "2023-03-30T09:30:00",
}
truth_df = spark.createDataFrame([truth_record], schema=schema)

# ------------------------------------------------------------------------------
# Load Target Table
# ------------------------------------------------------------------------------
target_df = spark.table(source_table)

# ------------------------------------------------------------------------------
# Normalize Data
# ------------------------------------------------------------------------------
def normalize(df):
    string_cols = [c for c, t in df.dtypes if t == "string"]
    return df.select(*[
        F.trim(F.lower(F.col(c))).alias(c) if c in string_cols else F.col(c)
        for c in df.columns
    ])

truth_df = normalize(truth_df)
target_df = normalize(target_df)

# ------------------------------------------------------------------------------
# Compare Values
# ------------------------------------------------------------------------------
load_id = truth_record["loadConfirmationNumber"]
target_rows = target_df.filter(F.col("loadConfirmationNumber") == load_id).collect()

results = []

if not target_rows:
    logger.error(f"No record found for loadConfirmationNumber={load_id}")
    for col_name in schema.fieldNames():
        results.append((col_name, "❌ Missing record", truth_record.get(col_name), None))
else:
    logger.info(f"Found record for loadConfirmationNumber={load_id}")
    target_values = target_rows[0].asDict()
    for col_name in schema.fieldNames():
        truth_val = truth_record.get(col_name)
        target_val = target_values.get(col_name)
        
        norm_truth = str(truth_val).strip().lower() if truth_val else None
        norm_target = str(target_val).strip().lower() if target_val else None
        
        status = "✅ Match" if norm_truth == norm_target else "❌ Mismatch"
        results.append((col_name, status, truth_val, target_val))

# ------------------------------------------------------------------------------
# Log Results
# ------------------------------------------------------------------------------
for field, status, truth, target in results:
    if status == "✅ Match":
        logger.info(f"{field:30} | {status:10} | truth='{truth}' | target='{target}'")
    else:
        logger.error(f"{field:30} | {status:10} | truth='{truth}' | target='{target}'")

# ------------------------------------------------------------------------------
# Final Check
# ------------------------------------------------------------------------------
errors = [r for r in results if r[1].startswith("❌")]
if errors:
    logger.error(f"Validation failed for {len(errors)} fields")
    raise ValueError(f"Validation failed for {len(errors)} fields")
else:
    logger.info("✅ All CY fields match perfectly.")