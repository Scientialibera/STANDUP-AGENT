# Fabric Notebook -- 01_ingest_main
# Reads raw standup JSON blobs from Azure Blob Storage into the Landing lakehouse.

# %run ../modules/config_module

from pyspark.sql import SparkSession
import pyspark.sql.functions as F

spark = SparkSession.builder.getOrCreate()

blob_base = f"wasbs://{BLOB_CONTAINER}@{BLOB_ACCOUNT_URL.replace('https://','').replace('.blob.core.windows.net','')}/"

raw_df = (
    spark.read
    .option("multiline", "true")
    .json(f"{blob_base}*/*/*.json")
)

expected_cols = ["team", "date", "user", "yesterday", "today", "blockers", "skipped", "timestamp"]
for col in expected_cols:
    if col not in raw_df.columns:
        raw_df = raw_df.withColumn(col, F.lit(None).cast("string"))

raw_df = raw_df.select(*expected_cols)
raw_df = raw_df.withColumn("_ingested_at", F.current_timestamp())

landing_path = get_table_path(LANDING_LAKEHOUSE, RAW_TABLE)
raw_df.write.format("delta").mode("overwrite").save(landing_path)

print(f"Ingested {raw_df.count()} records into {RAW_TABLE}.")
