# Fabric Notebook -- 02_transform_main
# Landing -> Bronze: normalize schema, type casting, dedup.

# %run ../modules/config_module
# %run ../modules/utils_module

from pyspark.sql import SparkSession
import pyspark.sql.functions as F

spark = SparkSession.builder.getOrCreate()

landing_path = get_table_path(LANDING_LAKEHOUSE, RAW_TABLE)
raw_df = spark.read.format("delta").load(landing_path)

clean_df = (
    raw_df
    .withColumn("date", safe_date_parse("date"))
    .withColumn("skipped", F.coalesce(F.col("skipped").cast("boolean"), F.lit(False)))
    .withColumn("team", F.lower(F.trim(F.col("team"))))
    .withColumn("user", F.lower(F.trim(F.col("user"))))
    .withColumn("yesterday", F.coalesce(F.col("yesterday"), F.lit("")))
    .withColumn("today", F.coalesce(F.col("today"), F.lit("")))
    .withColumn("blockers", F.coalesce(F.col("blockers"), F.lit("")))
    .withColumn("timestamp", F.to_timestamp(F.col("timestamp")))
)

clean_df = deduplicate(clean_df, partition_cols=["team", "date", "user"], order_col="timestamp")
clean_df = clean_df.withColumn("_transformed_at", F.current_timestamp())

bronze_path = get_table_path(BRONZE_LAKEHOUSE, CLEAN_TABLE)
clean_df.write.format("delta").mode("overwrite").save(bronze_path)

print(f"Transformed {clean_df.count()} records into {CLEAN_TABLE}.")
