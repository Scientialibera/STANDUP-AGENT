# Fabric Notebook -- 03_enrich_main
# Bronze -> Silver: sentiment analysis on blockers, topic extraction, blocker duration tracking.

# %run ../modules/config_module
# %run ../modules/utils_module

from pyspark.sql import SparkSession
from pyspark.sql.window import Window
import pyspark.sql.functions as F

spark = SparkSession.builder.getOrCreate()

bronze_path = get_table_path(BRONZE_LAKEHOUSE, CLEAN_TABLE)
clean_df = spark.read.format("delta").load(bronze_path)

enriched_df = (
    clean_df
    .withColumn("blocker_sentiment", classify_sentiment("blockers"))
    .withColumn("blocker_category", extract_blocker_category("blockers"))
    .withColumn("has_blocker",
        (~F.col("skipped")) & (F.length(F.trim(F.col("blockers"))) > 0) & (~F.lower(F.col("blockers")).isin("none", "n/a", "no", ""))
    )
)

# Blocker consecutive days: count streak of days with a blocker per user+team
w = Window.partitionBy("team", "user").orderBy("date")
enriched_df = enriched_df.withColumn("_prev_blocker", F.lag("has_blocker").over(w))
enriched_df = enriched_df.withColumn(
    "_streak_reset",
    F.when(
        (F.col("has_blocker") == True) & (F.coalesce(F.col("_prev_blocker"), F.lit(False)) == True),
        F.lit(0)
    ).otherwise(F.lit(1))
)
w2 = Window.partitionBy("team", "user").orderBy("date").rowsBetween(Window.unboundedPreceding, Window.currentRow)
enriched_df = enriched_df.withColumn("_streak_group", F.sum("_streak_reset").over(w2))
w3 = Window.partitionBy("team", "user", "_streak_group").orderBy("date")
enriched_df = enriched_df.withColumn(
    "blocker_consecutive_days",
    F.when(F.col("has_blocker"), F.row_number().over(w3)).otherwise(F.lit(0))
)
enriched_df = enriched_df.drop("_prev_blocker", "_streak_reset", "_streak_group")

# Update topics -- simple keyword extraction from today's work
update_keywords = ["deploy", "review", "test", "meeting", "design", "refactor", "bug", "feature", "docs", "release"]
for kw in update_keywords:
    enriched_df = enriched_df.withColumn(
        f"_topic_{kw}",
        F.when(F.lower(F.col("today")).contains(kw), F.lit(kw)).otherwise(F.lit(None))
    )

topic_cols = [f"_topic_{kw}" for kw in update_keywords]
enriched_df = enriched_df.withColumn(
    "update_topics",
    F.array_compact(F.array(*[F.col(c) for c in topic_cols]))
)
enriched_df = enriched_df.drop(*topic_cols)
enriched_df = enriched_df.withColumn("_enriched_at", F.current_timestamp())

silver_path = get_table_path(SILVER_LAKEHOUSE, ENRICHED_TABLE)
enriched_df.write.format("delta").mode("overwrite").save(silver_path)

print(f"Enriched {enriched_df.count()} records into {ENRICHED_TABLE}.")
