# Fabric Notebook -- 04_aggregate_main
# Silver -> Gold: response rates, blocker analysis, team health score, member activity.

# %run ../modules/config_module
# %run ../modules/utils_module

from pyspark.sql import SparkSession
import pyspark.sql.functions as F

spark = SparkSession.builder.getOrCreate()

silver_path = get_table_path(SILVER_LAKEHOUSE, ENRICHED_TABLE)
enriched_df = spark.read.format("delta").load(silver_path)

# ---- team_response_rates ----
response_rates = (
    enriched_df
    .groupBy("team", "date")
    .agg(
        F.count("*").alias("total_members"),
        F.sum(F.when(~F.col("skipped"), 1).otherwise(0)).alias("responded_count"),
        F.sum(F.when(F.col("skipped"), 1).otherwise(0)).alias("skipped_count"),
    )
    .withColumn("response_rate", F.col("responded_count") / F.col("total_members"))
    .withColumn("week", F.weekofyear(F.col("date")))
    .withColumn("month", F.month(F.col("date")))
    .withColumn("year", F.year(F.col("date")))
    .withColumn("_aggregated_at", F.current_timestamp())
)

rates_path = get_table_path(GOLD_LAKEHOUSE, GOLD_TABLES["team_response_rates"])
response_rates.write.format("delta").mode("overwrite").save(rates_path)
print(f"  team_response_rates: {response_rates.count()} rows")

# ---- blocker_analysis ----
blockers_df = enriched_df.filter(F.col("has_blocker") == True)
blocker_analysis = (
    blockers_df
    .groupBy("team", "blocker_category")
    .agg(
        F.count("*").alias("blocker_count"),
        F.avg("blocker_consecutive_days").alias("avg_duration_days"),
        F.max("blocker_consecutive_days").alias("max_duration_days"),
        F.countDistinct("user").alias("affected_members"),
        F.countDistinct("date").alias("affected_days"),
    )
    .withColumn("_aggregated_at", F.current_timestamp())
)

blocker_path = get_table_path(GOLD_LAKEHOUSE, GOLD_TABLES["blocker_analysis"])
blocker_analysis.write.format("delta").mode("overwrite").save(blocker_path)
print(f"  blocker_analysis: {blocker_analysis.count()} rows")

# ---- team_health_score ----
# Composite: avg_response_rate * 0.4 + (1 - avg_blocker_rate) * 0.3 + avg_update_length_score * 0.3
team_daily = (
    enriched_df
    .groupBy("team", "date")
    .agg(
        F.count("*").alias("total"),
        F.sum(F.when(~F.col("skipped"), 1).otherwise(0)).alias("responded"),
        F.sum(F.when(F.col("has_blocker"), 1).otherwise(0)).alias("blocked"),
        F.avg(F.length(F.col("today"))).alias("avg_update_len"),
    )
)
team_health = (
    team_daily
    .groupBy("team")
    .agg(
        F.avg(F.col("responded") / F.col("total")).alias("avg_response_rate"),
        F.avg(F.col("blocked") / F.col("total")).alias("avg_blocker_rate"),
        F.avg("avg_update_len").alias("avg_update_length"),
    )
    .withColumn("update_length_score", F.least(F.col("avg_update_length") / 200.0, F.lit(1.0)))
    .withColumn(
        "health_score",
        F.col("avg_response_rate") * 0.4
        + (1 - F.col("avg_blocker_rate")) * 0.3
        + F.col("update_length_score") * 0.3,
    )
    .withColumn("_aggregated_at", F.current_timestamp())
)

health_path = get_table_path(GOLD_LAKEHOUSE, GOLD_TABLES["team_health_score"])
team_health.write.format("delta").mode("overwrite").save(health_path)
print(f"  team_health_score: {team_health.count()} rows")

# ---- member_activity ----
member_activity = (
    enriched_df
    .groupBy("team", "user")
    .agg(
        F.count("*").alias("total_standups"),
        F.sum(F.when(~F.col("skipped"), 1).otherwise(0)).alias("submitted"),
        F.sum(F.when(F.col("skipped"), 1).otherwise(0)).alias("skipped"),
        F.sum(F.when(F.col("has_blocker"), 1).otherwise(0)).alias("blocker_days"),
        F.avg(F.length(F.col("yesterday"))).alias("avg_yesterday_len"),
        F.avg(F.length(F.col("today"))).alias("avg_today_len"),
        F.max("date").alias("last_standup_date"),
    )
    .withColumn("response_rate", F.col("submitted") / F.col("total_standups"))
    .withColumn("blocker_rate", F.col("blocker_days") / F.col("total_standups"))
    .withColumn("_aggregated_at", F.current_timestamp())
)

activity_path = get_table_path(GOLD_LAKEHOUSE, GOLD_TABLES["member_activity"])
member_activity.write.format("delta").mode("overwrite").save(activity_path)
print(f"  member_activity: {member_activity.count()} rows")

print("Gold aggregation complete.")
