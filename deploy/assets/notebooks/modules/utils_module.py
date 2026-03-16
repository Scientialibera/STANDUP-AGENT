# Fabric Notebook -- utils_module
# Shared analytics helpers for standup enrichment and aggregation.

from pyspark.sql import DataFrame
import pyspark.sql.functions as F


def deduplicate(df: DataFrame, partition_cols: list, order_col: str = "timestamp") -> DataFrame:
    """Keep latest record per partition key."""
    from pyspark.sql.window import Window

    w = Window.partitionBy(*partition_cols).orderBy(F.col(order_col).desc())
    return df.withColumn("_rn", F.row_number().over(w)).filter(F.col("_rn") == 1).drop("_rn")


def classify_sentiment(text_col: str) -> F.Column:
    """Rule-based sentiment: negative keywords -> negative, positive keywords -> positive, else neutral."""
    neg_pattern = "(?i)(block|stuck|wait|delay|issue|problem|bug|fail|broken|unable|cannot)"
    pos_pattern = "(?i)(done|complete|finish|ship|deploy|resolv|fix|success|merge)"
    return (
        F.when(F.col(text_col).rlike(neg_pattern), F.lit("negative"))
        .when(F.col(text_col).rlike(pos_pattern), F.lit("positive"))
        .otherwise(F.lit("neutral"))
    )


def extract_blocker_category(text_col: str) -> F.Column:
    """Rule-based blocker categorization."""
    return (
        F.when(F.col(text_col).rlike("(?i)(infra|deploy|ci|cd|pipeline|server|cloud|network)"), F.lit("infrastructure"))
        .when(F.col(text_col).rlike("(?i)(process|approval|review|meeting|decision|priorit)"), F.lit("process"))
        .when(F.col(text_col).rlike("(?i)(depend|upstream|downstream|team|api|service|external)"), F.lit("dependency"))
        .otherwise(F.lit("other"))
    )


def safe_date_parse(date_col: str) -> F.Column:
    return F.to_date(F.col(date_col), "yyyy-MM-dd")
