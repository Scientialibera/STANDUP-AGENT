# Fabric Notebook -- config_module
# Shared configuration and lakehouse helpers for the Standup Analytics pipeline.

LANDING_LAKEHOUSE = "lh_standup_landing"
BRONZE_LAKEHOUSE = "lh_standup_bronze"
SILVER_LAKEHOUSE = "lh_standup_silver"
GOLD_LAKEHOUSE = "lh_standup_gold"

BLOB_ACCOUNT_URL = ""
BLOB_CONTAINER = "standup-responses"

RAW_TABLE = "raw_standups"
CLEAN_TABLE = "standups_clean"
ENRICHED_TABLE = "standups_enriched"

GOLD_TABLES = {
    "team_response_rates": "team_response_rates",
    "blocker_analysis": "blocker_analysis",
    "team_health_score": "team_health_score",
    "member_activity": "member_activity",
}


def get_lakehouse_path(lakehouse: str, zone: str = "Tables") -> str:
    return f"abfss://{lakehouse}@onelake.dfs.fabric.microsoft.com/{zone}"


def get_table_path(lakehouse: str, table: str) -> str:
    return f"{get_lakehouse_path(lakehouse)}/{table}"
