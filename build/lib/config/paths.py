CATALOG = "workspace"

BRONZE_SCHEMA = "bronze"
SILVER_SCHEMA = "silver"
GOLD_SCHEMA = "gold"
QUARANTINE_SCHEMA = "quarantine"
CONTROL_SCHEMA = "control"

RAW_VOLUME_PATH = "/Volumes/workspace/bronze/raw"


def table_name(schema, table):
    return f"{CATALOG}.{schema}.{table}"


def raw_file_path(file_name):
    return f"{RAW_VOLUME_PATH}/{file_name}"