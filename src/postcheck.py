from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.window import Window

from config import paths


def main():
    spark = SparkSession.builder.getOrCreate()

    required_gold_tables = [
        "daily_sales",
        "product_performance",
        "category_performance",
        "customer_metrics",
    ]

    for table in required_gold_tables:
        full_table_name = paths.table_name(paths.GOLD_SCHEMA, table)

        if not spark.catalog.tableExists(full_table_name):
            raise RuntimeError(
                f"Required Gold table does not exist: {full_table_name}"
            )

        if spark.table(full_table_name).limit(1).count() == 0:
            raise RuntimeError(
                f"Gold table is empty: {full_table_name}"
            )

    control_table = paths.table_name(
        paths.CONTROL_SCHEMA,
        "processed_files",
    )

    if not spark.catalog.tableExists(control_table):
        raise RuntimeError(
            f"Control table does not exist: {control_table}"
        )

    control_df = spark.table(control_table)

    window = (
        Window
        .partitionBy("file_hash")
        .orderBy(f.desc("processed_at"))
    )

    latest_status_df = (
        control_df
        .withColumn(
            "row_number",
            f.row_number().over(window),
        )
        .filter(f.col("row_number") == 1)
    )

    failed_files = (
        latest_status_df
        .filter(f.col("status") != "SUCCESS")
        .limit(1)
        .count()
    )

    if failed_files > 0:
        raise RuntimeError(
            "At least one source file does not have SUCCESS as its latest status"
        )

    print("Postcheck completed successfully")


if __name__ == "__main__":
    main()