from pyspark.sql import SparkSession

from config import paths


def main():
    spark = SparkSession.builder.getOrCreate()

    required_files = [
        "products.csv",
        "customers.csv",
        "orders.csv",
        "order_items.csv",
    ]

    for file_name in required_files:
        path = paths.raw_file_path(file_name)

        try:
            spark.read.option("header", True).csv(path).limit(1).collect()
        except Exception as exc:
            raise RuntimeError(f"Cannot read required file: {path}") from exc

    required_schemas = [
        paths.BRONZE_SCHEMA,
        paths.SILVER_SCHEMA,
        paths.GOLD_SCHEMA,
        paths.QUARANTINE_SCHEMA,
        paths.CONTROL_SCHEMA,
    ]

    for schema in required_schemas:
        schema_name = f"{paths.CATALOG}.{schema}"

        schemas = [
            row.databaseName
            for row in spark.sql(f"SHOW SCHEMAS IN {paths.CATALOG}").collect()
        ]

        if schema not in schemas:
            raise RuntimeError(f"Required schema does not exist: {schema_name}")

    print("Precheck completed successfully")


if __name__ == "__main__":
    main()