import logging

from pyspark.sql import SparkSession
from pipelines.products_pipeline import process_products
from pipelines.customers_pipeline import process_customers
from pipelines.orders_pipeline import process_orders
from pipelines.order_items_pipeline import process_order_items
from pipelines.gold_pipeline import build_gold_layer
from config import paths

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

logger = logging.getLogger(__name__)


def main():
    spark = SparkSession.builder.getOrCreate()

    logger.info("Pipeline started")
    process_products(
        spark,
        paths.raw_file_path("products.csv"),
        paths.table_name(paths.BRONZE_SCHEMA, "products"),
        paths.table_name(paths.SILVER_SCHEMA, "products"),
        paths.table_name(paths.QUARANTINE_SCHEMA, "products"),
        paths.table_name(paths.CONTROL_SCHEMA, "processed_files"),
    )

    process_customers(
        spark,
        paths.raw_file_path("customers.csv"),
        paths.table_name(paths.BRONZE_SCHEMA, "customers"),
        paths.table_name(paths.SILVER_SCHEMA, "customers"),
        paths.table_name(paths.QUARANTINE_SCHEMA, "customers"),
        paths.table_name(paths.CONTROL_SCHEMA, "processed_files"),
    )

    process_orders(
        spark,
        paths.raw_file_path("orders.csv"),
        paths.table_name(paths.BRONZE_SCHEMA, "orders"),
        paths.table_name(paths.SILVER_SCHEMA, "orders"),
        paths.table_name(paths.QUARANTINE_SCHEMA, "orders"),
        paths.table_name(paths.CONTROL_SCHEMA, "processed_files"),
    )

    process_order_items(
        spark,
        paths.raw_file_path("order_items.csv"),
        paths.table_name(paths.BRONZE_SCHEMA, "order_items"),
        paths.table_name(paths.SILVER_SCHEMA, "order_items"),
        paths.table_name(paths.QUARANTINE_SCHEMA, "order_items"),
        paths.table_name(paths.CONTROL_SCHEMA, "processed_files"),
    )

    build_gold_layer(
        spark,
        paths.table_name(paths.SILVER_SCHEMA, "orders"),
        paths.table_name(paths.SILVER_SCHEMA, "order_items"),
        paths.table_name(paths.SILVER_SCHEMA, "products"),
        paths.table_name(paths.SILVER_SCHEMA, "customers"),
        paths.table_name(paths.GOLD_SCHEMA, "daily_sales"),
        paths.table_name(paths.GOLD_SCHEMA, "product_performance"),
        paths.table_name(paths.GOLD_SCHEMA, "category_performance"),
        paths.table_name(paths.GOLD_SCHEMA, "customer_metrics"),
    )

    logger.info("Pipeline completed successfully")

if __name__ == "__main__":
    main()
