import logging

from gold.category_performance import build_category_performance
from gold.customer_metrics import build_customer_metrics
from gold.daily_sales import build_daily_sales
from gold.product_performance import build_product_performance

logger = logging.getLogger(__name__)


def build_gold_layer(
    spark,
    orders_silver_table,
    order_items_silver_table,
    products_silver_table,
    customers_silver_table,
    daily_sales_gold_table,
    product_performance_gold_table,
    category_performance_gold_table,
    customer_metrics_gold_table,
):
    # Read shared Silver fact tables once and reuse them across Gold transformations
    orders_silver_df = spark.table(orders_silver_table)

    order_items_silver_df = spark.table(order_items_silver_table)

    # Build daily sales metrics from completed orders and order items
    logger.info("Gold Daily Sales transformation started")

    daily_sales_df = build_daily_sales(
        orders_silver_df,
        order_items_silver_df,
    )

    try:
        # Gold tables are fully rebuilt from current Silver state
        daily_sales_df.write.format("delta").mode("overwrite").saveAsTable(
            daily_sales_gold_table
        )

        logger.info("Gold Daily Sales saved successfully")

    except Exception:
        logger.exception("Gold Daily Sales load failed")
        raise

    # Product dimension is reused by product and category performance
    products_silver_df = spark.table(products_silver_table)

    # Build product-level sales metrics
    logger.info("Gold Product Performance transformation started")

    product_performance_df = build_product_performance(
        orders_silver_df,
        order_items_silver_df,
        products_silver_df,
    )

    try:
        product_performance_df.write.format("delta").mode("overwrite").saveAsTable(
            product_performance_gold_table
        )

        logger.info("Gold Product Performance saved successfully")

    except Exception:
        logger.exception("Gold Product Performance load failed")
        raise

    # Build category-level sales metrics
    logger.info("Gold Category Performance transformation started")

    category_performance_df = build_category_performance(
        orders_silver_df,
        order_items_silver_df,
        products_silver_df,
    )

    try:
        category_performance_df.write.format("delta").mode("overwrite").saveAsTable(
            category_performance_gold_table
        )

        logger.info("Gold Category Performance saved successfully")

    except Exception:
        logger.exception("Gold Category Performance load failed")
        raise

    # Customer dimension is only required for customer-level metrics
    customers_silver_df = spark.table(customers_silver_table)

    # Build customer-level sales and order metrics
    logger.info("Gold Customer Metrics transformation started")

    customer_metrics_df = build_customer_metrics(
        orders_silver_df,
        order_items_silver_df,
        customers_silver_df,
    )

    try:
        customer_metrics_df.write.format("delta").mode("overwrite").saveAsTable(
            customer_metrics_gold_table
        )

        logger.info("Gold Customer Metrics saved successfully")

    except Exception:
        logger.exception("Gold Customer Metrics load failed")
        raise
