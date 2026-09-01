import logging

logger = logging.getLogger(__name__)


# used in main.py
def load_to_quarantine(spark, invalid_df, quarantine_table, entity_id_column):
    logger.info("Started loading quarantine")

    # First load -> Quarantine non existent -> append all invalid rows
    if not spark.catalog.tableExists(quarantine_table):
        logger.info("Quarantine first load")
        invalid_df.write.format("delta").mode("append").saveAsTable(quarantine_table)
        return

    # Quarantine already exists -> read the Quarantine rows
    logger.info("Quarantine exists | Deduplicating current batch")
    existing_quarantine_df = spark.table(
        quarantine_table
    )

    # Keep only new invalid rows by batch_id + entity ID
    new_invalid_df = invalid_df.join(
        existing_quarantine_df,
        on=["batch_id", entity_id_column],
        how="left_anti",
    )

    # Append new invalid rows to Quarantine
    new_invalid_df.write.format("delta").mode("append").saveAsTable(quarantine_table)
    logger.info("Quarantine saved")
