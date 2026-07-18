# Databricks notebook source
from pyspark.sql.functions import col, when

# Read from Bronze table directly - no file paths needed
bronze_df = spark.table("rahul_de_databricks_dev.bronze.pharma_sales_raw")

# Sanitized transformation: add IsWeekend flag
silver_df = bronze_df.withColumn(
    "IsWeekend",
    when(col("WeekdayName").isin("Saturday", "Sunday"), "Yes").otherwise("No")
)

silver_df.show(5)

# Write to Unity Catalog - Silver/Sanitized layer
spark.sql("CREATE SCHEMA IF NOT EXISTS rahul_de_databricks_dev.sanitized")
silver_df.write.mode("overwrite").saveAsTable("rahul_de_databricks_dev.sanitized.pharma_sales")

print("Silver table written successfully")

# COMMAND ----------

silver_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("rahul_de_databricks_dev.sanitized.pharma_sales")

# COMMAND ----------

try:
    bronze_df = spark.table("rahul_de_databricks_dev.bronze.pharma_sales_raw")
    print(f"Successfully read Bronze table, {bronze_df.count()} rows")
except Exception as e:
    print(f"ERROR: Failed to read Bronze table: {e}")
    raise

try:
    silver_df = bronze_df.withColumn(
        "IsWeekend",
        when(col("WeekdayName").isin("Saturday", "Sunday"), "Yes").otherwise("No")
    )
    spark.sql("CREATE SCHEMA IF NOT EXISTS rahul_de_databricks_dev.sanitized")
    silver_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("rahul_de_databricks_dev.sanitized.pharma_sales")
    print("Silver table written successfully")
except Exception as e:
    print(f"ERROR: Failed to write Silver table: {e}")
    raise
