# Databricks notebook source
from pyspark.sql.functions import sum as spark_sum, round as spark_round

# Read from Silver table directly
silver_df = spark.table("rahul_de_databricks_dev.sanitized.pharma_sales")

# Aggregate: total monthly sales per drug category, split by weekend/weekday
gold_df = silver_df.groupBy("SalesYear", "SalesMonth", "IsWeekend").agg(
    spark_round(spark_sum("M01AB"), 2).alias("Total_M01AB"),
    spark_round(spark_sum("M01AE"), 2).alias("Total_M01AE"),
    spark_round(spark_sum("N02BA"), 2).alias("Total_N02BA"),
    spark_round(spark_sum("N02BE"), 2).alias("Total_N02BE"),
    spark_round(spark_sum("N05B"), 2).alias("Total_N05B"),
    spark_round(spark_sum("N05C"), 2).alias("Total_N05C"),
    spark_round(spark_sum("R03"), 2).alias("Total_R03"),
    spark_round(spark_sum("R06"), 2).alias("Total_R06")
)

gold_df.orderBy("SalesYear", "SalesMonth").show(10)

# Write to Unity Catalog - Gold/Optimized layer
spark.sql("CREATE SCHEMA IF NOT EXISTS rahul_de_databricks_dev.optimized")
gold_df.write.mode("overwrite").saveAsTable("rahul_de_databricks_dev.optimized.pharma_sales_summary")

print("Gold table written successfully")

# COMMAND ----------

gold_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("rahul_de_databricks_dev.optimized.pharma_sales_summary")

# COMMAND ----------

spark.sql("SELECT * FROM rahul_de_databricks_dev.optimized.pharma_sales_summary ORDER BY SalesYear, SalesMonth LIMIT 10").show()