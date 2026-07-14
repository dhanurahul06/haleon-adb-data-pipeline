# Databricks notebook source
# Retrieve SQL credentials securely from Key Vault via the secret scope
sql_username = dbutils.secrets.get(scope="kv-dev-scope", key="sql-dev-username")
sql_password = dbutils.secrets.get(scope="kv-dev-scope", key="sql-dev-password")

# JDBC connection details
jdbc_hostname = "rahul-de-sql-dev.database.windows.net"
jdbc_database = "pharma-sales-dev"
jdbc_port = 1433

jdbc_url = f"jdbc:sqlserver://{jdbc_hostname}:{jdbc_port};database={jdbc_database}"

connection_properties = {
    "user": sql_username,
    "password": sql_password,
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

# Read the full PharmaSalesDaily table directly via JDBC - no ADF needed
df = spark.read.jdbc(url=jdbc_url, table="PharmaSalesDaily", properties=connection_properties)
df.show(5)
print("Row count:", df.count())

# COMMAND ----------

# Create a schema for this layer if needed
spark.sql("CREATE SCHEMA IF NOT EXISTS rahul_de_databricks_dev.bronze")

# Write raw data into a Bronze Unity Catalog table
df.write.mode("overwrite").saveAsTable("rahul_de_databricks_dev.bronze.pharma_sales_raw")

print("Bronze table written successfully")

# COMMAND ----------

from pyspark.sql.functions import lit

# Read the current watermark directly from SQL via JDBC
watermark_df = spark.read.jdbc(url=jdbc_url, table="LoadControl", properties=connection_properties)
watermark_df.show()

last_loaded_date = watermark_df.filter(watermark_df.TableName == "PharmaSalesDaily").collect()[0]["LastLoadedDate"]
print("Last loaded date:", last_loaded_date)

# Read only rows newer than the watermark - incremental read via JDBC query pushdown
incremental_query = f"(SELECT * FROM PharmaSalesDaily WHERE SaleDate > '{last_loaded_date}') AS incremental_data"

df_incremental = spark.read.jdbc(url=jdbc_url, table=incremental_query, properties=connection_properties)
df_incremental.show()
print("Incremental row count:", df_incremental.count())

# COMMAND ----------

from py4j.java_gateway import java_import

# Update LastLoadedDate in SQL after successful load
max_date = df_incremental.agg({"SaleDate": "max"}).collect()[0][0]

if max_date is not None:
    import java.sql as sql  # placeholder comment - actual JDBC write below
    
    update_query = f"""
    UPDATE LoadControl 
    SET LastLoadedDate = '{max_date}' 
    WHERE TableName = 'PharmaSalesDaily'
    """
    
    # Use a direct JDBC connection to execute the UPDATE (Spark's DataFrame writer can't run raw UPDATE statements)
    driver_manager = spark._sc._gateway.jvm.java.sql.DriverManager
    conn = driver_manager.getConnection(jdbc_url, sql_username, sql_password)
    stmt = conn.createStatement()
    stmt.executeUpdate(update_query)
    conn.close()
    print(f"Watermark updated to {max_date}")
else:
    print("No new rows - watermark unchanged")
