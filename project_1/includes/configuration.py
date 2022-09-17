# Databricks notebook source
storage_account_name = "formul1adls"
storage_account_key  = "qYLNVmVhHGR7H8sSg/hiJz7jxX5zCCfErINRQBkH1YieO4/FZ9fZKOQ73H6YRwsWp2wRVSIT5Qve+AStJWoNEg=="

spark.conf.set(
    f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net",
    f"{storage_account_key}")

container_name = "raw" 
container_name1 = "processed"

# COMMAND ----------

raw_path = "abfss://{container_name}@{storage_account_name}.dfs.core.windows.net"
processed_path = "abfss://{container_name1}@{storage_account_name}.dfs.core.windows.net"

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------


