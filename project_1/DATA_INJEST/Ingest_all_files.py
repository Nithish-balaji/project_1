# Databricks notebook source
v_result = dbutils.notebook.run("DATA_INJESTION", 0, {"p_data_source": "Ergast API"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("DATA_INJESTION1", 0, {"p_data_source": "Ergast API"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("4.ingest_drivers_file", 0, {"p_data_source": "Ergast API"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("5.ingest_results_file", 0, {"p_data_source": "Ergast API"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("6.ingest_pit_stops_file", 0, {"p_data_source": "Ergast API"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("8.ingest_qualifying_file", 0, {"p_data_source": "Ergast API"})

# COMMAND ----------

v_result
