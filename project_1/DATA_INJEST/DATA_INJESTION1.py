# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType
lap_times_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                      StructField("driverId", IntegerType(), True),
                                      StructField("lap", IntegerType(), True),
                                      StructField("position", IntegerType(), True),
                                      StructField("time", StringType(), True),
                                      StructField("milliseconds", IntegerType(), True)
                                     ])

# COMMAND ----------

lap_times_df = spark.read \
.schema(lap_times_schema) \
.csv(f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/lap_times")

# COMMAND ----------

lap_times_df.count()

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

final_df = lap_times_df.withColumnRenamed("driverId", "driver_id") \
.withColumnRenamed("raceId", "race_id") \
.withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

final_df.write.mode("overwrite").parquet(f"abfss://{container_name1}@{storage_account_name}.dfs.core.windows.net/lap_times")

# COMMAND ----------

# MAGIC %run "/project_1/DATA_INJEST//END"
