# Databricks notebook source
# MAGIC %md
# MAGIC #data injestion

# COMMAND ----------

storage_account_name = "formul1adls"
storage_account_key  = "qYLNVmVhHGR7H8sSg/hiJz7jxX5zCCfErINRQBkH1YieO4/FZ9fZKOQ73H6YRwsWp2wRVSIT5Qve+AStJWoNEg=="

spark.conf.set(
    f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net",
    f"{storage_account_key}")

container_name = "raw" 
container_name1 = "processed"

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType,DateType
from pyspark.sql.functions import col
from pyspark.sql.functions import current_timestamp,to_timestamp, concat, col, lit

# COMMAND ----------

circuits_schema = StructType(fields=[StructField("circuitId", IntegerType(), False),
                                     StructField("circuitRef", StringType(), True),
                                     StructField("name", StringType(), True),
                                     StructField("location", StringType(), True),
                                     StructField("country", StringType(), True),
                                     StructField("lat", DoubleType(), True),
                                     StructField("lng", DoubleType(), True),
                                     StructField("alt", IntegerType(), True),
                                     StructField("url", StringType(), True)
])

circuits_df = spark.read \
.option("header", True) \
.schema(circuits_schema) \
.csv(f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/circuits.csv")



# COMMAND ----------

circuits_selected_df = circuits_df.select(col("circuitId"), col("circuitRef"), col("name"), col("location"), col("country"), col("lat"), col("lng"), col("alt"))

circuits_renamed_df = circuits_selected_df.withColumnRenamed("circuitId", "circuit_id") \
.withColumnRenamed("circuitRef", "circuit_ref") \
.withColumnRenamed("lat", "latitude") \
.withColumnRenamed("lng", "longitude") \
.withColumnRenamed("alt", "altitude") 

circuits_final_df = circuits_renamed_df.withColumn("ingestion_date", current_timestamp()) 

# COMMAND ----------

circuits_final_df.write.mode("overwrite").parquet(f"abfss://{container_name1}@{storage_account_name}.dfs.core.windows.net/circuits")

# COMMAND ----------

races_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                  StructField("year", IntegerType(), True),
                                  StructField("round", IntegerType(), True),
                                  StructField("circuitId", IntegerType(), True),
                                  StructField("name", StringType(), True),
                                  StructField("date", DateType(), True),
                                  StructField("time", StringType(), True),
                                  StructField("url", StringType(), True) 
])

# COMMAND ----------

races_df = spark.read \
.option("header", True) \
.schema(races_schema) \
.csv(f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/races.csv")

# COMMAND ----------

races_with_timestamp_df = races_df.withColumn("ingestion_date", current_timestamp()) \
                                  .withColumn("race_timestamp", to_timestamp(concat(col('date'), lit(' '), col('time')), 'yyyy-MM-dd HH:mm:ss'))

races_selected_df = races_with_timestamp_df.select(col('raceId').alias('race_id'), col('year').alias('race_year'), 
                                                   col('round'), col('circuitId').alias('circuit_id'),col('name'), col('ingestion_date'), col('race_timestamp'))

# COMMAND ----------

races_selected_df.write.mode('overwrite').partitionBy('race_year').parquet(f"abfss://{container_name1}@{storage_account_name}.dfs.core.windows.net/races")

# COMMAND ----------

constructors_schema = "constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING"

# COMMAND ----------

constructor_df = spark.read \
.schema(constructors_schema) \
.json(f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/constructors.json")

# COMMAND ----------

constructor_dropped_df = constructor_df.drop(col('url'))

constructor_final_df = constructor_dropped_df.withColumnRenamed("constructorId", "constructor_id") \
                                             .withColumnRenamed("constructorRef", "constructor_ref") \
                                             .withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

constructor_final_df.write.mode("overwrite").parquet(f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/constructors")
