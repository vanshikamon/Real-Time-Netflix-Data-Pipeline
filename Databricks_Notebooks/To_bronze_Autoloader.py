# Databricks notebook source
# MAGIC %md
# MAGIC Incremental Data Loading using AutoLoader

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA neflixdata_catalog.netflix_schema;

# COMMAND ----------

checkpoint_path = "abfss://silver@netflixdatastore.dfs.core.windows.net/checkpoint"

# COMMAND ----------

df=spark.readStream\
    .format("cloudFiles")\
    .option("cloudFiles.format","csv")\
    .option("cloudFiles.schemalocation",checkpoint_path)\
        .load("abfss://raw@netflixdatastore.dfs.core.windows.net")


# COMMAND ----------

display(df)

# COMMAND ----------

df.writeStream\
    .option("checkpointLocation", checkpoint_path)\
        .trigger(processingTime="10 seconds")\
            .start("abfss://bronze@netflixdatastore.dfs.core.windows.net/netflix_titles")