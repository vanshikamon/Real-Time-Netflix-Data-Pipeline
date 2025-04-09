# Databricks notebook source
# MAGIC %md
# MAGIC Silver Notebook Lookup Tables

# COMMAND ----------

# MAGIC %md
# MAGIC PARAMETERS

# COMMAND ----------

dbutils.widgets.text("sourcefolder","netflix_directors")
dbutils.widgets.text("targetfolder","netflix_directors")

# COMMAND ----------

# MAGIC %md
# MAGIC VARIABLES

# COMMAND ----------

var_src_folder=dbutils.widgets.get("sourcefolder")
var_trg_folder=dbutils.widgets.get("targetfolder")

# COMMAND ----------

df=spark.read.format("csv")\
    .option("header","true")\
        .option("inferSchema","true")\
        .load(f"abfss://bronze@netflixdatastore.dfs.core.windows.net/{var_src_folder}")

# COMMAND ----------

df.display()

# COMMAND ----------

df.write.format("delta")\
    .mode("append")\
        .option("mergeSchema","true")\
        .option(f"path","abfss://silver@netflixdatastore.dfs.core.windows.net/{var_trg_folder}")\
            .save()