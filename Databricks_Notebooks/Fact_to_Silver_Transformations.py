# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import * 

# COMMAND ----------

df=spark.read.format("delta")\
    .option("header",True)\
        .option("inferSchema",True)\
            .load("abfss://bronze@netflixdatastore.dfs.core.windows.net/netflix_titles")

# COMMAND ----------

df=df.fillna({"duration_minutes":0,"duration_seasons":1})

# COMMAND ----------

df=df.withColumn("duration_minutes",df["duration_minutes"].cast(IntegerType()))
df=df.withColumn("duration_seasons",df["duration_seasons"].cast(IntegerType()))

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df=df.withColumn("Shorttitle",split(col("title"),":")[0])
df.display()

# COMMAND ----------

df=df.withColumn("rating",split(col('rating'),"-")[0])
df.display()

# COMMAND ----------

df=df.withColumn("type_flag",when(col('type')=='Movie',1)\
    .when(col('type')=='TV Show',2)\
        .otherwise(0))

# COMMAND ----------

from pyspark.sql.window import Window

# COMMAND ----------

df=df.withColumn("duration_ranking",dense_rank().over(Window.orderBy(col("duration_minutes").desc())))
df.display()

# COMMAND ----------

df1=df.groupBy("type").agg(count("*").alias("total_count"))
display(df1)

# COMMAND ----------

df.write.format("delta")\
    .mode("overwrite")\
        .option("path","abfss://silver@netflixdatastore.dfs.core.windows.net/netflix_titles")\
            .save()
