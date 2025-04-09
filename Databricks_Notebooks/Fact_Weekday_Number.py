# Databricks notebook source
var=dbutils.jobs.taskValues.get(taskKey="FactTable_RunSchedule",Key="weekoutput")

# COMMAND ----------

print(var)
