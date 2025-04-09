# Databricks notebook source
dbutils.widgets.text("weekday", "0")
var=int(dbutils.widgets.get("weekday"))
dbutils.jobs.taskValues.set(key="weekoutput",value= var)