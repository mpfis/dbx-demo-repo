# Databricks notebook source
from pyspark.sql.functions import *
df = spark.range(1000)
df = df.selectExpr("id", "(id * 1000) as pid")
df = df.selectExpr("id", "pid", "(pid * 2) as pid2")
df = df.selectExpr("id", "pid", "pid2", "(pid/pid2) as quotient")
print("hello")
print("goodbye")
print("Hi Jess!")
df2 = df.withColumn("id3", col("id")*3)
df.head(10)

dbutils.fs.ls("/")