# Databricks notebook source
df = spark.range(1000)
df = df.selectExpr("id", "(id * 1000) as pid")
df.head(10)
