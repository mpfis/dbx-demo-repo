# Databricks notebook source
df = spark.range(1000)
df = df.selectExpr("id", "(id * 1000) as pid")
df = df.selectExpr("id", "pid", "(pid * 2) as pid2")
df = df.selectExpr("id", "pid", "pid2", "(pid/pid2) as quotient")
df.head(10)
