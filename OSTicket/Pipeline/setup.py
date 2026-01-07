# Databricks notebook source
nm_project = "osticket"
base_path = f"/FileStore/datalake/{nm_project}"

spark.sql(f"create catalog if not exists {nm_project}")
spark.sql(f"use catalog {nm_project}")

for layer in ["raw", "bronze", "silver", "gold"]:
    spark.sql(f"create schema if not exists {layer}")
    spark.sql(f"create volume if not exists {layer}")
    


