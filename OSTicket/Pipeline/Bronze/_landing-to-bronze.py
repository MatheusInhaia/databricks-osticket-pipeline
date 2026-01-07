# Databricks notebook source
layer = "bronze"

# COMMAND ----------

# MAGIC %run ../functions

# COMMAND ----------

dbutils.fs.ls("/Volumes/osticket/raw/files_csv")

# COMMAND ----------

files = [file.name for file in dbutils.fs.ls("/Volumes/osticket/raw/files_csv")]
for table in files:
    print(f"\t{table}")

# COMMAND ----------

for table in files:
    path = f"/Volumes/osticket/raw/files_csv/{table}"
    nm_table = table.replace(".csv", "")

    df_bronze = (
        spark.read.format("csv")
        .option("header", "true")
        .option("delimiter", ";")
        .option("multiLine", "true")
        .option("quote", '"')
        .option("escape", '"')
        .option("mode", "PERMISSIVE")
        .load(path)
    )

    write_table(df_bronze, layer, nm_table)

      
