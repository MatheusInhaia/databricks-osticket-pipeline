# Databricks notebook source
layer = "gold"
nm_gold = "fact_ticket"
read_layer = "silver"
table = "ticket"

# COMMAND ----------

# MAGIC %run ../functions

# COMMAND ----------

df = read_table(read_layer, table)
df.display()

# COMMAND ----------

write_table(df, layer, nm_gold)
