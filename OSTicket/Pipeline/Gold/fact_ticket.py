# Databricks notebook source
layer = "gold"
nm_gold = "fact_ticket"
read_layer = "silver"
table = "ticket"

# COMMAND ----------

# MAGIC %run ../functions

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from osticket.silver.ticket
# MAGIC where id_status = 3 and created > '2025-12-01';

# COMMAND ----------

df = read_table(read_layer, table)
df.display()

# COMMAND ----------

write_table(df, layer, nm_gold)
