# Databricks notebook source
layer = "gold"
nm_gold = "dime_ticket_status"
read_layer = "silver"
table = "ticket_status"

# COMMAND ----------

# MAGIC %run ../functions

# COMMAND ----------

df = spark.sql("""
               select * from osticket.silver.ticket_status
               """)
df.display()

# COMMAND ----------

write_table(df, layer, nm_gold)
