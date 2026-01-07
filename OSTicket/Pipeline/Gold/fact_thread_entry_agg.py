# Databricks notebook source
layer = "gold"
nm_gold = "fact_thread_ticket_agg"

# COMMAND ----------

# MAGIC %run ../functions

# COMMAND ----------

df = spark.sql("""
              select * from osticket.silver.thread_entry_agg
               """)

# COMMAND ----------

df.display()

# COMMAND ----------

write_table(df, layer, nm_gold)
