# Databricks notebook source
layer = "gold"
nm_gold = "dime_staff"
read_layer = "silver"
table = "staff"

# COMMAND ----------

# MAGIC %run ../functions

# COMMAND ----------

df = spark.sql("""
               select id_staff, full_name, email_staff from osticket.silver.staff;
               """)
df.display()

# COMMAND ----------

write_table(df, layer, nm_gold)
