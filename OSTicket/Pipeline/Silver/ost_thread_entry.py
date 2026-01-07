# Databricks notebook source
new_layer = "silver"
layer = "bronze"
table = "ost_thread_entry"
new_table = "thread_entry"

# COMMAND ----------

# MAGIC %run ../functions

# COMMAND ----------

df_bronze = read_table(layer, table)
df_bronze.display()

# COMMAND ----------

mapping_columns = {
    "id": "id_thread_entry",
    "pid":"pid_thread_entry",
    "thread_id": "id_thread",
    "staff_id": "id_staff",
    "user_id": "id_user",
    "poster": "poster",
    "title": "title",
    "body": "body",
    "created": "created"
}

new_df_bronze = select_columns(df_bronze, mapping_columns)
new_df_bronze = rename_columns(new_df_bronze, mapping_columns)

new_df_bronze.printSchema()

# COMMAND ----------

cast_map = {
    "id_thread_entry": "int",
    "pid_thread_entry": "int",
    "id_thread": "int",
    "id_staff": "int",
    "id_user": "int",
    "created": "timestamp"
}

df_silver = new_df_bronze.filter(col("id").rlike("^[0-9]+$"))

new_df_bronze = cast_columns(df_silver, cast_map)
new_df_bronze.printSchema()

# COMMAND ----------


#from pyspark.sql import functions as F está importado no notebook functions
df_clean = new_df_bronze.withColumn(
    "body",
    F.regexp_replace("body", "<[^>]*>", "")    # remove todas as tags <...>
).withColumn(
    "title",
    F.regexp_replace(
            F.col("title"),
            r"(?i)^((re|res|enc):\s*)+",
            ""      
))

df_clean.display()

# COMMAND ----------

write_table(df_clean, new_layer, new_table)
