# Databricks notebook source
new_layer = "silver"
layer = "bronze"
table = "ost_ticket"
new_table = "ticket"

# COMMAND ----------

# MAGIC %run ../functions

# COMMAND ----------

df_bronze = spark.sql("""
                      select ost.*, cdata.subject from osticket.bronze.ost_ticket as ost
                      left join osticket.bronze.ost_ticket_cdata as cdata
                      on ost.ticket_id = cdata.ticket_id
                      """)
df_bronze.display()

# COMMAND ----------

mapping_columns = {
    "ticket_id": "id_ticket",
    "subject": "subject",
    "status_id": "id_status",
    "staff_id": "id_staff",
    "reopened": "reopened",
    "closed": "closed",
    "created": "created"
}

new_df_bronze = select_columns(df_bronze, mapping_columns)
new_df_bronze = rename_columns(new_df_bronze, mapping_columns)

new_df_bronze.printSchema()

# COMMAND ----------

cast_map = {
    "id_ticket": "int",
    "id_status": "int",
    "id_staff": "int",
    "reopened": "timestamp",
    "closed": "timestamp",
    "created": "timestamp"
}

new_df_bronze = cast_columns(new_df_bronze, cast_map)
new_df_bronze.printSchema()

# COMMAND ----------

new_df_bronze = new_df_bronze.withColumn(
    "closed_time",
    (
        F.col("closed").cast("long") - F.coalesce(F.col("created").cast("long"))
    ) / 60
)

# COMMAND ----------

df_clean = new_df_bronze.withColumn(
    "subject",
    F.regexp_replace(
            F.col("subject"),
            r"(?i)^((re|res|enc):\s*)+",
            ""
))
df_clean.display()

# COMMAND ----------

write_table(df_clean, new_layer, new_table)
