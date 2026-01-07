# Databricks notebook source
new_layer = "silver"
layer = "bronze"
table = "ost_ticket_status"
new_table = "ticket_status"

# COMMAND ----------

# MAGIC %run ../functions

# COMMAND ----------

df_bronze = read_table(layer, table)
df_bronze.display()

# COMMAND ----------

mapping_columns = {
    "id": "id_ticket_status",
    "name": "name_ticket_status",
    "state": "state_ticket_status"
}

new_df_bronze = select_columns(df_bronze, mapping_columns)
new_df_bronze = rename_columns(new_df_bronze, mapping_columns)

new_df_bronze.printSchema()

# COMMAND ----------

cast_map = {
    "id_ticket_status": "int",
}

new_df_bronze = cast_columns(new_df_bronze, cast_map)
new_df_bronze.printSchema()

# COMMAND ----------

new_df_bronze = new_df_bronze.select('id_ticket_status', 'name_ticket_status')

# COMMAND ----------

write_table(new_df_bronze, new_layer, new_table)
