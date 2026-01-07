# Databricks notebook source
new_layer = "silver"
layer = "bronze"
table = "ost_staff"
new_table = "staff"

# COMMAND ----------

# MAGIC %run ../functions

# COMMAND ----------

df_bronze = read_table(layer, table)
df_bronze.display()


# COMMAND ----------

mapping_columns = {
    "staff_id": "id_staff",
    "username": "user_name",
    "firstname": "first_name",
    "lastname": "last_name",
    "email": "email_staff"
}

new_df_bronze = select_columns(df_bronze, mapping_columns)
new_df_bronze = rename_columns(new_df_bronze, mapping_columns)

new_df_bronze = new_df_bronze.withColumn(
    "full_name",
    concat(col("first_name"), lit(" "), col("last_name"))
)

new_df_bronze = new_df_bronze.drop("first_name", "last_name")

new_df_bronze.printSchema()

# COMMAND ----------

cast_map = {
    "id_staff": "int",  
    }

new_df_bronze = cast_columns(new_df_bronze, cast_map)
new_df_bronze.printSchema()

# COMMAND ----------

write_table(new_df_bronze, new_layer, new_table)
