# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE osticket.gold.dim_data AS
# MAGIC WITH limites AS (
# MAGIC     SELECT
# MAGIC         date_trunc('month', min(created)) AS min_data,
# MAGIC         date_trunc('month', max(created)) AS max_data
# MAGIC     FROM osticket.silver.thread_entry
# MAGIC ),
# MAGIC datas AS (
# MAGIC     SELECT explode(
# MAGIC         sequence(
# MAGIC             min_data,
# MAGIC             max_data,
# MAGIC             interval 1 month
# MAGIC         )
# MAGIC     ) AS data
# MAGIC     FROM limites
# MAGIC )
# MAGIC SELECT
# MAGIC     data                           AS date,
# MAGIC     year(data)                     AS year,
# MAGIC     month(data)                    AS month_num,
# MAGIC     date_format(data, 'MMM')       AS month_name,
# MAGIC     date_format(data, 'MMMM')      AS month_full_name,
# MAGIC     date_format(data, 'yyyy-MM')   AS ano_month,
# MAGIC     year(data) * 100 + month(data) AS id_data
# MAGIC FROM datas
# MAGIC ORDER BY data;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from osticket.gold.dim_data;

# COMMAND ----------


