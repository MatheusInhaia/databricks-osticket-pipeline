# Databricks notebook source
# MAGIC %sql
# MAGIC create or replace table osticket.gold.kpi_sla_mensal as
# MAGIC select
# MAGIC     year(created)  as year,
# MAGIC     month(created) as month,
# MAGIC     (year(created) * 100 + month(created)) as year_month,
# MAGIC
# MAGIC     count(staff_first_resp) as total_tickets,
# MAGIC
# MAGIC     sum(
# MAGIC         case when resp_time <= 1440 then 1 else 0 end
# MAGIC     ) as sla_24h,
# MAGIC
# MAGIC     round(
# MAGIC         sla_24h
# MAGIC         / total_tickets,
# MAGIC         4
# MAGIC     ) as perc_sla_24h
# MAGIC
# MAGIC from osticket.silver.thread_entry_agg
# MAGIC group by
# MAGIC     year(created),
# MAGIC     month(created),
# MAGIC     year_month
# MAGIC order by
# MAGIC     year(created),
# MAGIC     month(created),
# MAGIC     year_month;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from osticket.gold.kpi_sla_mensal

# COMMAND ----------


