# Databricks notebook source
# MAGIC %md
# MAGIC # **Rodar o pipeline de dados do OSTicket**
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC - ### **Bronze**

# COMMAND ----------

# MAGIC %run ../Pipeline/Bronze/_landing-to-bronze

# COMMAND ----------

# MAGIC %md
# MAGIC -  ### Silver

# COMMAND ----------

# MAGIC %run ../Pipeline/Silver/ost_staff

# COMMAND ----------

# MAGIC %run ../Pipeline/Silver/ost_thread_entry

# COMMAND ----------

# MAGIC %run ../Pipeline/Silver/ost_thread_entry_agg

# COMMAND ----------

# MAGIC %run ../Pipeline/Silver/ost_ticket_status

# COMMAND ----------

# MAGIC %run ../Pipeline/Silver/ost_ticket

# COMMAND ----------

# MAGIC %md
# MAGIC - ### **Gold**

# COMMAND ----------

# MAGIC %run ../Pipeline/Gold/dime_staff

# COMMAND ----------

# MAGIC %run ../Pipeline/Gold/dime_ticket_status

# COMMAND ----------

# MAGIC %run ../Pipeline/Gold/dime_data

# COMMAND ----------

# MAGIC %run ../Pipeline/Gold/fact_ticket

# COMMAND ----------

# MAGIC %run ../Pipeline/Gold/fact_thread_entry_agg

# COMMAND ----------

# MAGIC %run ../Pipeline/Gold/fact_kpi_staff

# COMMAND ----------

# MAGIC %run ../Pipeline/Gold/fact_kpi_total

# COMMAND ----------

# MAGIC %run ../Pipeline/Gold/kpi_sla_mensal

# COMMAND ----------

# MAGIC %run ../Pipeline/Gold/ticket_report_staff

# COMMAND ----------

# MAGIC %run ../Pipeline/Gold/ticket_report_status

# COMMAND ----------

# MAGIC %md
# MAGIC Pipeline finalizado.
