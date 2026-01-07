# Databricks notebook source
layer = "gold"
nm_gold_staff = "kpi_staff"


# COMMAND ----------

# MAGIC %run ../functions

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH limites AS (
# MAGIC   SELECT
# MAGIC     percentile_approx(resp_time, 0.15) AS p15,
# MAGIC     percentile_approx(resp_time, 0.85) AS p85
# MAGIC   FROM osticket.gold.fact_thread_ticket_agg
# MAGIC )
# MAGIC SELECT
# MAGIC   AVG(resp_time) AS avg_resp_time_sem_outliers
# MAGIC FROM osticket.gold.fact_thread_ticket_agg f
# MAGIC CROSS JOIN limites l
# MAGIC WHERE f.resp_time BETWEEN l.p15 AND l.p85;

# COMMAND ----------

# MAGIC %sql
# MAGIC with percent as (
# MAGIC   select
# MAGIC     percentile_approx(resp_time, 0.25) as q1,
# MAGIC     percentile_approx(resp_time, 0.75) as q3,
# MAGIC     percentile_approx(resp_time, 0.75)
# MAGIC       + 1.5 * (
# MAGIC         percentile_approx(resp_time, 0.75)
# MAGIC         - percentile_approx(resp_time, 0.25)
# MAGIC       ) as percent_limit
# MAGIC   from osticket.silver.thread_entry_agg
# MAGIC ),
# MAGIC
# MAGIC filtered_base as (
# MAGIC   select
# MAGIC     t.id_staff,
# MAGIC     t.resp_time,
# MAGIC     year(created) as year,
# MAGIC     month(created) as month,
# MAGIC     (year(created) * 100 + month(created)) as year_month
# MAGIC   from osticket.silver.thread_entry_agg t
# MAGIC   cross join percent p
# MAGIC   where t.resp_time <= p.percent_limit
# MAGIC   and t.id_staff is not null
# MAGIC )
# MAGIC
# MAGIC select
# MAGIC   year_month,
# MAGIC   id_staff,
# MAGIC   year,
# MAGIC   month,
# MAGIC   median(resp_time) as median_time,
# MAGIC   floor(median_time/60) as hours,
# MAGIC   floor(mod(median_time, 60)) as minutes,
# MAGIC   concat(
# MAGIC   lpad(cast(hours as string), 2, '0'),
# MAGIC   ':',
# MAGIC   lpad(cast(minutes as string), 2, '0')
# MAGIC ) as hours_minutes
# MAGIC from filtered_base
# MAGIC group by id_staff, year, month, year_month
# MAGIC order by id_staff, year, month, year_month

# COMMAND ----------

# MAGIC %md
# MAGIC Decisão de uso da mediana e IQR:
# MAGIC -
# MAGIC -
# MAGIC Aqui foi decidio o uso de mediana ao uso de média, como o tempo de resposta em alguns chamados pode alterar de forma muito significativa por n motivos, assim pegaremos um valor central do tempo de resposta. Pra finalizar e dar um contexto mais real do tempo de resposta de cada staff, resolvi usar o método IQR para remover da contagem valores muito descrepantes, mesmo que a mediana já desse uma valor central, removendo esses outliers temos um valor mais acertivo para o nosso contexto.
# MAGIC

# COMMAND ----------

df_staff = spark.sql("""
with percent as (
  select
    percentile_approx(resp_time, 0.25) as q1,
    percentile_approx(resp_time, 0.75) as q3,
    percentile_approx(resp_time, 0.75)
      + 1.5 * (
        percentile_approx(resp_time, 0.75)
        - percentile_approx(resp_time, 0.25)
      ) as percent_limit
  from osticket.silver.thread_entry_agg
),

filtered_base as (
  select
    t.id_staff,
    t.resp_time,
    year(created) as year,
    month(created) as month,
    (year(created) * 100 + month(created)) as year_month
  from osticket.silver.thread_entry_agg t
  cross join percent p
  where t.resp_time <= p.percent_limit
  and t.id_staff is not null
)

select
  year_month,
  id_staff,
  year,
  month,
  median(resp_time) as median_time,
  floor(median_time/60) as hours,
  floor(mod(median_time, 60)) as minutes,
  concat(
  lpad(cast(hours as string), 2, '0'),
  ':',
  lpad(cast(minutes as string), 2, '0')
) as hours_minutes
from filtered_base
group by id_staff, year, month, year_month
order by id_staff, year, month, year_month
""")
df_staff.display()


# COMMAND ----------

write_table(df_staff, layer, nm_gold_staff)
