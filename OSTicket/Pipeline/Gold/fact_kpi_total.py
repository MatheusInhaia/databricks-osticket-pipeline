# Databricks notebook source
layer = "gold"
nm_gold_total = "kpi_total"

# COMMAND ----------

# MAGIC %run ../functions

# COMMAND ----------

df_total = spark.sql("""

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
group by year, month, year_month
order by year, month, year_month                
""")
df_total.display()


# COMMAND ----------

write_table(df_total, layer, nm_gold_total)
