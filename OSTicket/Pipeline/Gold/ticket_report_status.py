# Databricks notebook source
layer = "gold"
nm_gold = "kpi_ticket"

# COMMAND ----------

# MAGIC %run ../functions

# COMMAND ----------

df = spark.sql("""
    select 
        (year(created) * 100 + month(created)) as year_month,
        year(to_date(t.created, 'dd/mm/yyyy')) as year,
        month(to_date(t.created, 'dd/mm/yyyy')) as month,
        count(*) as total,

        sum(case when t.id_status = 2  then 1 else 0 end) as resolved,
        sum(case when t.id_status  = 3  then 1 else 0 end) as closed
    
    from osticket.silver.ticket t
    left join osticket.silver.ticket_status ts
        on t.id_status = ts.id_ticket_status
    group by
        year_month,
        year,
        month
    order by
        year_month
""")
df.display()

# COMMAND ----------

write_table(df, layer, nm_gold)
