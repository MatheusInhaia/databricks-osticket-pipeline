# Databricks notebook source
layer = "gold"
nm_gold = "kpi_ticket_staff"

# COMMAND ----------

# MAGIC %run ../functions

# COMMAND ----------

df = spark.sql("""
    select
        t.id_staff,
        (year(created) * 100 + month(created)) as year_month,
        year(to_date(t.created, 'dd/mm/yyyy')) as year,
        month(to_date(t.created, 'dd/mm/yyyy')) as month,
        count(*) as total,

        sum(case when t.id_status = 2  then 1 else 0 end) as resolved,
        sum(case when t.id_status  = 3  then 1 else 0 end) as closed,
        sum(case when t.id_status = 1  then 1 else 0 end) as open

    from osticket.silver.ticket t
    left join osticket.silver.staff s
        on t.id_staff = s.id_staff
    group by
        t.id_staff,
        year,
        month,
        year_month
    order by
        t.id_staff,
        year_month
               """)
df.display()               

# COMMAND ----------

write_table(df, layer, nm_gold)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from osticket.silver.ticket
# MAGIC where created >= '2025-11-01' and created < '2025-12-1' and id_staff = 2 and subject = 'Julia mencionou DB'

# COMMAND ----------


