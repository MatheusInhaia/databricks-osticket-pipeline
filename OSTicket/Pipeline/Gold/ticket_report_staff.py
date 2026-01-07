# Databricks notebook source
layer = "gold"
nm_gold = "kpi_ticket_staff"

# COMMAND ----------

# MAGIC %run ../functions

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC         t.id_staff,
# MAGIC         (year(created) * 100 + month(created)) AS year_month,
# MAGIC         year(to_date(t.created, 'dd/mm/yyyy')) as year,
# MAGIC         month(to_date(t.created, 'dd/mm/yyyy')) as month,
# MAGIC         count(*) as total,
# MAGIC
# MAGIC         sum(case when t.id_status = 2  then 1 else 0 end) as resolved,
# MAGIC         sum(case when t.id_status = 3  then 1 else 0 end) as closed,
# MAGIC         sum(case when t.id_status = 1  then 1 else 0 end) as open
# MAGIC
# MAGIC     from osticket.silver.ticket t
# MAGIC     left join osticket.silver.staff s
# MAGIC         on t.id_staff = s.id_staff
# MAGIC     group by
# MAGIC         t.id_staff,
# MAGIC         year,
# MAGIC         month,
# MAGIC         year_month
# MAGIC     order by
# MAGIC         t.id_staff,
# MAGIC         year_month
# MAGIC                

# COMMAND ----------

df = spark.sql("""
    select
        t.id_staff,
        (year(created) * 100 + month(created)) AS year_month,
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
