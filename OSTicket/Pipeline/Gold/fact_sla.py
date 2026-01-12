# Databricks notebook source
spark.sql("""
            create or replace table osticket.gold.fact_sla as
            select
                year(created)  as year,
                month(created) as month,
                (year(created) * 100 + month(created)) as year_month,

                count(staff_first_resp) as total_tickets,

                sum(
                    case when resp_time <= 1440 then 1 else 0 end
                ) as sla_24h,

                round(
                    sla_24h
                    / total_tickets,
                    4
                ) as perc_sla_24h

            from osticket.silver.thread_entry_agg
            group by
                year(created),
                month(created),
                year_month
            order by
                year(created),
                month(created),
                year_month;
          """)

# COMMAND ----------


