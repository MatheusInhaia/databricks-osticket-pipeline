# Databricks notebook source
spark.sql("""
            create or replace table osticket.gold.dim_date as
            with limits as (
                select
                    date_trunc('month', min(created)) AS min_date,
                    date_trunc('month', max(created)) AS max_date
                from osticket.silver.thread_entry
            ),
            dates as (
                select explode(
                    sequence(
                        min_date,
                        max_date,
                        interval 1 month
                    )
                ) as date
                from limits
            )
            select
                date                           as date,
                year(date)                     as year,
                month(date)                    as month_num,
                date_format(date, 'MMM')       as month_name,
                date_format(date, 'MMMM')      as month_full_name,
                date_format(date, 'yyyy-MM')   as ano_month,
                year(date) * 100 + month(date) as id_data
            from dates
            order by date;
          """)

# COMMAND ----------


