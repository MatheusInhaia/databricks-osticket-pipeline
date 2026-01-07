# Databricks notebook source
new_layer = "silver"
new_table = "thread_entry_agg"

# deve ser executada somente após a execução da ost_thread_entry e ost_staff, pois ela cria uma tabela agregada da thread_entry por chamado

# COMMAND ----------

# MAGIC %run ../functions

# COMMAND ----------

df = spark.sql("""
                with p_id as (
                select *
                from osticket.silver.thread_entry
                where pid_thread_entry = 0
            ),
            thread_ticket as (
                select 
                    pid_thread_entry as id_pid,
                    id_thread_entry as id_t,
                    poster as user,
                    created as dt_email,
                    body as msg
                from osticket.silver.thread_entry
                where pid_thread_entry <> 0
            )
            select 
                id_thread_entry,
                id_thread,
                title,
                poster,
                created,
                body as msg,
                collect_list(
                    named_struct(
                        'id_thread_entry', t.id_t,
                        'pid', t.id_t,
                        'poster', t.user,
                        'dt_email', t.dt_email,
                        'email', t.msg
                    )
                
                )
                as thread
            from p_id as p
            left join thread_ticket as t 
                on p.id_thread_entry = t.id_pid 
            group by 
                id_thread_entry,
                id_thread,
                title,
                poster,
                created,
                body
            order by id_thread_entry
               """)

# COMMAND ----------

staff_array = (
    spark.table("osticket.silver.staff")
         .select(F.collect_set("full_name").alias("staffs"))
         .first()["staffs"]
)

# COMMAND ----------

staffs_lit = F.array(*[F.lit(s) for s in staff_array])

df = df.withColumn(
    "thread_staff",
    F.filter(
        F.col("thread"),
        lambda x: F.array_contains(staffs_lit, x["poster"])
    )
)

df = df.withColumn(
    "staff_first_resp",
    F.get(F.col("thread_staff"), 0).getField("poster")
)

#df = df.withColumn("tempo_atendimento_minutos", F.datediff(F.col("created_date"), F.col("thread")[0].date))
df = df.withColumn(
    "resp_time",
    (
        F.expr("get(thread_staff, 0).dt_email").cast("long") - F.col("created").cast("long")
       
    ) / 60)


# COMMAND ----------

df = df.alias("d").join(spark.table("osticket.silver.staff").alias("s"), F.col("d.staff_first_resp") == F.col("s.full_name"), "left").select("d.*", F.col("s.id_staff"))


# COMMAND ----------

df.display()

# COMMAND ----------

write_table(df, new_layer, new_table)
