# Databricks notebook source
from concurrent.futures import ThreadPoolExecutor, as_completed
from pyspark.sql import functions as F
from delta.tables import *
from functools import reduce
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
from pytz import timezone
import builtins
import datetime
import json
import re
import requests

# COMMAND ----------

def write_table(df, layer, nm_table):
    """
    Salva um DataFrame como tabela Delta no catálogo Spark.

    Parâmetros:
    df          : DataFrame do Spark
    camada      : str, nome da camada ('bronze', 'silver', 'gold')
    nome_tabela : str, nome da tabela

    Retorna:
    None (apenas imprime o status)
    """
    try:
        df.write.format('delta').mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"osticket.{layer}.{nm_table}")
        print(f"Data successfully saved to osticket.{layer}.{nm_table}")
    except Exception as e:
        print(f"An error occurred while saving the data: {e}")


# COMMAND ----------

def read_table(layer, file):
    df = spark.read.table(f"osticket.{layer}.{file}")
    return df

# COMMAND ----------

def normalize_null(col_name):
    return when(
        (col(col_name).isNull()) |
        (trim(col(col_name)) == "") |
        (trim(col(col_name)) == "None") |
        (trim(col(col_name)) == "null") |
        (trim(col(col_name)) == "NULL"),
        None
    ).otherwise(col(col_name))

# COMMAND ----------

def cast_columns(df, map):
    for c, tipo in map.items():
        df = df.withColumn(
            c,
            normalize_null(c).cast(tipo)
        )
    return df

# COMMAND ----------

def select_columns(p_dataframe_new, p_mapping_columns : dict):
    """Função para selecionar colunas. Utilizada na migração de dados da camada bronze
    para a silver.
    
    Parâmetros
    ----------
    p_dataframe_new : spark.dataframe
        Dataframe com os dados da camada bronze
    p_mapping_columns : dict
        Dicionário onde as chaves são os nomes das colunas a serem selecionadas
    
    Retorna
    -------
    spark.dataframe
        Dataframe apenas com as colunas selecionadas"""
    
    return p_dataframe_new.select([col(c) for c in p_mapping_columns.keys()])

# COMMAND ----------

def rename_columns(p_dataframe_new, p_mapping_columns : dict):
    """Função para renomear colunas. Utilizada na migração de dados da camada bronze
    para a silver.
    
    Parâmetros
    ----------
    p_dataframe_new : spark.dataframe
        Dataframe com as colunas selecionadas da camada bronze
    p_mapping_columns : dict
        Dicionário no formato {'old_column_name':'new_column_name'}
    
    Retorna
    -------
    spark.dataframe
        Dataframe apenas com as colunas renomeadas"""
    
    return p_dataframe_new.select([col(f"`{c}`").alias(p_mapping_columns.get(c,c)) for c in p_dataframe_new.columns])
