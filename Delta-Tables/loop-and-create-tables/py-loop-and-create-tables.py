# Databricks notebook source
# MAGIC %md
# MAGIC # Loop through all existing tables in a database, get table definitions and create them in a target database

# COMMAND ----------

from pyspark.sql.types import *

DatabaseDF = "devl_br9_pcm_ppp_pilot"
#df = spark.sql(f"show Tables FROM {DatabaseDF} like 'pcm_cpp_prices_sub_natl'")
df = spark.sql(f"show Tables FROM {DatabaseDF}")

df = df.select("TableName")
list = [x["TableName"] for x in df.collect()]

## Iterate through list of schema
for x in list:
    tempTable = x
    src_table="la_bdf_devl.devl_br9_pcm_ppp_pilot."+ tempTable
    dst_if_exists_table="drop table if exists la_bdf_devl.devl_br9_pcm_ppp_pilot_02."+ tempTable
    
    # drop table if exists
    spark.sql(dst_if_exists_table)
    
    # create table in new db
    query=spark.sql("show create table "+src_table)
    y=query.head()
    qry_createTable=y["createtab_stmt"].replace("devl_br9_pcm_ppp_pilot","devl_br9_pcm_ppp_pilot_02")
    print(qry_createTable)
    spark.sql(qry_createTable)


# COMMAND ----------


