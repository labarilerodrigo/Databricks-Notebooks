# Databricks notebook source
# MAGIC %md
# MAGIC # Loop through all existing tables in a database, and insert the data from each table into table in target db

# COMMAND ----------

from pyspark.sql.types import *

DatabaseDF = "devl_br9_pcm_ppp_pilot"
# df = spark.sql(f"show Tables FROM {DatabaseDF} like 'pcm_cpp_prices_sub_natl'")
df = spark.sql(f"show Tables FROM {DatabaseDF}")

df = df.select("TableName")
list = [x["TableName"] for x in df.collect()]

## Iterate through list of schema
for x in list:
    tempTable = x
    src_table = "la_bdf_devl.devl_br9_pcm_ppp_pilot." + tempTable
    dst_table = "la_bdf_devl.devl_br9_pcm_ppp_pilot_02." + tempTable
    dst_truncate_table = "truncate table " + dst_table

    # print(src_table)
    # print(dst_table)

    # truncate data
    spark.sql(dst_truncate_table)
    # print(dst_truncate_table)

    # populate dst table from src db
    insert_qry = "insert into " + dst_table + " select * from " + src_table
    spark.sql(insert_qry)
    # print(insert_qry)

# COMMAND ----------


