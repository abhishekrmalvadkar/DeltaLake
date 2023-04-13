# Databricks notebook source
# DBTITLE 1,Read the sales source data
from pyspark.sql.functions import *
from pyspark.sql.types import *

schema = StructType ([
    StructField("OrderDate",StringType(),True),\
    StructField("Category",StringType(),True),\
    StructField("City",StringType(),True),\
    StructField("Country",StringType(),True),\
    StructField("CustomerName",StringType(),True),\
    StructField("Discount",StringType(),True),\
    StructField("OrderID",StringType(),True),\
    StructField("PostalCode",StringType(),True),\
        StructField("ProductName",StringType(),True),\
])


path ="dbfs:/FileStore/tables/sales1.csv"

df= spark.read.format("csv").option("sep",",").option("header",True).load(path)

# COMMAND ----------

display(df.count())

# COMMAND ----------

# DBTITLE 1,Create the delta table
df.write.format("delta").saveAsTable("salesTable")

# COMMAND ----------

# DBTITLE 1,Enable delta cache
spark.conf.set("spark.databricks.io.cache.enabled","true")

# COMMAND ----------

spark.conf.get("spark.databricks.io.cache.enabled")

# COMMAND ----------

# DBTITLE 1,Select comes from DBFS
# MAGIC %sql
# MAGIC Select * from salestable;

# COMMAND ----------

# DBTITLE 1,Preload and cache entire delta table into local disk
# MAGIC %sql
# MAGIC CACHE select * from salestable;

# COMMAND ----------

# DBTITLE 1,Now select will come from local disk
# MAGIC %sql
# MAGIC select * from salestable;

# COMMAND ----------


