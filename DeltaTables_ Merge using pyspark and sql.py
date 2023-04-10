# Databricks notebook source
# DBTITLE 1,PySpark Merge (UPSERT) statement
from pyspark.sql.functions import * 
from pyspark.sql.types import *

schema  = StructType([
    StructField("emp_id",IntegerType(),True),
    StructField("name",StringType(),True),
    StructField("city",StringType(),True),
    StructField("country",StringType(),True),
    StructField("contact_no",IntegerType(),True),
])

data = [(1,'NameOne','Bengaluru','India',843131303)]

df = spark.createDataFrame(data=data,schema=schema)

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC CREATE OR REPLACE TABLE dim_emp (
# MAGIC   emp_id int,
# MAGIC   name String,
# MAGIC   city String,
# MAGIC   country String,
# MAGIC   contact_no int
# MAGIC 
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION "/FileStore/tables/DeltaOne"

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dim_emp

# COMMAND ----------

# DBTITLE 1,Method 1: Spark Sql
df.createOrReplaceTempView("source_view")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dim_emp

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * from source_view

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC MERGE INTO dim_emp as target
# MAGIC USING source_view as source
# MAGIC   ON target.emp_id = source.emp_id
# MAGIC   WHEN MATCHED
# MAGIC THEN UPDATE SET
# MAGIC   target.city = source.city,
# MAGIC   target.country = source.country,
# MAGIC   target.contact_no = source.contact_no,
# MAGIC   target.name = source.name
# MAGIC WHEN NOT MATCHED
# MAGIC THEN 
# MAGIC INSERT (emp_id,name,city,country,contact_no) VALUES (emp_id,name,city,country,contact_no)

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT * from dim_emp

# COMMAND ----------

data = [(1,'NameOne','Mumbai','India',843131303),(2,'NameTwo','Mumbai','India',993131303)]

df = spark.createDataFrame(data=data,schema=schema)

# COMMAND ----------

display(df)

# COMMAND ----------

df.createOrReplaceTempView("source_view")

# COMMAND ----------

# MAGIC %sql
# MAGIC Select * from source_view

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC MERGE INTO dim_emp as target
# MAGIC USING source_view as source
# MAGIC   ON target.emp_id = source.emp_id
# MAGIC   WHEN MATCHED
# MAGIC THEN UPDATE SET
# MAGIC   target.city = source.city,
# MAGIC   target.country = source.country,
# MAGIC   target.contact_no = source.contact_no,
# MAGIC   target.name = source.name
# MAGIC WHEN NOT MATCHED
# MAGIC THEN 
# MAGIC INSERT (emp_id,name,city,country,contact_no) VALUES (emp_id,name,city,country,contact_no)

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT * from dim_emp

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT * from source_view

# COMMAND ----------

# DBTITLE 1,Method 2 : PySpark
data = [(3,'NameThree','Gulbarga','India',843131303),(4,'NameFour','Pune','India',993131303)]

df = spark.createDataFrame(data=data,schema=schema)

# COMMAND ----------

from delta.tables import *
delta_df = DeltaTable.forPath(spark,'/FileStore/tables/DeltaOne')

# COMMAND ----------

delta_df.alias("target").merge(
    source=df.alias("source"),
    condition="target.emp_id = source.emp_id"
).whenMatchedUpdate(set = 
    {
        "name":"source.name",
        "city":"source.city",
        "country":"source.country",
        "contact_no":"source.contact_no"
    }
).whenNotMatchedInsert(values =
        {
            "emp_id":"source.emp_id",
            "name":"source.name",
            "city":"source.city",
            "country":"source.country",
            "contact_no":"source.contact_no"
        }
).execute()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dim_emp

# COMMAND ----------

 
