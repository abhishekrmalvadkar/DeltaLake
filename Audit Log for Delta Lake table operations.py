# Databricks notebook source
from pyspark.sql.functions import * 
from pyspark.sql.types import *

schema  = StructType([
    StructField("emp_id",IntegerType(),True),
    StructField("name",StringType(),True),
    StructField("city",StringType(),True),
    StructField("country",StringType(),True),
    StructField("contact_no",IntegerType(),True),
])

data = [(1,'One','Bengaluru','India',843131303),(2,'Two','Bengaluru','India',843131303),(3,'Three','Mumbai','India',843131303),(4,'Four','Bengaluru','India',843131303),(5,'Five','Bengaluru','India',843131303)]

df = spark.createDataFrame(data=data,schema=schema)

# COMMAND ----------

display(df)

# COMMAND ----------

df.write.format("delta").mode("append").saveAsTable("auditLog")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from auditlog

# COMMAND ----------

from delta.tables import *

DeltaTable.create(spark).tableName("auditLog").\
addColumn("emp_id","integer").\
addColumn("name","string").\
addColumn("city","string").\
addColumn("country","string").\
addColumn("contact_no","integer").\
property("description","My first Delta Table").\
location("/FileStore/tables/target/auditLog/delta/").execute()

# COMMAND ----------

# MAGIC %sql
# MAGIC update auditlog set city = "Gulbarga" where emp_id = 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from auditlog

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS audit_log(
# MAGIC   operation STRING,
# MAGIC   updated_time timestamp,
# MAGIC   userName STRING,
# MAGIC   notebookName STRING,
# MAGIC   numTargetRowsUpdated INT,
# MAGIC   numTargetRowsInserted INT,
# MAGIC   numTargetRowsDeleted INT
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from audit_log

# COMMAND ----------

# DBTITLE 1,create DF with last operation in delta lake
from delta.tables import *
delta_df = DeltaTable.forName(spark,'auditlog')

lastOperationDf = delta_df.history(1)
display(lastOperationDf)

# COMMAND ----------

# DBTITLE 1,Expand  operationMetrics columns
explode_df = lastOperationDf.select(lastOperationDf.operation, explode(lastOperationDf.operationMetrics))
explode_df_select = explode_df.select(explode_df.operation,explode_df.key,explode_df.value.cast('INT'))
display(explode_df_select)

# COMMAND ----------

# DBTITLE 1,Pivot operation to convert rows to columns 
pivot_df = explode_df_select.groupBy("operation").pivot("key").sum("value")
display(pivot_df)

# COMMAND ----------

# DBTITLE 1,Selecting only req columns for audit_log table
pivot_df_select = pivot_df.select(pivot_df.operation,pivot_df.numUpdatedRows,pivot_df.numAddedFiles,pivot_df.numRemovedFiles)
display(pivot_df_select)

# COMMAND ----------

# DBTITLE 1,Adding notebook parameters like username, notebook path, updated time, etc
audit_df = pivot_df_select.withColumn("user_name",lit(dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get())).\
    withColumn("notebook_name",lit(dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get())).\
        withColumn("updated_time",lit(current_timestamp()))

display(audit_df)

# COMMAND ----------

# DBTITLE 1,Rearranging columns to match auditLog table
audit_df_select = audit_df.select(audit_df.operation,audit_df.updated_time, audit_df.user_name, audit_df.notebook_name, audit_df.numUpdatedRows, audit_df.numAddedFiles, audit_df.numRemovedFiles)
display(audit_df_select)

# COMMAND ----------

# DBTITLE 1,create temp view on df
audit_df_select.createOrReplaceTempView("audit_df_table")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from audit_df_table

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from audit_log

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into audit_log 
# MAGIC select * from audit_df_table

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from audit_log

# COMMAND ----------


