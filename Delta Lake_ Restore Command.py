# Databricks notebook source
from pyspark.sql.functions import * 
from pyspark.sql.types import *
from  delta import *

DeltaTable.create(spark).tableName("Tablerestore").addColumn("id","string").addColumn("fname","string").addColumn("lname","string").execute()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from tablerestore

# COMMAND ----------

# MAGIC %sql
# MAGIC Insert into tablerestore values ("1","abc","abc");
# MAGIC Insert into tablerestore values ("2","bbc","bbc");
# MAGIC Insert into tablerestore values ("3","cbc","cbc");
# MAGIC Insert into tablerestore values ("4","dbc","dbc");

# COMMAND ----------

tableInstance = DeltaTable.forName(spark,"Tablerestore")


# COMMAND ----------

display(tableInstance.history())

# COMMAND ----------

# MAGIC %sql 
# MAGIC Select * from tablerestore

# COMMAND ----------

tableInstance.delete(col('id')=="1")

# COMMAND ----------

# MAGIC %sql 
# MAGIC Select * from tablerestore

# COMMAND ----------

# MAGIC %sql
# MAGIC delete FROM tablerestore  where id = "2";

# COMMAND ----------

# MAGIC %sql
# MAGIC update tablerestore set fname="ARM" where id = "4"

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from tablerestore

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC describe history tablerestore

# COMMAND ----------

tableInstance.restoreToVersion(5)

# COMMAND ----------

tableInstance.restoreToTimestamp("2023-04-12T06:45:47.000+0000")

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT * FROM tablerestore

# COMMAND ----------


