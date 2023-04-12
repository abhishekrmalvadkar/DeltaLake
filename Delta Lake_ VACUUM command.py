# Databricks notebook source
from delta.tables import *

DeltaTable.create(spark).tableName("Abhishek").\
addColumn("ID","string").\
addColumn("Name","string").\
property("description","My first Delta Table").\
location("/FileStore/tables/target/deltavacuum/").execute()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from Abhishek;

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC insert into Abhishek values ("100","Abhishek");
# MAGIC insert into Abhishek values ("200","Malvadkar");
# MAGIC insert into Abhishek values ("300","Ravindra");
# MAGIC insert into Abhishek values ("400","ARM");

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from Abhishek;

# COMMAND ----------

deltainstance1 = DeltaTable.forPath(spark,"/FileStore/tables/target/deltavacuum/")
display(deltainstance1.toDF())

# COMMAND ----------

deltainstance1.delete("ID=200")


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from Abhishek;

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC OPTIMIZE Abhishek;

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC VACUUM Abhishek DRY RUN

# COMMAND ----------

# MAGIC %sql
# MAGIC VACUUM Abhishek RETAIN 0 HOURS DRY RUN

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SET spark.databricks.delta.retentionDurationCheck.enabled = FALSE

# COMMAND ----------

# MAGIC %sql
# MAGIC VACUUM Abhishek RETAIN 0 HOURS DRY RUN

# COMMAND ----------

# MAGIC %fs
# MAGIC ls dbfs:/FileStore/tables/target/deltavacuum

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC VACUUM Abhishek

# COMMAND ----------

# MAGIC %fs
# MAGIC ls dbfs:/FileStore/tables/target/deltavacuum

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC optimize Abhishek 
# MAGIC Zorder By Name

# COMMAND ----------


