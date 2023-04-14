# Databricks notebook source
from delta import *
DeltaTable.createOrReplace(spark).\
    tableName("internal_arch").\
        addColumn("col1","string").\
            addColumn("col2","string").\
                addColumn("col3","string").\
                    property("description","Internal architecture").\
                        location("/Filestore/tables/delta/internal_arc").\
                        execute()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from internal_arc

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /Filestore/tables/delta/internal_arc/_delta_log/00000000000000000001.json

# COMMAND ----------

# MAGIC %fs
# MAGIC head /Filestore/tables/delta/internal_arc/_delta_log/00000000000000000001.json

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC insert into internal_arch values ("ONE","ONE","ONE")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from internal_arc

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC insert into internal_arch values ("Two","Two","Two");
# MAGIC insert into internal_arch values ("Two","Two","Two");
# MAGIC insert into internal_arch values ("Three","Three","Three");
# MAGIC insert into internal_arch values ("Four","Four","Four")

# COMMAND ----------

# MAGIC %sql
# MAGIC delete from internal_arch where col1="Two"

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM internal_arch

# COMMAND ----------

# MAGIC %fs
# MAGIC 
# MAGIC ls /Filestore/tables/delta/internal_arc/_delta_log/

# COMMAND ----------

df = spark.read.format("parquet").load("dbfs:/Filestore/tables/delta/internal_arc/_delta_log/00000000000000000010.checkpoint.parquet")

display(df)

# COMMAND ----------


