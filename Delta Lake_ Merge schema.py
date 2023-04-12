# Databricks notebook source
from delta import *
DeltaTable.create(spark).\
    tableName("employee").\
    addColumn("emp_id","INT").\
        addColumn("fname","STRING").\
            addColumn("lname","STRING").\
                property("description","Merge Schema").\
                    location("/FileStore/tables/delta/MergeSchema").\
                    execute()

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from employee

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into employee values (1, "Abhishek", "Malvadkar");
# MAGIC insert into employee values (2, "Abhishek", "Malvadkar");
# MAGIC insert into employee values (3, "Abhishek", "Malvadkar");
# MAGIC insert into employee values (4, "Abhishek", "Malvadkar");

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *

data = [(5,"Malvadkar","Abhishek",1000000)]

schema =  StructType([
    StructField("emp_id",IntegerType(),True),
    StructField("fname",StringType(),True),
    StructField("lname",StringType(),True),
    StructField("salary",IntegerType(),True),
])

df = spark.createDataFrame(data = data, schema = schema)

# COMMAND ----------

df.show()

# COMMAND ----------

df.write.format("delta").mode('append').saveAsTable("employee")

# COMMAND ----------

df.write.option("mergeSchema",True).format("delta").mode('append').saveAsTable("employee")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from employee

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *

data = [(5,"Malvadkar","Abhishek","CS")]

schema =  StructType([
    StructField("emp_id",IntegerType(),True),
    StructField("fname",StringType(),True),
    StructField("lname",StringType(),True),
    StructField("dept",StringType(),True),
])

df = spark.createDataFrame(data = data, schema = schema)

# COMMAND ----------

df.show()

# COMMAND ----------

df.write.option("mergeSchema",True).format("delta").mode("append").saveAsTable("employee")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from employee

# COMMAND ----------


