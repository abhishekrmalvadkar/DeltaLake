# Databricks notebook source
# DBTITLE 1,Creating Delta table
from delta.tables import *

DeltaTable.create(spark).tableName("Abhishek").\
addColumn("ID","string").\
addColumn("Name","string").\
property("description","My first Delta Table").\
location("/FileStore/tables/target/delta/").execute()

# COMMAND ----------

# DBTITLE 1,Show data from table
# MAGIC %sql
# MAGIC select * from Abhishek

# COMMAND ----------

# DBTITLE 1,Insert data into delta table
# MAGIC %sql
# MAGIC 
# MAGIC insert into Abhishek values ("100","Abhishek");
# MAGIC insert into Abhishek values ("200","Malvadkar");
# MAGIC insert into Abhishek values ("300","Ravindra");
# MAGIC insert into Abhishek values ("400","ARM");

# COMMAND ----------

# DBTITLE 1,Create delta instance (Method 1)
deltainstance1 = DeltaTable.forPath(spark,"/FileStore/tables/target/delta/")

# COMMAND ----------

# DBTITLE 1,convert the delta instance to a DF
display(deltainstance1.toDF())

# COMMAND ----------

deltainstance1.delete("ID=200")

# COMMAND ----------

# DBTITLE 1,Create delta instance (Method 2)
deltainstance2 = DeltaTable.forName(spark, "Abhishek")

# COMMAND ----------

deltainstance2.delete("ID = 300")
display(deltainstance2.toDF())

# COMMAND ----------

# DBTITLE 1,Inserting data into delta table (Method 1: SQL style insert)
# MAGIC  %sql
# MAGIC 
# MAGIC insert into Abhishek values ("113","record1");

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from abhishek

# COMMAND ----------

display(spark.sql("select * from abhishek"))

# COMMAND ----------

# DBTITLE 1,Inserting data into delta table (Method 2: DF insert)
from pyspark.sql.types import StructField,StructType,StringType
data = [("114","record2")]

schema  = StructType([
    StructField("ID",StringType(),True),
    StructField("Name",StringType(),True)
])

df = spark.createDataFrame(data = data, schema = schema)

# COMMAND ----------

df.display()

# COMMAND ----------

df.write.format("delta").mode("append").saveAsTable("abhishek")

# COMMAND ----------

display(spark.sql("select * from abhishek"))

# COMMAND ----------

# DBTITLE 1,Inserting data into delta table (Method 3: DF insert into Method)
from pyspark.sql.types import StructField,StructType,StringType
data1 = [("115","record3")]

schema1  = StructType([
    StructField("ID",StringType(),True),
    StructField("Name",StringType(),True)
])

df1 = spark.createDataFrame(data = data1, schema = schema1)

# COMMAND ----------

df1.display()

# COMMAND ----------

df1.write.insertInto("abhishek",overwrite=False)

# COMMAND ----------

display(spark.sql("select * from abhishek"))

# COMMAND ----------

# DBTITLE 1,Inserting data into delta table (Method 4: Insert using Temp View)
df1.createOrReplaceTempView("abhishek_temp")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from abhishek_temp

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into abhishek
# MAGIC select * from abhishek_temp

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from abhishek

# COMMAND ----------

# DBTITLE 1,Insert the data to be deleted 
# MAGIC %sql
# MAGIC insert into Abhishek values ("151","Delete1");
# MAGIC insert into Abhishek values ("152","Delete2");
# MAGIC insert into Abhishek values ("153","Delete3");
# MAGIC insert into Abhishek values ("154","Delete4");
# MAGIC insert into Abhishek values ("155","Delete5");
# MAGIC insert into Abhishek values ("156","Delete6");
# MAGIC 
# MAGIC select * from Abhishek

# COMMAND ----------

# DBTITLE 1,Deleting data from delta table (Method 1: standard SQL method)
# MAGIC %sql
# MAGIC delete from Abhishek where Name = 'Delete1'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from Abhishek where Name="Delete1"

# COMMAND ----------

# DBTITLE 1,Deleting data from delta table (Method 2: deleting from delta location)
# MAGIC %sql
# MAGIC 
# MAGIC delete from delta. `/FileStore/tables/target/delta ` where Name='Delete2'

# COMMAND ----------

# DBTITLE 1,Deleting data from delta table (Method 3: Spark sql)
 spark.sql("delete from abhishek where name = 'Delete3'")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from abhishek

# COMMAND ----------

# DBTITLE 1,Deleting data from delta table (Method 4: PySpark delta table instance)
from delta.tables import *

deltaTable3 = DeltaTable.forName(spark,"Abhishek")

# COMMAND ----------

deltaTable3.delete("Name='Delete4'")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from abhishek

# COMMAND ----------

# DBTITLE 1,Deleting data from delta table (Method 4: Multiple conditions using SQL predicate)
deltaTable3.delete("Name='Delete5' and ID=155")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from abhishek

# COMMAND ----------

# DBTITLE 1,Deleting data from delta table (Method 5: Spark SQL predicate)
from pyspark.sql.functions import col
deltaTable3.delete(col('Name')=='Delete6' )

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from abhishek

# COMMAND ----------

# DBTITLE 1,Updating the data (Method 1 : standard SQL)
# MAGIC %sql
# MAGIC UPDATE abhishek set name = "ABHISHEK R MALVADKAR" where ID = '113';

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from Abhishek

# COMMAND ----------

# DBTITLE 1,Updating the data (Method 2 : PySpark, using table instance)
from pyspark.sql.functions import *
from delta.tables import *

update_instance1 = DeltaTable.forName(spark,'Abhishek')
# update_instance1 = DeltaTable.forPath(spark, '')
update_instance1.update(
    condition="Name='Abhishek'",
    set={"ID":"13131313"}
)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from Abhishek

# COMMAND ----------

# DBTITLE 1,Updating the data (Method 3 : Spark sql functions)
update_instance1.update(
    condition=col("ID")=='115',
    set = {"Name":lit('DiggiByte Technologies')}
)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from Abhishek

# COMMAND ----------


