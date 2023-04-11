# Databricks notebook source
# MAGIC %sql 
# MAGIC create or replace table scd2 (
# MAGIC   pk1 int,
# MAGIC   pk2 string,
# MAGIC   dim1 int,
# MAGIC   dim2 int,
# MAGIC   dim3 int,
# MAGIC   dim4 int,
# MAGIC   active_status string,
# MAGIC   start_date timestamp,
# MAGIC   end_date timestamp
# MAGIC ) using DELTA
# MAGIC location "/FileStore/tables/SCDTYPE2"

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into scd2 values(111,'Unit1',200,500,800,400,'Y',CURRENT_TIMESTAMP(),'9999-12-31');
# MAGIC insert into scd2 values(222,'Unit2',900,NULL,700,100,'Y',CURRENT_TIMESTAMP(),'9999-12-31');
# MAGIC insert into scd2 values(333,'Unit3',300,900,250,650,'Y',CURRENT_TIMESTAMP(),'9999-12-31');

# COMMAND ----------

from delta.tables import *
targetTable = DeltaTable.forPath(spark, "/FileStore/tables/SCDTYPE2")
targetDf = targetTable.toDF()
display(targetDf)

# COMMAND ----------

 from pyspark.sql.functions import *
 from pyspark.sql.types import *

schema  = StructType([
    StructField("pk1",IntegerType(),True),
    StructField("pk2",StringType(),True),
    StructField("dim1",IntegerType(),True),
    StructField("dim2",IntegerType(),True),
    StructField("dim3",IntegerType(),True),
    StructField("dim4",IntegerType(),True),
    
])

data = [(111,"Unit1",200,500,800,400),(222,"Unit2",800,1300,800,500),(444,"Unit4",100,None,700,300)]

sourceDF = spark.createDataFrame(data = data , schema = schema )

display(sourceDF)

# COMMAND ----------

joinDF = sourceDF.join(targetDf, (sourceDF.pk1 == targetDf.pk1) & \
                       (sourceDF.pk2 == targetDf.pk2) & \
                       (targetDf.active_status == "Y"), "leftouter")\
                        .select(sourceDF["*"],\
                                    targetDf.pk1.alias("target_pk1"),\
                                    targetDf.pk2.alias("target_pk2"),\
                                    targetDf.dim1.alias("target_dim1"),\
                                    targetDf.dim2.alias("target_dim2"),\
                                    targetDf.dim3.alias("target_dim3"),\
                                    targetDf.dim4.alias("target_dim4"))
display(joinDF)

 #& (col("sourceDF.pk2") == col("targetDF.pk2")) & (col("targetDF.active_status") =='Y')

# COMMAND ----------

filterDF = joinDF.filter(xxhash64(joinDF.dim1,joinDF.dim2,joinDF.dim3,joinDF.dim4)!=xxhash64(targetDf.dim1,targetDf.dim2,targetDf.dim3,targetDf.dim4))
display(filterDF)

# COMMAND ----------

mergeDF = filterDF.withColumn("MERGEKEY",concat(filterDF.pk1,filterDF.pk2))
display(mergeDF)

# COMMAND ----------

matchedDF = mergeDF.filter("target_pk1 is NOT NULL").withColumn("MERGEKEY",lit("None"))
display(matchedDF)

# COMMAND ----------

unionDF = matchedDF.union(mergeDF)
display(unionDF)

# COMMAND ----------

spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")
targetTable.alias("target").merge(
    source = unionDF.alias("source"),
    condition= "concat(target.pk1,target.pk2) = source.MERGEKEY and target.active_status ='Y'"
).whenMatchedUpdate(set=
{
    "active_status":"'N'",
    "end_date":"current_date"
}).whenNotMatchedInsert(values = 
{
    "pk1":"source.pk1",
    "pk2":"source.pk2",
    "dim1":"source.dim1",
    "dim2":"source.dim2",
    "dim3":"source.dim3",
    "dim4":"source.dim4",
    "active_status":"'Y'",
    "start_date":"current_date",
    "end_date":"""to_date('9999-12-31','YYYY-MM-DD')"""
    
}).execute()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from scd2

# COMMAND ----------


