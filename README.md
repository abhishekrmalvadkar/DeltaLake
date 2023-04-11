SCD TYPE 2 Question:

We have a delta table with 2 Primary keys [pk1 and pk2], four dimension attribute [dim1, dim2, dim3 and dim4], a status flag [active_status], start_date and 
end_date which will hold a hypothetical value if it is an active record.

Source data has  2 Primary keys [pk1 and pk2], four dimension attribute [dim1, dim2, dim3 and dim4].
The source data has the following combinations:
  1. Record with pk1 = 111 has no change at all
  2. Record with pk1 = 444 is a brand new record which should be inserted to the target table.
  3. Record with pk1 = 222 has some change of value 
So we have to make the previous version of combination (pk1 = 222 and pk2 = unit2) as inactive and insert the new record in the target table 

<img width="946" alt="scdtype2 Q1" src="https://user-images.githubusercontent.com/48563516/231143477-2d5c006d-e31e-4764-b23a-66b2c297485c.png">
<img width="828" alt="scdtype2 Q2" src="https://user-images.githubusercontent.com/48563516/231143534-910a3d65-ca55-43a8-8661-f2f75488be79.png">

Expected output and the important steps are shown in the screenshots

---------------------------------------------------------------------------------------------------------------------------------------------------------------------
Audit Log for Delta Lake table operations :
Create a audit log table to populated required information like type of operation, updated_time, userName,  notebookName, numTargetRowsUpdated , 
numTargetRowsInserted , numTargetRowsDeleted 

---------------------------------------------------------------------------------------------------------------------------------------------------------------------

DeltaTables_ Merge using pyspark and sql :

Create two tables and merged them using PySpark and Spark Sql methods to populate the data

---------------------------------------------------------------------------------------------------------------------------------------------------------------------

DeltaTables:

•	Different ways to insert data in delta Lake tables (SQL style insert, DF insert, DF insert Into Method, Insert using Temp View)
•	Different ways to delete data from delta Lake tables (standard SQL method, deleting via delta Lake location, Spark SQL, PySpark delta table instance, Multiple conditions using SQL predicate, Spark SQL predicate)
•	Different ways to update data in delta Lake tables (standard SQL method, PySpark, using table instance, Spark SQL functions)
