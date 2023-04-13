# Databricks notebook source
from delta import *

DeltaTable.create(spark).\
    tableName("EmployeeTab").\
    addColumn("emp_id","INT").\
    addColumn("emp_name","STRING").\
    addColumn("SSN","STRING").\
    property("description","employee table encryption").\
    execute()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from employeetab

# COMMAND ----------

# DBTITLE 1,Insert PII data
# MAGIC %sql
# MAGIC insert into employeetab values(1,"empOne",'1234');
# MAGIC insert into employeetab values(2,"empTwo",'5678');
# MAGIC insert into employeetab values(3,"empThree",'9101');

# COMMAND ----------

# DBTITLE 1,View PII data
# MAGIC %sql
# MAGIC select * from employeetab

# COMMAND ----------

# DBTITLE 1,Cryptography library installation
pip install cryptography

# COMMAND ----------

# DBTITLE 1,Generate encryption key
from cryptography.fernet import Fernet

key = Fernet.generate_key()
f = Fernet(key)

# COMMAND ----------

# DBTITLE 1,encrypt sample data
piiData = b"abhishek@gmail.com"
testdata = f.encrypt(piiData)
print(testdata)

# COMMAND ----------

# DBTITLE 1,Decrypt sample data
print(f.decrypt(testdata))

# COMMAND ----------

# DBTITLE 1,Define UDF to encrypt data
def encrypt_data(data,KEY):
    from cryptography.fernet import Fernet
    f = Fernet(KEY)
    data_bytes = bytes(data, 'utf-8')
    encrypted_data = f.encrypt(data_bytes)
    encrypted_data = str(encrypted_data.decode('ascii'))
    return encrypted_data


# COMMAND ----------

# DBTITLE 1,UDF to decrypt data
def decrypt_data(encrypted_data,KEY):
    from cryptography.fernet import Fernet
    f = Fernet(KEY)
    decrypted_data = f.decrypt(encrypted_data.encode()).decode()
    return decrypted_data


# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

encryption= udf(encrypt_data, StringType())
decryption= udf(decrypt_data, StringType())

# COMMAND ----------

df1 = spark.table("EmployeeTab")
encryptedDF = df1.withColumn("SSN_encrypted", encryption("SSN",lit(key)))
display(encryptedDF)

# COMMAND ----------

decryptedDF =  encryptedDF.withColumn("SSN_decrypted", decryption("SSN_encrypted",lit(key)))
display(decryptedDF)

# COMMAND ----------

final_df = decryptedDF.drop(col('SSN'))

# COMMAND ----------

display(final_df)

# COMMAND ----------


