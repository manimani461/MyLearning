# Databricks notebook source
# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql.functions import col, udf
from datetime import datetime


# COMMAND ----------

spark.conf.set(
  "fs.azure.account.key.badevadlsg2.blob.core.windows.net",
  "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx")

# COMMAND ----------

dbutils.fs.ls("wasbs://adlsdev@badevadlsg2.blob.core.windows.net/Governed Data/Sources/LMAS/QAD/")

# COMMAND ----------

# MAGIC %md Dev/Test ADLS Connection 

# COMMAND ----------

spark.conf.set(
  "fs.azure.account.key.badevadlsgen2.dfs.core.windows.net",
  "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx")

# COMMAND ----------

dbutils.fs.ls("abfss://adlsdev@badevadlsgen2.dfs.core.windows.net/")

# COMMAND ----------

# MAGIC %md Copy from Dev Container to Test container...All Files 

# COMMAND ----------

dbutils.fs.cp("abfss://adlsdev@badevadlsgen2.dfs.core.windows.net/","abfss://adlstest@badevadlsgen2.dfs.core.windows.net/", recurse = True)

# COMMAND ----------

# MAGIC %md  Prod ADLS Connection

# COMMAND ----------

spark.conf.set(
  "fs.azure.account.key.baprodadlsgen2.dfs.core.windows.net",
  "CeU4VE1ykwoS9Hl4G7XWgjcRBzS5u6PFHs6cVpoAftOfDsVll1xQXM3ymVQAAjgKVBCJN3mVWCFo2naoIDI0qg==")

# COMMAND ----------

# MAGIC %md Copy Files From Test to Production Data Lake

# COMMAND ----------

dbutils.fs.cp("abfss://adlstest@badevadlsgen2.dfs.core.windows.net/","abfss://adlsprod@baprodadlsgen2.dfs.core.windows.net/", recurse = True)

# COMMAND ----------


