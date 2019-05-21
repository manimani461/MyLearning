# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql.functions import col, udf
from datetime import datetime

# Creating ADLS Configuration for mount 
configs = {"dfs.adls.oauth2.access.token.provider.type": "ClientCredential",
           "dfs.adls.oauth2.client.id": "cad9df8b-126c-452a-94db-c81aade7921c",
           "dfs.adls.oauth2.credential": "tUDnRWLSbS1ipZ6uOwVz5jmw++RpBIIZq1vurv5dUXk=",
           "dfs.adls.oauth2.refresh.url": "https://login.microsoftonline.com/76a2ae5a-9f00-4f6b-95ed-5d33d77c4d61/oauth2/token"}
# Creating Mount for the defined ADLS Path 
dbutils.fs.mount(source = "adl://lilleputt1lagring.azuredatalakestore.net/Production_Data.csv",mount_point = "/mnt/kp-adls",extra_configs = configs)

# COMMAND ----------

# Creating Data frame for the given ADLS File
df = spark.read.format("csv").options(header='true', delimiter = ',').load("dbfs:/mnt/kp-adls/Production_Data.csv")

# COMMAND ----------

# Analysing the data 
df.printSchema()
df.count()
display(df)

# COMMAND ----------

from pyspark.sql import functions as F
df1 = df.select(col('WellID'),col('TotalProduction')).groupBy('WellID').agg(F.sum('TotalProduction').alias('Sum'))
display(df1)

# COMMAND ----------

# changing the data type 
# Created UDF for date conversion
DateConversion =  udf (lambda x: datetime.strptime(x, '%m/%d/%Y'), DateType())
# Casting  the data types for few columns 
df = df.select(df.WellID,df.Field,DateConversion(df.Month).alias('Month'),df.AvgProduction.cast("double"),df.TotalProduction.cast("double"))
#df = df.withColumn('Date', DateConversion(col('Month')))

# COMMAND ----------

# JDBC connection properties 
jdbcHostname = "avanadtaskserver01.database.windows.net"
jdbcDatabase = "avanadetaskdb01"
jdbcPort = 1433
jdbcUsername='mabotula'
jdbcPassword='man.mani%461'
jdbcUrl = "jdbc:sqlserver://{0}:{1};database={2};user={3};password={4}".format(jdbcHostname, jdbcPort, jdbcDatabase, jdbcUsername, jdbcPassword)

# COMMAND ----------

# JDBC connection properties 
jdbcUrl = "jdbc:sqlserver://{0}:{1};database={2}".format(jdbcHostname, jdbcPort, jdbcDatabase)
connectionProperties = {
  "user" : jdbcUsername,
  "password" : jdbcPassword, 
  "driver" : "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

# COMMAND ---------

#Write Data to Sql server 
df.select(col('WellID'),col('Field'),col('Month'),col('AvgProduction'),col('TotalProduction')).write.jdbc(url=jdbcUrl, table="avanadetaskdb01.dbo.ProductionData", mode="append", properties=connectionProperties)

# COMMAND ----------


databricks secrets put --scope mykeyvalut --key data-bricks-key

https://lilleputtvault.vault.azure.net/secrets/data-bricks-key/e9449cb9c72c408c8b7c86929d1a772c
https://westeurope.azuredatabricks.net/?o=2148178971413283#

https://westeurope.azuredatabricks.net#secrets/createScope


https://lilleputt-databricks.vault.azure.net/

mykeyvalut

https://lilleputtvault.vault.azure.net/secrets/data-bricks-key/e9449cb9c72c408c8b7c86929d1a772c
