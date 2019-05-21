# Databricks notebook source
jdbcHostname = "lilleputt.database.windows.net"
jdbcDatabase = "AdventureWorks2012"
jdbcPort = 1433
jdbcUsername='sstavlan'
jdbcPassword='sYkt¤hEmm.Elig68'
jdbcUrl = "jdbc:sqlserver://{0}:{1};database={2};user={3};password={4}".format(jdbcHostname, jdbcPort, jdbcDatabase, jdbcUsername, jdbcPassword)

# COMMAND ----------

jdbcUrl = "jdbc:sqlserver://{0}:{1};database={2}".format(jdbcHostname, jdbcPort, jdbcDatabase)
connectionProperties = {
  "user" : jdbcUsername,
  "password" : jdbcPassword,
  "driver" : "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

# COMMAND ----------

pushdown_query = "(select * from dbo.imagetest) img_bin"
df = spark.read.jdbc(url=jdbcUrl, table=pushdown_query, properties=connectionProperties)
display(df)

# COMMAND ----------

type(df.image_col[0])

# COMMAND ----------

photo=df.rdd.map(lambda p: bytearray(p.image_col[0]))

# COMMAND ----------

print(photo)

# COMMAND ----------

photo2=bytes(df['image_col'].values)

# COMMAND ----------

df.select('image_col').show()

# COMMAND ----------

df.dtypes

# COMMAND ----------

def write_file(data, filename):
  with open(filename,'wb') as f:
    f.write(data)

# COMMAND ----------

filename='capgemini.jpg'


# COMMAND ----------

df.select('image_col').write.save(filename)

# COMMAND ----------

dbutils.fs.ls("adl://lilleputt1lagring.azuredatalakestore.net/Storage")

# COMMAND ----------

configs = {"dfs.adls.oauth2.access.token.provider.type": "ClientCredential",
           "dfs.adls.oauth2.client.id": "cad9df8b-126c-452a-94db-c81aade7921c",
           "dfs.adls.oauth2.credential": "tUDnRWLSbS1ipZ6uOwVz5jmw++RpBIIZq1vurv5dUXk=",
           "dfs.adls.oauth2.refresh.url": "https://login.microsoftonline.com/76a2ae5a-9f00-4f6b-95ed-5d33d77c4d61/oauth2/token"}

dbutils.fs.mount(
  source = "adl://lilleputt1lagring.azuredatalakestore.net/Storage",
  mount_point = "/mnt/test",
  extra_configs = configs)

# COMMAND ----------

# MAGIC %fs ls /mnt/test/

# COMMAND ----------

df.select('image_col').write.save('dbfs:/mnt/test/'+filename)

# COMMAND ----------

# MAGIC %fs text /mnt/test/capgemini.jpg/part-00000-tid-282251761264420048-633e7ba0-30f4-44ed-9ef0-db2707d772ab-31-c000.snappy.parquet

# COMMAND ----------

# MAGIC %fs head /mnt/test/capgemini.jpg/part-00000-tid-282251761264420048-633e7ba0-30f4-44ed-9ef0-db2707d772ab-31-c000.snappy.parquet

# COMMAND ----------

with open("dbfs:/mnt/test/capgemini.jpg/part-00000-tid-282251761264420048-633e7ba0-30f4-44ed-9ef0-db2707d772ab-31-c000.snappy.parquet") as f:
  f.readlines()

# COMMAND ----------

write_file(df.select('image_col'),'dbfs:/mnt/test/test.jpg')

# COMMAND ----------

for img in df.rdd.toLocalIterator():
  photo=img[0]


# COMMAND ----------

print(photo)

# COMMAND ----------

images = sc.binaryFiles("dbfs:/mnt/test/")
#image_to_array = lambda rawdata: np.asarray(Image.open(StringIO(rawdata)))
images.values().map(photo)

# COMMAND ----------

print(images)

# COMMAND ----------

display(images)

# COMMAND ----------

dbutils.fs.unmount("/mnt/test")

# COMMAND ----------

spark.conf.set("dfs.adls.oauth2.access.token.provider.type", "ClientCredential")
spark.conf.set("dfs.adls.oauth2.client.id", "cad9df8b-126c-452a-94db-c81aade7921c")
spark.conf.set("dfs.adls.oauth2.credential", "tUDnRWLSbS1ipZ6uOwVz5jmw++RpBIIZq1vurv5dUXk=")
spark.conf.set("dfs.adls.oauth2.refresh.url", "https://login.microsoftonline.com/76a2ae5a-9f00-4f6b-95ed-5d33d77c4d61/oauth2/token")

# COMMAND ----------

dbutils.fs.ls("adl://lilleputt1lagring.azuredatalakestore.net/Storage")

# COMMAND ----------

df.write.csv('dbfs:/mnt/kp-adls/Test.csv')


# COMMAND ----------

configs = {"dfs.adls.oauth2.access.token.provider.type": "ClientCredential",
           "dfs.adls.oauth2.client.id": "cad9df8b-126c-452a-94db-c81aade7921c",
           "dfs.adls.oauth2.credential": "tUDnRWLSbS1ipZ6uOwVz5jmw++RpBIIZq1vurv5dUXk=",
           "dfs.adls.oauth2.refresh.url": "https://login.microsoftonline.com/76a2ae5a-9f00-4f6b-95ed-5d33d77c4d61/oauth2/token"}

dbutils.fs.mount(
  source = "adl://lilleputt1lagring.azuredatalakestore.net/Storage",
  mount_point = "/mnt/img",
  extra_configs = configs)

# COMMAND ----------

import io
from PIL import  Image
import numpy as np
image_array=np.asarray(Image.open(io.BytesIO(photo)))
newimg=Image.open(io.BytesIO(photo))
newimg.save('dbfs:/mnt/img/cap.jpg')
#image_array.save('/mnt/test/cap.jpg')
#images=sc.binaryFiles("adl://lilleputt1lagring.azuredatalakestore.net/Storage/")
#images_to_array = lambda photo: np.asarray(Image.open(StringIO(Photo)))
#images.values().map(image_to_array)

# COMMAND ----------

newimg.save('cap.jpg')

# COMMAND ----------

# MAGIC %fs ls dbfs:/

# COMMAND ----------

dbutils.fs.ls("dbfs:/mnt/img")

# COMMAND ----------


