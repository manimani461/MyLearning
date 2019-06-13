# Databricks notebook source
# MAGIC %md Import Libraries 

# COMMAND ----------

# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql.functions import col, udf
from datetime import datetime

# COMMAND ----------

# MAGIC %md
# MAGIC Configuration Setting

# COMMAND ----------

spark.conf.set(
  "fs.azure.account.key.badevadlsg2.blob.core.windows.net",
  "w/AdJ4feLp5Pqbc9zRAtOfRRoax70KcYtik0ASoTzz6lG3kuqIOldgbHblTKbv7/BBJhf4s6UYhV76NUZEE1IQ==")

jdbcHostname = "ba-dev-asqlserver.database.windows.net"
jdbcDatabase = "ba-dev-asqldb"
jdbcPort = 1433
jdbcUsername='sql_user'
jdbcPassword='LaerdalTest#123'
jdbcUrl = "jdbc:sqlserver://{0}:{1};database={2};user={3};password={4}".format(jdbcHostname, jdbcPort, jdbcDatabase, jdbcUsername, jdbcPassword)

connectionProperties = {
  "user" : jdbcUsername,
  "password" : jdbcPassword, 
  "driver" : "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

spark.conf.set("spark.sql.crossJoin.enabled", "true")

# COMMAND ----------

# MAGIC %md
# MAGIC Import Datasets

# COMMAND ----------

  
ld_det = spark.read.format("csv").options(header='true', delimiter = ',').load("wasbs://adlsdev@badevadlsg2.blob.core.windows.net/Governed Data/Sources/LMAS/QAD/Pro2qad/ld_det.csv")
sct_det = spark.read.format("csv").options(header='true', delimiter = ',').load("wasbs://adlsdev@badevadlsg2.blob.core.windows.net/Governed Data/Sources/LMAS/QAD/Pro2qad/sct_det.csv")
DimCurrency = spark.read.jdbc(url = jdbcUrl, table = '[dbo].[DimCurrency]', properties = connectionProperties)
DimDomain = spark.read.jdbc(url = jdbcUrl, table = '[dbo].[DimDomain]', properties = connectionProperties)
DimSupplier = spark.read.jdbc(url = jdbcUrl, table = '[dbo].[DimSupplier]', properties = connectionProperties)
DimBuyerPlanner = spark.read.jdbc(url = jdbcUrl, table = '[dbo].[DimBuyerPlanner]', properties = connectionProperties)
DimDistributionCenter = spark.read.jdbc(url = jdbcUrl, table = '[dbo].[DimDistributionCenter]', properties = connectionProperties)
DimInventoryLocation = spark.read.jdbc(url = jdbcUrl, table = '[dbo].[DimInventoryLocation]', properties = connectionProperties)
DimPartMaster = spark.read.jdbc(url = jdbcUrl, table = '[dbo].[DimPartMaster]', properties = connectionProperties)
DimItemPlanningDetail = spark.read.jdbc(url = jdbcUrl, table = '[dbo].[DimItemPlanningDetail]', properties = connectionProperties)


# COMMAND ----------

vd_mstr = spark.read.format("csv").options(header='true', delimiter = ',').load("wasbs://adlsdev@badevadlsg2.blob.core.windows.net/Governed Data/Sources/LMAS/QAD/Pro2qad/sct_det.csv")

# COMMAND ----------

ld_det.count()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Fact Transformation

# COMMAND ----------

FactInventoryLocation = ld_det.join(DimPartMaster, (F.upper(ld_det.ld_part) == F.upper(DimPartMaster.PartID)),how='left'). \
join(sct_det, (F.upper(ld_det.ld_domain) == F.upper(sct_det.sct_domain)) & (F.upper(ld_det.ld_part) == F.upper(sct_det.sct_part)) & (F.upper(ld_det.ld_site) == F.upper(sct_det.sct_site)) & (F.upper(sct_det.sct_sim) == "STANDARD"),how='left'). \
join(DimItemPlanningDetail, (F.upper(ld_det.ld_domain) == F.upper(DimItemPlanningDetail.Domain)) & (F.upper(ld_det.ld_part) == F.upper(DimItemPlanningDetail.Part)) & (F.upper(ld_det.ld_site) == F.upper(DimItemPlanningDetail.Site)),how='left'). \
join(DimBuyerPlanner, (F.upper(DimItemPlanningDetail.BuyerPlanner) == F.upper(DimBuyerPlanner.BuyerPlannerCode)),how='left'). \
join(DimSupplier, (F.upper(DimItemPlanningDetail.Suppplier) == F.upper(DimSupplier.SupplierID)),how='left'). \
join(DimDistributionCenter, (F.upper(ld_det.ld_site) == F.upper(DimDistributionCenter.SiteCode)),how='left'). \
join(DimDomain, (F.upper(ld_det.ld_domain) == F.upper(DimDomain.DomainCode)),how='left'). \
join(DimCurrency, (F.upper(DimDomain.DomainCurrency) == F.upper(DimCurrency.CurrencyCode)),how='left'). \
join(DimInventoryLocation,(F.upper(ld_det.ld_domain) == F.upper(DimInventoryLocation.Domain)) & (F.upper(ld_det.ld_loc) == F.upper(DimInventoryLocation.Location)) & (F.upper(ld_det.ld_site) == F.upper(DimInventoryLocation.Site)),how='left'). \
select( \
F.date_format(F.current_timestamp(), 'yyyy-MM-dd HH:mm:ss').alias("Time_Id"), \
DimInventoryLocation.InventoryLocationId.alias("InventoryLocationId"), \
DimPartMaster.ID.alias("PartId"), \
DimBuyerPlanner.BuyerPlannerId.alias("BuyerPlannerId"), \
DimSupplier.SupplierRowId.alias("SupplierId"), \
DimItemPlanningDetail.ItemPlanningId.alias("ItemPlanningId"), \
DimDistributionCenter.SiteId.alias("SiteId"), \
DimDomain.DomainId.alias("DomainId"), \
DimCurrency.CurrencyId.alias("CurrencyId"), \
ld_det.ld_lot.alias("Lot/Serial"), \
ld_det.ld_status.alias("LocationStatus"), \
ld_det.ld_domain.alias("DomainCode"), \
ld_det.ld_site.alias("SiteCode"), \
ld_det.ld_part.alias("PartCode"), \
DimPartMaster.ItemStatus.alias("ItemStatus"), \
F.round(ld_det.ld_qty_oh.cast("double"),3).alias("QtyOnHand"), \
F.round(ld_det.ld_qty_all.cast("double"),3).alias("QtyAllocated"), \
F.round((sct_det.sct_cst_tot.cast("double") * DimCurrency.ExchangeRate.cast("double")),3).alias("StandardCost"), \
F.round((ld_det.ld_qty_oh.cast("double") * sct_det.sct_cst_tot.cast("double") * DimCurrency.ExchangeRate.cast("double")), 3).alias("InventoryValue"), \
F.round((ld_det.ld_qty_oh.cast("double") -  ld_det.ld_qty_all.cast("double")),3).alias("QtyAvailable"), \
F.lit(1).alias("NoOfLines"), \
DimPartMaster.Metric_Ship_Weight_KG.alias("Weight"), \
DimPartMaster.Volume_CM.alias("Volume"))

# COMMAND ----------

FactInventoryLocation.count()

# COMMAND ----------

FactInventoryLocation.count() - FactInventoryLocation.dropDuplicates().count()

# COMMAND ----------

display(FactInventoryLocation)

# COMMAND ----------

# MAGIC %md
# MAGIC Loading to ADLS

# COMMAND ----------

FactInventoryLocation.write.format("com.databricks.spark.csv").option("header", "true").option("delimiter", ",").option("quoteAll", "true").mode('overwrite').save("wasbs://adlsdev@badevadlsg2.blob.core.windows.net/Governed Data/Sources/LMAS/QAD/Staging/FactInventoryLocation")

# COMMAND ----------

# MAGIC %md Clear unwanted files under staging folder

# COMMAND ----------

Files = dbutils.fs.ls("wasbs://adlsdev@badevadlsg2.blob.core.windows.net/Governed Data/Sources/LMAS/QAD/Staging/FactInventoryLocation/")

for lst in Files:
  for index,File in enumerate(lst):
    if index == 0:
      if File.endswith(".csv"):
        print(File)
      else:
        dbutils.fs.rm(File)

# COMMAND ----------


