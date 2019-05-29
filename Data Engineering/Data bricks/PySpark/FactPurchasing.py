# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC Import Libraries 

# COMMAND ----------

# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql.functions import col, udf
from datetime import datetime

# COMMAND ----------

datediff_udf = udf(lambda  x,y: F.datediff(F.to_date(x, 'yyyy-MM-dd'),F.to_date(y, 'yyyy-MM-dd')), IntegerType())

# COMMAND ----------

spark.conf.set(
  "fs.azure.account.key.badevadlsg2.blob.core.windows.net",
  "xxxxxxxxxxxxxxxxkey")

jdbcHostname = "ba-dev-asqlserver.database.windows.net"
jdbcDatabase = "ba-dev-asqldb"
jdbcPort = 1433
jdbcUsername='sql_user'
jdbcPassword='xxxxxxxxxx'
jdbcUrl = "jdbc:sqlserver://{0}:{1};database={2};user={3};password={4}".format(jdbcHostname, jdbcPort, jdbcDatabase, jdbcUsername, jdbcPassword)

connectionProperties = {
  "user" : jdbcUsername,
  "password" : jdbcPassword, 
  "driver" : "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

# COMMAND ----------

  
prh_hist = spark.read.format("csv").options(header='true', delimiter = ',').load("wasbs://adlsdev@badevadlsg2.blob.core.windows.net/Governed Data/Sources/LMAS/QAD/Pro2qad/prh_hist.csv")
pod_det = spark.read.format("csv").options(header='true', delimiter = ',').load("wasbs://adlsdev@badevadlsg2.blob.core.windows.net/Governed Data/Sources/LMAS/QAD/Pro2qad/pod_det.csv")
po_mstr = spark.read.format("csv").options(header='true', delimiter = ',').load("wasbs://adlsdev@badevadlsg2.blob.core.windows.net/Governed Data/Sources/LMAS/QAD/Pro2qad/po_mstr.csv")
DimCurrency = spark.read.jdbc(url = jdbcUrl, table = '[dbo].[DimCurrency]', properties = connectionProperties)
DimDomain = spark.read.jdbc(url = jdbcUrl, table = '[dbo].[DimDomain]', properties = connectionProperties)
DimSupplier = spark.read.jdbc(url = jdbcUrl, table = '[dbo].[DimSupplier]', properties = connectionProperties)
DimItemPlanningDetail = spark.read.jdbc(url = jdbcUrl, table = '[dbo].[DimItemPlanningDetail]', properties = connectionProperties)
DimShippingMode = spark.read.jdbc(url = jdbcUrl, table = '[dbo].[DimShippingMode]', properties = connectionProperties)
DimTransactionType = spark.read.jdbc(url = jdbcUrl, table = '[dbo].[DimTransactionType]', properties = connectionProperties)
DimDistributionCenter = spark.read.jdbc(url = jdbcUrl, table = '[dbo].[DimDistributionCenter]', properties = connectionProperties)
DimPOLineDetails = spark.read.jdbc(url = jdbcUrl, table = '[dbo].[DimPOLineDetails]', properties = connectionProperties)
DimPartMaster = spark.read.jdbc(url = jdbcUrl, table = '[dbo].[DimPartMaster]', properties = connectionProperties)

# COMMAND ----------

join = prh_hist.join(pod_det, (prh_hist.prh_domain == pod_det.pod_domain)&(prh_hist.prh_nbr == pod_det.pod_nbr)&(prh_hist.prh_line == pod_det.pod_line),how='left') \
.join(po_mstr,(pod_det.pod_domain == po_mstr.po_domain)&(pod_det.pod_nbr == po_mstr.po_nbr),how='left') \
.join(DimDomain,(prh_hist.prh_domain == DimDomain.DomainCode),how='left') \
.join(DimCurrency,(prh_hist.prh_curr == DimCurrency.CurrencyCode),how='left') \
.join(DimSupplier,(po_mstr.po_vend == DimSupplier.SupplierID),how='left') \
.join(DimItemPlanningDetail,(prh_hist.prh_domain == DimItemPlanningDetail.Domain)&(prh_hist.prh_part == DimItemPlanningDetail.Part),how='left') \
.join(DimShippingMode,(po_mstr.po_shipvia == DimShippingMode.ShippingMode),how='left') \
.join(DimDistributionCenter,(prh_hist.prh_site == DimDistributionCenter.SiteCode),how='left') \
.join(DimPOLineDetails,(prh_hist.prh_domain == DimPOLineDetails.PODomain)&(pod_det.pod_nbr == DimPOLineDetails.PONumber),how='left') \
.join(DimTransactionType,(pod_det.pod_type == DimTransactionType.TransactionTypeCode),how='left') \
.join(DimPartMaster,(prh_hist.prh_part == DimPartMaster.ITEM_NUMBER),how='left') \
.select( \
DimDistributionCenter.SiteId.alias('SiteID'), \
DimPartMaster.ID.alias('PartMasterID'), \
DimTransactionType.TransactionTypeId.alias('TransactionTypeId'), \
DimSupplier.SupplierRowId.alias('SupplierID'), \
DimDomain.DomainId.alias('DomainId'), \
DimCurrency.CurrencyId.alias('CurrencyId'), \
DimShippingMode.ShippingModeId.alias('ShippingModeId'), \
DimItemPlanningDetail.ItemPlanningId.alias('ItemPlanningId'), \
F.when(F.datediff(F.to_date(prh_hist.prh_per_date, 'yyyy-MM-dd'),F.to_date(prh_hist.prh_rcp_date, 'yyyy-MM-dd')) < -21,F.lit(1)). \
when((F.datediff(F.to_date(prh_hist.prh_per_date, 'yyyy-MM-dd'),F.to_date(prh_hist.prh_rcp_date, 'yyyy-MM-dd')) >= -21) & (F.datediff(F.to_date(prh_hist.prh_per_date, 'yyyy-MM-dd'),F.to_date(prh_hist.prh_rcp_date, 'yyyy-MM-dd')) <= -15),F.lit(2)). \
when((F.datediff(F.to_date(prh_hist.prh_per_date, 'yyyy-MM-dd'),F.to_date(prh_hist.prh_rcp_date, 'yyyy-MM-dd')) >= -14) & (F.datediff(F.to_date(prh_hist.prh_per_date, 'yyyy-MM-dd'),F.to_date(prh_hist.prh_rcp_date, 'yyyy-MM-dd')) <= -8),F.lit(3)). \
when((F.datediff(F.to_date(prh_hist.prh_per_date, 'yyyy-MM-dd'),F.to_date(prh_hist.prh_rcp_date, 'yyyy-MM-dd')) >= -7) & (F.datediff(F.to_date(prh_hist.prh_per_date, 'yyyy-MM-dd'),F.to_date(prh_hist.prh_rcp_date, 'yyyy-MM-dd')) <= -1),F.lit(4)). \
when(F.datediff(F.to_date(prh_hist.prh_per_date, 'yyyy-MM-dd'),F.to_date(prh_hist.prh_rcp_date, 'yyyy-MM-dd')) == 0,F.lit(5)). \
when((F.datediff(F.to_date(prh_hist.prh_per_date, 'yyyy-MM-dd'),F.to_date(prh_hist.prh_rcp_date, 'yyyy-MM-dd')) >= 1) & (F.datediff(F.to_date(prh_hist.prh_per_date, 'yyyy-MM-dd'),F.to_date(prh_hist.prh_rcp_date, 'yyyy-MM-dd')) <= 7),F.lit(6)). \
when((F.datediff(F.to_date(prh_hist.prh_per_date, 'yyyy-MM-dd'),F.to_date(prh_hist.prh_rcp_date, 'yyyy-MM-dd')) >= 8) & (F.datediff(F.to_date(prh_hist.prh_per_date, 'yyyy-MM-dd'),F.to_date(prh_hist.prh_rcp_date, 'yyyy-MM-dd')) <= 14),F.lit(7)). \
when((F.datediff(F.to_date(prh_hist.prh_per_date, 'yyyy-MM-dd'),F.to_date(prh_hist.prh_rcp_date, 'yyyy-MM-dd')) >= 15) & (F.datediff(F.to_date(prh_hist.prh_per_date, 'yyyy-MM-dd'),F.to_date(prh_hist.prh_rcp_date, 'yyyy-MM-dd')) <= 21),F.lit(8)). \
when(F.datediff(F.to_date(prh_hist.prh_per_date, 'yyyy-MM-dd'),F.to_date(prh_hist.prh_rcp_date, 'yyyy-MM-dd')) > 21,F.lit(9)).alias('DeliveryprecisionID'), \
prh_hist.prh_site.alias('SiteCode'), \
prh_hist.prh_domain.alias('Domain'), \
prh_hist.prh_rcp_date.alias('TimeID'), \
prh_hist.prh_per_date.alias('TimeIdPerform'), \
prh_hist.prh_ship_date.alias('TimeIdShip'), \
F.when(po_mstr.po_ord_date.isNull(), prh_hist.prh_rcp_date).otherwise(po_mstr.po_ord_date).alias('OrderTimeId'), \
F.datediff(F.to_date(prh_hist.prh_per_date, 'yyyy-MM-dd'),F.to_date(prh_hist.prh_rcp_date, 'yyyy-MM-dd')).alias('DaysLate'), \
prh_hist.prh_nbr.alias('PONumber'), \
prh_hist.prh_line.alias('POLine'), \
F.lit('1').alias('NoLines'), \
prh_hist.prh_rcvd.alias('QtyReceived'), \
prh_hist.prh_qty_ord.alias('QtyOrdered'), \
F.datediff(F.to_date(po_mstr.po_ord_date, 'yyyy-MM-dd'),F.to_date(prh_hist.prh_rcp_date, 'yyyy-MM-dd')).alias('CycleDays'), \
prh_hist.prh_pur_cost.alias('UnitCost'), \
F.round((prh_hist.prh_pur_std.cast("double") * (F.regexp_replace(DimCurrency.ExchangeRate, ',', '.').cast("double"))),3).alias('POStandardCost'), \
F.round(prh_hist.prh_pur_cost.cast("double") * prh_hist.prh_rcvd.cast("double") * (F.regexp_replace(DimCurrency.ExchangeRate, ',', '.').cast("double")),3).alias('ReceiptCost'), \
F.round(prh_hist.prh_pur_std.cast("double") * prh_hist.prh_rcvd.cast("double") * (F.regexp_replace(DimCurrency.ExchangeRate, ',', '.').cast("double")),3).alias('StandardReceiptCost'), \
prh_hist.prh_curr.alias('POCurrency'), \
pod_det.pod_type.alias('POType'), \
F.when(((prh_hist.prh_rcvd.cast("double") < 0 ) & (prh_hist.prh_rcp_type == "R")) , F.lit('RO')).when((prh_hist.prh_rcvd.cast("double") > 0) & (prh_hist.prh_rcp_type == "R") , F.lit('RI')).otherwise(F.when(prh_hist.prh_rcp_type.isNull(),F.lit("P")).otherwise(prh_hist.prh_rcp_type)).alias('POReturnType'), \
F.when(prh_hist.prh_reason.isNull(), F.lit('No return reason entered')).otherwise(prh_hist.prh_reason).alias('Time_Id_Order'), \
po_mstr.po_shipvia.alias('ShippingMode'), \
prh_hist.prh_part.alias('PartID'), \
DimSupplier.SupplierName.alias('SupplierName'), \
DimCurrency.ExchangeRate.alias('ExchangeRate'), \
F.round((prh_hist.prh_curr_amt.cast("double") * (prh_hist.prh_rcvd.cast("double"))),3).alias('ReceiptCostOriginalCurrency'), \
prh_hist.prh_vend.alias('VendorID'), \
prh_hist.prh_curr_amt.alias('POCurrencyAmount'), \
F.round((DimPartMaster.Metric_Ship_Weight.cast("double")) * (prh_hist.prh_rcvd.cast("double")),3).alias('Weight'), \
F.round((DimPartMaster.Length.cast("double")) * (DimPartMaster.Width.cast("double")) * (DimPartMaster.Height.cast("double")) * (prh_hist.prh_rcvd.cast("double")),3).alias('Volume'))


# COMMAND ----------

join.show()

# COMMAND ----------

FactPurchasing.write.format("com.databricks.spark.csv").option("header", "true").option("delimiter", ",").option("quoteAll", "true").mode('overwrite').save("wasbs://adlsdev@badevadlsg2.blob.core.windows.net/Governed Data/Sources/LMAS/QAD/Staging/FactPurchasing")

# COMMAND ----------

Files = dbutils.fs.ls("wasbs://adlsdev@badevadlsg2.blob.core.windows.net/Governed Data/Sources/LMAS/QAD/Staging/FactPurchasing/")

for lst in Files:
  for index,File in enumerate(lst):
    if index == 0:
      if File.endswith(".csv"):
        print(File)
      else:
        dbutils.fs.rm(File)
