// Databricks notebook source
// MAGIC %scala
// MAGIC var accountsDF = spark
// MAGIC   .read
// MAGIC   .option("mode", "FAILFAST")
// MAGIC   .json("/mnt/data/exchange/sales-force/accounts.json")
// MAGIC   .selectExpr("Id as id", "cast(LastModifiedDate as timestamp) as shipment_date", "Name as address_name", "Physical_Address__c as location_address", "type", "ParentId as parent_id")
// MAGIC   .write
// MAGIC   .mode("overwrite")
// MAGIC   .saveAsTable("sf_accounts");

// COMMAND ----------

// MAGIC %scala
// MAGIC var accountsDF = spark
// MAGIC   .read
// MAGIC   .option("mode", "FAILFAST")
// MAGIC   .json("/mnt/data/exchange/sales-force/assets.json")
// MAGIC   .selectExpr("AccountId as account_id", "SerialNumber as serial_number")
// MAGIC   .write
// MAGIC   .mode("overwrite")
// MAGIC   .saveAsTable("sf_assets");

// COMMAND ----------


