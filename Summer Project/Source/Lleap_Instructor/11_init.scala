// Databricks notebook source
// MAGIC %run ./10_settings

// COMMAND ----------

// MAGIC %run ../shared/shared_code

// COMMAND ----------

if (RecreateDatabases)
  dropAndCreateDatabases(StageDbName, TargetDbName);

// COMMAND ----------


