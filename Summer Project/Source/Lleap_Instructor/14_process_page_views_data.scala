// Databricks notebook source
// MAGIC %run ./10_settings

// COMMAND ----------

// MAGIC %run ../shared/shared_code

// COMMAND ----------

// MAGIC %run ../shared/workarounds

// COMMAND ----------

/*
  It is workaround. Please check description of __applyWorkaround_TableCanNotBeOverwritten in workarounds.
  Feel free to remove when it is not actual anymore.
*/
__applyWorkaround_TableCanNotBeOverwritten("lleap_instructor_stage","raw_delta_device")
__applyWorkaround_TableCanNotBeOverwritten("lleap_instructor_stage","raw_delta_location")
__applyWorkaround_TableCanNotBeOverwritten("lleap_instructor_stage","raw_delta_session_page_view")

// COMMAND ----------

var jsonDf = InitEtl(
  spark, 
  sc, 
  PageViewsBlobPrefix,
  FromDate, 
  UntilDate,
  StorageAccConnStr,
  StorageAccContName,
  PageViewsDataSetSchemaFileName,
  RegenerateDataSetSchema
);

// COMMAND ----------

var cachedJsonDf = jsonDf.cache();

// COMMAND ----------

extractAndMergeLocationData(cachedJsonDf, StageDbName, StageTableNamePrefix, TargetDbName);
extractAndMergeDeviceData(cachedJsonDf, StageDbName, StageTableNamePrefix, TargetDbName);
extractAndMergePageViewsData(cachedJsonDf, StageDbName, StageTableNamePrefix, TargetDbName);

// COMMAND ----------

cachedJsonDf.unpersist();

// COMMAND ----------


