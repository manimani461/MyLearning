// Databricks notebook source
import org.joda.time.format.{DateTimeFormat};
import org.joda.time.{DateTime, Days, DateTimeZone};
/*
  SETTINGS
  
  DON'T FORGET TO SWITCH TO USING SECRET SCOPES INSTEAD OF HARDCOED CREDENTIALS!
*/

var StorageAccConnStr = "DefaultEndpointsProtocol=https;AccountName=lleapaistorage;AccountKey=jCAFguFIpWiVl4GK/figqwOWV7ntnN3zmILMHjst/BKGE5IwnupaxQ1wYAx/mmNq4c3yIUe/LRopIaZWvs6NBg==;EndpointSuffix=core.windows.net";
var StorageAccContName = "lleap-ai-storage";
var EventsBlobPrefix = "lleap-instructorapplication-insights_a651b32868f34dae8390a5683a2888b9/Event";
var PageViewsBlobPrefix = "lleap-instructorapplication-insights_a651b32868f34dae8390a5683a2888b9/PageViews";

var TodayDate = new DateTime(DateTimeZone.UTC);
var FromDate = TodayDate.plusDays(-3);
var UntilDate = TodayDate;

var TargetDbName = "lleap_instructor_prod";
var StageDbName = "lleap_instructor_stage";
var StageTableNamePrefix = "raw_delta";

var EventsDataSetSchemaFileName = "spark/lleap-instructor-events-schema.json";
var PageViewsDataSetSchemaFileName = "spark/lleap-instructor-pageviews-schema.json";

var RegenerateDataSetSchema = false;
var RecreateDatabases = false;



// COMMAND ----------


