// Databricks notebook source
// MAGIC %scala
// MAGIC 
// MAGIC var databases = Array("simpad_prod");
// MAGIC 
// MAGIC for (dbName <- databases)
// MAGIC {
// MAGIC   var tables = spark
// MAGIC     .catalog
// MAGIC     .listTables(dbName)
// MAGIC     .selectExpr("name")
// MAGIC     .collect();
// MAGIC   
// MAGIC   for (tbl <- tables)
// MAGIC   {
// MAGIC     var tblName = tbl.getString(0);
// MAGIC     
// MAGIC     spark.sql(s"analyze table simpad_prod.$tblName compute statistics");
// MAGIC     println (s"--describe extended simpad_prod.$tblName");
// MAGIC   }  
// MAGIC }

// COMMAND ----------

// MAGIC %sql
// MAGIC describe extended simpad_prod.session_page_view

// COMMAND ----------


