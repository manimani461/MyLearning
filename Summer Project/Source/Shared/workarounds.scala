// Databricks notebook source
// MAGIC %scala
// MAGIC /*
// MAGIC   WORKAROUND: It solves the following problem
// MAGIC   - let you have table mydb.mytbl
// MAGIC   - let you have a code overwriting the table
// MAGIC     df
// MAGIC       .write
// MAGIC       .mode(SaveMode.Overwrite)
// MAGIC       .saveAsTable(tableName);
// MAGIC       
// MAGIC   - let the job was canceled by someone by some reason
// MAGIC   - it will fail on the next run with error: 
// MAGIC   
// MAGIC     org.apache.spark.sql.AnalysisException: Can not create the managed table('`mydb`.`mytbl`').
// MAGIC     The associated location('dbfs:/user/hive/warehouse/mydb.db/mytbl') already exists.;
// MAGIC   - the behaviour is due to bug in the underlying Apache Spark codebase (confirmed by Azure SUpport Team)
// MAGIC   - as a solution we can remove garbage file on filesystem before overwrite table
// MAGIC */
// MAGIC def __applyWorkaround_TableCanNotBeOverwritten(dbName: String, tableName: String) : Unit = {
// MAGIC   var warehouseDir = spark.conf.get("spark.sql.warehouse.dir");
// MAGIC   dbutils.fs.rm(s"$warehouseDir/$dbName.db/$tableName", true);
// MAGIC }

// COMMAND ----------


