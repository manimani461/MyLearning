// Databricks notebook source
import util.control.Breaks._
import org.apache.spark.sql.functions._

var databases = Array("lleap_instructor_prod", "lleap_lsh_prod");

for (dbName <- databases)
{
  var tables = spark
    .catalog
    .listTables(dbName)
    .selectExpr("name")
    .collect();
  
  for (tbl <- tables)
  {
    breakable {
      var tblName = tbl.getString(0);
      
      println(tblName);
      
      var isDelta = spark
        .sql(s"DESCRIBE EXTENDED $dbName.$tblName")
        .where(lower($"col_name") === "provider" && lower($"data_type") === "delta")
        .count();

      if (isDelta != 1)
        break;

      spark.sql(s"VACUUM $dbName.$tblName");
      spark.sql(s"OPTIMIZE $dbName.$tblName");
      spark.sql(s"ANALYZE TABLE $dbName.$tblName COMPUTE STATISTICS");          
    }
  }  
}

