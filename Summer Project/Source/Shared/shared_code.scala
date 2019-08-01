// Databricks notebook source
import com.microsoft.azure.storage._;
import com.microsoft.azure.storage.blob._;
import collection.JavaConverters._;
import java.net.URI;
import java.util.Properties
import scala.xml.XML;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.functions._;
import org.apache.spark.sql.types.{DataType, StructType, StructField, StringType, IntegerType, DecimalType, LongType, BooleanType, TimestampType, ArrayType};
import org.apache.spark.sql.{SparkSession, DataFrame, Dataset};
import org.joda.time.format.{DateTimeFormat};
import org.joda.time.{DateTime, Days, DateTimeZone};
import org.xml.sax.SAXParseException;
import org.apache.spark.sql.functions._;
import scala.reflect.runtime.universe.{TypeTag};
import org.apache.spark.sql.expressions.UserDefinedFunction;

import scala.util.Try;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.{FileSystem, Path};
import org.apache.hadoop.io.IOUtils;
import java.io.IOException;

import org.apache.spark.sql.functions.col;
import org.apache.spark.sql.{Column};

/*
  SHARED CODE
*/

def listFiles (
  blobPrefix: String, 
  container: CloudBlobContainer
) : Iterable[URI] = {
  
  var files = container.listBlobs(blobPrefix, true).asScala;
  for (f <- files) yield f.getUri();
}

def getAzureBlobStorageContainer (
  azureBlobStorageConnStr: String, 
  containerName: String
) : CloudBlobContainer = {
  
  var storageAccount = CloudStorageAccount.parse(azureBlobStorageConnStr);
  var blobClient = storageAccount.createCloudBlobClient();
  var container = blobClient.getContainerReference(containerName);
  return container;
}

def readTextBlob (
  blobUri: URI, 
  container: CloudBlobContainer
) : String = {
  
  var blob = createBlobReference(blobUri, container);
  return blob.downloadText();
}

def readTextBlob (
  azureBlobStorageConnStr: String, 
  containerName: String, 
  blobUri: URI
) : String = {
  
  var container = getAzureBlobStorageContainer(azureBlobStorageConnStr, containerName);
  return readTextBlob(blobUri, container);
}

def saveTextBlob (
  azureBlobStorageConnStr: String, 
  containerName: String, 
  blobUri: URI, 
  text: String
) : Unit = {
  
  var container = getAzureBlobStorageContainer(azureBlobStorageConnStr, containerName);
  saveTextBlob(blobUri, container, text)
}

def saveTextBlob (
  blobUri: URI, 
  container: CloudBlobContainer, 
  text: String
) : Unit = {
  
  var blob = createBlobReference(blobUri, container);
  blob.uploadText(text);
}

def createBlobReference (
  blobUri: URI, 
  container: CloudBlobContainer
) : CloudBlockBlob = {
  
  var containerUri = container.getUri();
  var blobName = containerUri.relativize(blobUri).toString();
  return container.getBlockBlobReference(blobName);
}

def getBlobPrefixes(
  blobPrefix: String, 
  fromDate: DateTime, 
  untilDate: DateTime
) : Seq[String] = {
  
  val numberOfDays = Days.daysBetween(fromDate, untilDate).getDays();
  var blobPrefixes =
    for (day <- numberOfDays to (0, -1))
        yield String.format("%s/%s", blobPrefix, fromDate.plusDays(day).toString("yyyy-MM-dd"));
  
  blobPrefixes.toSeq;
}

/*
def getBlobPrefixes(blobPrefix: String, fromDate: DateTime, untilDate: DateTime) : Seq[String] = {
  val numberOfDays = Days.daysBetween(fromDate, untilDate).getDays();
  var blobPrefixes =
    for (day <- numberOfDays to (0, -1); hour <- 23 to (0, -1))
        yield String.format("%s/%s/%s", blobPrefix, fromDate.plusDays(day).toString("yyyy-MM-dd"), "%02d".format(hour));
  
  blobPrefixes.toSeq;
}
*/

def getBlobDataSet(
  sc: SparkContext, 
  spark: SparkSession,
  blobPrefix: String,
  fromDate: DateTime, 
  untilDate: DateTime,
  storAccConStr: String, 
  storAccContName: String
) : Dataset[String] = {
  
  var blobPrefixes = getBlobPrefixes(blobPrefix, fromDate, untilDate);
  
  sc
    .parallelize(blobPrefixes, blobPrefixes.length)
    .mapPartitions(partition => {
      var container = getAzureBlobStorageContainer(storAccConStr, storAccContName);
      partition
        .flatMap(blobPrefix => listFiles(blobPrefix, container))
        .map(blobUri => readTextBlob(blobUri, container))
        .flatMap(content => content.split("\\r?\\n"))
    })
    .toDF
    .as[String];
}

def getJsonDataSet(
  spark: SparkSession,
  ds: Dataset[String],
  schema: StructType
) : DataFrame = {
  
  spark
    .read
    .schema(schema)
    .option("mode", "FAILFAST")
    .json(ds);
}

def saveData(
  df: DataFrame, 
  tableName: String
) : Unit = {
  
  df
    .write
    .mode(SaveMode.Overwrite)
    .saveAsTable(tableName);
}

def mergeData(
  spark: SparkSession, 
  srcDbName: String, 
  srcTableName: String, 
  trgtDbName: String, 
  trgtTableName: String, 
  joinColName: String
) : Unit = {
  
  spark.sql(
    s"CREATE TABLE IF NOT EXISTS $trgtDbName.$trgtTableName USING DELTA AS SELECT * FROM $srcDbName.$srcTableName WHERE 1 = 2"
  );

  spark.sql(
    s"""
      INSERT INTO $trgtDbName.$trgtTableName
      SELECT src.*
      FROM
        $srcDbName.$srcTableName AS src
          LEFT ANTI JOIN
        $trgtDbName.$trgtTableName AS trgt ON src.$joinColName = trgt.$joinColName
    """
  );  
}

def inferDataSetSchema(
  spark: SparkSession, 
  ds: Dataset[String]
) : String = {
  
  spark
    .read
    .option("mode", "FAILFAST")
    .json(ds)
    .schema
    .prettyJson;
}

def inferAndPersistDataSetSchema(
  spark: SparkSession, 
  ds: Dataset[String], 
  schemaStorageAccConnStr: String, 
  schemaContName: String, 
  schemaFileName: String
) : Unit = {
  
  var schema = inferDataSetSchema(spark, ds);
  saveTextBlob(schemaStorageAccConnStr, schemaContName, new URI(schemaFileName), schema);
}

def dropAndCreateDatabases(
  stageDbName: String, 
  targetDbName: String
) : Unit = {
  
  spark.sql(s"DROP DATABASE IF EXISTS $stageDbName CASCADE");
  spark.sql(s"DROP DATABASE IF EXISTS $targetDbName CASCADE");
  spark.sql(s"CREATE DATABASE IF NOT EXISTS $stageDbName");
  spark.sql(s"CREATE DATABASE IF NOT EXISTS $targetDbName");
}

def readDataSetSchema(
  schemaStorageAccConnStr: String, 
  schemaContName: String, 
  schemaFileName: String
) : StructType = {

  var json = readTextBlob(schemaStorageAccConnStr, schemaContName, new URI(schemaFileName));
  DataType.fromJson(json).asInstanceOf[StructType];  
}

def InitEtl(
  spark: SparkSession, 
  sc: SparkContext, 
  blobPrefix: String,
  fromDate: DateTime, 
  untilDate: DateTime,
  storageAccConnStr: String,
  storageAccContName: String,
  schemaFileName: String,
  regenerateDataSetSchema: Boolean
) : DataFrame = {  
  var blobDs = getBlobDataSet(sc, spark, blobPrefix, fromDate, untilDate, storageAccConnStr, storageAccContName);

  if (regenerateDataSetSchema)
    inferAndPersistDataSetSchema(spark, blobDs, storageAccConnStr, storageAccContName, schemaFileName);

  var jsonSchema = readDataSetSchema(storageAccConnStr, storageAccContName, schemaFileName);
  getJsonDataSet(spark, blobDs, jsonSchema);
}

def extractAndMergeLocationData(df: DataFrame, stageDbName: String, stageTableNamePrefix: String, targetDbName: String): Unit = {
  // spark.sql(s"REFRESH TABLE $stageDbName.${stageTableNamePrefix}_location");
  
  df
    .selectExpr(
      "context.location.city as city",
      "context.location.continent as continent",
      "context.location.country as country",
      "context.location.province as province"
    )
    .selectExpr("*", "md5(concat(nvl(continent, ''), '|', nvl(country, ''), '|', nvl(province, ''), '|', nvl(city, ''))) as location_id")
    .distinct()
    .write
    .mode(SaveMode.Overwrite)
    .saveAsTable(s"$stageDbName.${stageTableNamePrefix}_location");

  mergeData(spark, stageDbName, s"${stageTableNamePrefix}_location", targetDbName, "location", "location_id");  
}

def extractAndMergeDeviceData(df: DataFrame, stageDbName: String, stageTableNamePrefix: String, targetDbName: String): Unit = {
  // spark.sql(s"REFRESH TABLE $stageDbName.${stageTableNamePrefix}_location");

  df
    .selectExpr(
      "context.device.locale as locale",
      "context.device.osVersion as os_ver",
      "context.device.roleInstance as role_inst",
      "context.device.screenResolution.value as scr_res",
      "context.device.type as type"
    )
    .selectExpr(
      "*",
      "md5(concat(nvl(type, ''), '|', nvl(role_inst, ''), '|', nvl(os_ver, ''), '|', nvl(locale, ''), '|', nvl(scr_res, ''))) as device_id"
    )
    .distinct()
    .write
    .mode(SaveMode.Overwrite)
    .saveAsTable(s"$stageDbName.${stageTableNamePrefix}_device");

  mergeData(spark, stageDbName, s"${stageTableNamePrefix}_device", targetDbName, "device", "device_id");
}

def extractAndMergeSessionEventData(df: DataFrame, stageDbName: String, stageTableNamePrefix: String, targetDbName: String): Unit = {
  df
    .withColumn("e", explode_outer($"event"))
    .selectExpr(
      "context.location.city as city",
      "context.location.continent as continent",
      "context.location.country as country",
      "context.location.province as province",
      "context.device.locale as locale",
      "context.device.osVersion as os_ver",
      "context.device.roleInstance as role_inst",
      "context.device.screenResolution.value as scr_res",
      "context.device.type as type",
      "context.data.eventTime as time_stamp",
      "context.user.anonId as user_id",
      "context.session.id as session_id",
      "internal.data.id as event_id",
      "e.name as event_name"
    )
    .selectExpr(
      "cast(time_stamp as timestamp) as time_stamp",
      "user_id",
      "session_id",
      "event_id",
      "event_name",
      "md5(concat(nvl(continent, ''), '|', nvl(country, ''), '|', nvl(province, ''), '|', nvl(city, ''))) as location_id",
      "md5(concat(nvl(type, ''), '|', nvl(role_inst, ''), '|', nvl(os_ver, ''), '|', nvl(locale, ''), '|', nvl(scr_res, ''))) as device_id"
    )
    .write
    .mode(SaveMode.Overwrite)
    .saveAsTable(s"$stageDbName.${stageTableNamePrefix}_session_event");

  mergeData(spark, stageDbName, s"${stageTableNamePrefix}_session_event", targetDbName, "session_event", "event_id");
}

def extractAndMergePageViewsData(df: DataFrame, stageDbName: String, stageTableNamePrefix: String, targetDbName: String): Unit = {
  df
    .withColumn("pv", explode_outer($"view"))
    .selectExpr(
      "context.location.city as city",
      "context.location.continent as continent",
      "context.location.country as country",
      "context.location.province as province",
      "context.device.locale as locale",
      "context.device.osVersion as os_ver",
      "context.device.roleInstance as role_inst",
      "context.device.screenResolution.value as scr_res",
      "context.device.type as type",
      "context.data.eventTime as time_stamp",
      "context.user.anonId as user_id",
      "context.session.id as session_id",
      "internal.data.id as page_view_id",
      "pv.name as page_name",
      "pv.durationMetric.value as duration"
    )
    .selectExpr(
      "cast(time_stamp as timestamp) as time_stamp",
      "user_id",
      "session_id",
      "page_view_id",
      "page_name",
      "duration",
      "md5(concat(nvl(continent, ''), '|', nvl(country, ''), '|', nvl(province, ''), '|', nvl(city, ''))) as location_id",
      "md5(concat(nvl(type, ''), '|', nvl(role_inst, ''), '|', nvl(os_ver, ''), '|', nvl(locale, ''), '|', nvl(scr_res, ''))) as device_id"
    )
    .write
    .mode(SaveMode.Overwrite)
    .saveAsTable(s"$stageDbName.${stageTableNamePrefix}_session_page_view");

  mergeData(spark, stageDbName, s"${stageTableNamePrefix}_session_page_view", targetDbName, "session_page_view", "page_view_id");
}

implicit class DataFrameFlattener(df: DataFrame) {
  def flattenSchema: DataFrame = {
    df.select(flatten(Nil, df.schema): _*)
  }

  protected def flatten(path: Seq[String], schema: DataType): Seq[Column] = schema match {
    case s: StructType => s.fields.flatMap(f => flatten(path :+ f.name, f.dataType))
    case other => col(path.map(n => s"`$n`").mkString(".")).as(path.mkString(".")) :: Nil
  }
}

// COMMAND ----------


