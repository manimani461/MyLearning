// Databricks notebook source
// MAGIC %scala
// MAGIC /* Don't forget to put all the credentials into Security Scope as it's done in 10_settings */
// MAGIC import java.util.Properties;
// MAGIC import org.apache.spark.sql.functions._
// MAGIC 
// MAGIC val jdbcQadHostname = "it-bi-sql.database.windows.net";
// MAGIC val jdbcQadPort = 1433;
// MAGIC val jdbcQadDatabase = "it-bi-sql";
// MAGIC val jdbcQadUsername = "Pro2Read";
// MAGIC val jdbcQadPassword = "######";
// MAGIC 
// MAGIC val jdbcQadConnectionProperties = new Properties();
// MAGIC val jdbcQadUrl = s"jdbc:sqlserver://${jdbcQadHostname}:${jdbcQadPort};database=${jdbcQadDatabase}";
// MAGIC jdbcQadConnectionProperties.put("user", s"${jdbcQadUsername}");
// MAGIC jdbcQadConnectionProperties.put("password", s"${jdbcQadPassword}");
// MAGIC 
// MAGIC val jdbcDriverClass = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
// MAGIC jdbcQadConnectionProperties.setProperty("Driver", jdbcDriverClass);

// COMMAND ----------

// MAGIC %scala
// MAGIC 
// MAGIC val query = """
// MAGIC select distinct   
// MAGIC     idhh.idh_serial as serial_number,
// MAGIC     ihh.ih_ship_date as shipment_date,
// MAGIC     concat(
// MAGIC       a.ad_name, ' ',
// MAGIC       a.ad_line1, ' ',
// MAGIC       a.ad_line2, ' ',
// MAGIC       a.ad_city, ' ',
// MAGIC       a.ad_state, ' ',
// MAGIC       a.ad_country, ' ',
// MAGIC       a.ad_zip, ' ',
// MAGIC       a.ad_phone, ' ',
// MAGIC       a.ad_phone2, ' '
// MAGIC     ) as location_address,
// MAGIC     a.ad_name as address_name,
// MAGIC     a.ad_line1 as address_line1,
// MAGIC     a.ad_line2 as address_line2,
// MAGIC     a.ad_city as address_city,
// MAGIC     a.ad_state as address_state,
// MAGIC     a.ad_country as address_country,
// MAGIC     a.ad_zip as address_zip,
// MAGIC     a.ad_phone as address_phone,
// MAGIC     a.ad_phone2 as address_phone2,
// MAGIC 
// MAGIC     cast(null as nvarchar(4000)) as asset_desc,
// MAGIC     cast(null as nvarchar(4000)) as asset_number,
// MAGIC     cast(null as nvarchar(4000)) as primary_account,
// MAGIC     cast(null as nvarchar(4000)) as primary_quad_num,
// MAGIC     cast(null as nvarchar(4000)) as product_code
// MAGIC     
// MAGIC from
// MAGIC     ih_hist as ihh
// MAGIC         inner hash join
// MAGIC     idh_hist as idhh on
// MAGIC             idh_domain = ih_domain
// MAGIC             and idh_inv_nbr = ih_inv_nbr
// MAGIC 		inner join
// MAGIC 	ad_mstr as a on ihh.ih_ship = a.ad_addr
// MAGIC """;
// MAGIC 
// MAGIC spark
// MAGIC   .read
// MAGIC   .jdbc(jdbcQadUrl, s"($query) as t", jdbcQadConnectionProperties)
// MAGIC   .write
// MAGIC   .mode(SaveMode.Overwrite)
// MAGIC   .saveAsTable("qad_shipment");

// COMMAND ----------

// MAGIC %scala
// MAGIC 
// MAGIC val query = """
// MAGIC   select
// MAGIC       ca_serial,
// MAGIC       ca_desc,
// MAGIC       ca_nxt_date, 
// MAGIC       ca_evt_date, 
// MAGIC       ca_opn_date, 
// MAGIC       ca_cls_date,
// MAGIC       ca_date_stmp,
// MAGIC       ca_eu_date,
// MAGIC       ca_comp_date,
// MAGIC       ca__qadt01
// MAGIC   from
// MAGIC       [dbo].[ca_mstr]
// MAGIC """;
// MAGIC 
// MAGIC val df = spark
// MAGIC   .read
// MAGIC   .jdbc(jdbcQadUrl, s"($query) as t", jdbcQadConnectionProperties)
// MAGIC   .withColumn(
// MAGIC     "__dates_list", 
// MAGIC     array(
// MAGIC       $"ca_nxt_date", 
// MAGIC       $"ca_evt_date", 
// MAGIC       $"ca_opn_date", 
// MAGIC       $"ca_date_stmp", 
// MAGIC       $"ca_eu_date", 
// MAGIC       $"ca_comp_date", 
// MAGIC       $"ca__qadt01"
// MAGIC     )
// MAGIC   )
// MAGIC   .withColumn("min_date", sort_array($"__dates_list")(0))
// MAGIC   .selectExpr("ca_serial as serial_number", "ca_desc as fail_desc", "min_date as open_date")
// MAGIC   .write
// MAGIC   .mode(SaveMode.Overwrite)
// MAGIC   .saveAsTable("qad_service_requests");
