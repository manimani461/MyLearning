# Databricks notebook source
# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql.functions import col, udf
from datetime import datetime

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS lleap_poc.session_event;
# MAGIC DROP TABLE IF EXISTS lleap_poc.device;
# MAGIC DROP TABLE IF EXISTS lleap_poc.location;
# MAGIC DROP TABLE IF EXISTS lleap_poc.fact_table;
# MAGIC DROP TABLE IF EXISTS lleap_poc.simulator;
# MAGIC DROP TABLE IF EXISTS lleap_poc.event_application_start;
# MAGIC DROP TABLE IF EXISTS lleap_poc.event_application_duration;
# MAGIC DROP TABLE IF EXISTS lleap_poc.event_client_status_collection;
# MAGIC DROP TABLE IF EXISTS lleap_poc.event_simulator_hardware_statistics;
# MAGIC DROP TABLE IF EXISTS lleap_poc.event_debrief_system_xxxxx;
# MAGIC DROP TABLE IF EXISTS lleap_poc.event_sdk_lleap_plugin_loaded;
# MAGIC DROP TABLE IF EXISTS lleap_poc.event_sdk_parameter_used;
# MAGIC DROP TABLE IF EXISTS lleap_poc.event_session_start_fail;
# MAGIC DROP TABLE IF EXISTS lleap_poc.event_session_xxxxx;
# MAGIC DROP TABLE IF EXISTS lleap_poc.event_simulator_connection_xxxxx;
# MAGIC DROP TABLE IF EXISTS lleap_poc.event_simulator_hardware_warning;
# MAGIC DROP TABLE IF EXISTS lleap_poc.event_simulator_status_collection;
# MAGIC DROP TABLE IF EXISTS lleap_poc.event_theme_switch;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS lleap_poc.session_event ( 
# MAGIC     session_id string,
# MAGIC     time_stamp timestamp,
# MAGIC     user_id string,
# MAGIC     event_id string,
# MAGIC     event_name string,
# MAGIC     event_int_id long
# MAGIC     );
# MAGIC     
# MAGIC CREATE TABLE IF NOT EXISTS lleap_poc.device (
# MAGIC     locale string,
# MAGIC     os_ver string,
# MAGIC     role_inst string,
# MAGIC     scr_res string,
# MAGIC     type string,
# MAGIC     device_id string
# MAGIC     );  
# MAGIC     
# MAGIC CREATE TABLE IF NOT EXISTS lleap_poc.simulator (
# MAGIC     sim_serial string,
# MAGIC     simulator_int_id long
# MAGIC     ); 
# MAGIC     
# MAGIC CREATE TABLE IF NOT EXISTS lleap_poc.location (
# MAGIC     city string,
# MAGIC     continent string,
# MAGIC     country string,
# MAGIC     province string,
# MAGIC     location_id string
# MAGIC     );
# MAGIC     
# MAGIC CREATE TABLE IF NOT EXISTS lleap_poc.fact_table (
# MAGIC     session_id string,
# MAGIC     time_stamp timestamp,
# MAGIC     user_id string,
# MAGIC     event_name string,
# MAGIC     location_id string,
# MAGIC     device_id string,
# MAGIC     event_int_id string,
# MAGIC     row_id long,
# MAGIC     simulator_int_id long
# MAGIC     );
# MAGIC 
# MAGIC CREATE TABLE IF NOT EXISTS lleap_poc.event_application_start (
# MAGIC     app_name string,
# MAGIC     basebrd_ser_num string,
# MAGIC     encl_ser_num string,
# MAGIC     inst_cult string,
# MAGIC     lang string,
# MAGIC     os_arch string,
# MAGIC     os_ver string,
# MAGIC     scr_res string,
# MAGIC     ser_num string,
# MAGIC     ver string,
# MAGIC     event_int_id long
# MAGIC     );
# MAGIC 
# MAGIC CREATE TABLE IF NOT EXISTS lleap_poc.event_application_duration (
# MAGIC     app_name string,
# MAGIC     elapsed_time_ms double,
# MAGIC     event_int_id long
# MAGIC     );
# MAGIC 
# MAGIC CREATE TABLE IF NOT EXISTS lleap_poc.event_client_status_collection (
# MAGIC     battery_status string,
# MAGIC     conn_mode string,
# MAGIC     wlan_strength double,
# MAGIC     event_int_id long
# MAGIC     );
# MAGIC 
# MAGIC CREATE TABLE IF NOT EXISTS lleap_poc.event_simulator_hardware_statistics (
# MAGIC     comp_name string,
# MAGIC     mac string,
# MAGIC     sim_name string,
# MAGIC     battery_1_connected double,
# MAGIC     battery_1_current double,
# MAGIC     battery_1_current_at_start double,
# MAGIC     battery_1_cycle_count double,
# MAGIC     battery_1_design_capacity double,
# MAGIC     battery_1_design_voltage double,
# MAGIC     battery_1_full_capacity double,
# MAGIC     battery_1_relative_charge double,
# MAGIC     battery_1_relative_charge_at_start double,
# MAGIC     battery_1_remaining_capacity double,
# MAGIC     battery_1_ser_num double,
# MAGIC     battery_1_emp_c double,
# MAGIC     battery_1_temp_at_start_c double,
# MAGIC     battery_1_time_to_empty double,
# MAGIC     battery_1_voltage double,
# MAGIC     battery_1_voltage_at_start double,
# MAGIC     battery_2_connected double,
# MAGIC     battery_2_current double,
# MAGIC     battery_2_current_at_start double,
# MAGIC     battery_2_cycle_cnt double,
# MAGIC     battery_2_design_capacity double,
# MAGIC     battery_2_design_voltage double,
# MAGIC     battery_2_full_capacity double,
# MAGIC     battery_2_relative_charge double,
# MAGIC     battery_2_relative_charge_at_start double,
# MAGIC     battery_2_remaining_capacity double,
# MAGIC     battery_2_ser_num double,
# MAGIC     battery_2_temp_c double,
# MAGIC     battery_2_temp_at_start_c double,
# MAGIC     battery_2_time_to_empty double,
# MAGIC     battery_2_voltage double,
# MAGIC     battery_2_voltage_at_start double,
# MAGIC     battery_remaining_minutes double,
# MAGIC     battery_remaining_minutes_at_start double,
# MAGIC     battery_remaining_pct double,
# MAGIC     battery_temp_c double,
# MAGIC     blood_amnt double,
# MAGIC     comp_num double,
# MAGIC     compressor_max_temp_board_since_replaced double,
# MAGIC     compressor_max_temp_ext_1_since_replaced double,
# MAGIC     compressor_max_temp_ext_2_since_replaced double,
# MAGIC     compressor_on_time double,
# MAGIC     compressor_on_time_limit double,
# MAGIC     compressor_on_time_sec_at_ext_1_below_50_c_since_replaced double,
# MAGIC     compressor_on_time_sec_at_ext_1_below_55_c_since_replaced double,
# MAGIC     compressor_on_time_sec_at_ext_1_below_60_c_since_replaced double,
# MAGIC     compressor_on_time_sec_at_ext_1_below_65_c_since_replaced double,
# MAGIC     compressor_on_time_sec_at_ext_1_below_70_c_since_replaced double,
# MAGIC     compressor_on_time_sec_at_ext_1_below_75_c_since_replaced double,
# MAGIC     compressor_on_time_sec_at_ext_1_below_80_c_since_replaced double,
# MAGIC     compressor_on_time_sec_at_ext_1_below_85_c_since_replaced double,
# MAGIC     compressor_on_time_sec_at_ext_1_below_90_c_since_replaced double,
# MAGIC     compressor_on_time_sec_at_ext_1_gt_eq_90_c_since_replaced double,
# MAGIC     compressor_on_time_sec_at_ext_2_below_50_c_since_replaced double,
# MAGIC     compressor_on_time_sec_at_ext_2_below_55_c_since_replaced double,
# MAGIC     compressor_on_time_sec_at_ext_2_below_60_c_since_replaced double,
# MAGIC     compressor_on_time_sec_at_ext_2_below_65_c_since_replaced double,
# MAGIC     compressor_on_time_sec_at_ext_2_below_70_c_since_replaced double,
# MAGIC     compressor_on_time_sec_at_ext_2_below_75_c_since_replaced double,
# MAGIC     compressor_on_time_sec_at_ext_2_below_80_c_since_replaced double,
# MAGIC     compressor_on_time_sec_at_ext_2_below_85_c_since_replaced double,
# MAGIC     compressor_on_time_sec_at_ext_2_below_90_c_since_replaced double,
# MAGIC     compressor_on_time_sec_at_ext_2_gt_eq_90_c_since_replaced double,
# MAGIC     compressor_on_time_sec_since_replaced double,
# MAGIC     compressor_on_time_sec_since_replaced_limit double,
# MAGIC     drugs_amnt double,
# MAGIC     on_time_batt double,
# MAGIC     on_time_pwr double,
# MAGIC     pwr_on_num double,
# MAGIC     som_check_disk_err_cnt double,
# MAGIC     shock_1_num double,
# MAGIC     shock_2_num double,
# MAGIC     shock_3_num double,
# MAGIC     shock_4_num double,
# MAGIC     time_since_start_ms double,
# MAGIC     vent_num double,
# MAGIC     event_int_id long
# MAGIC     );
# MAGIC     
# MAGIC CREATE TABLE IF NOT EXISTS lleap_poc.event_debrief_system_xxxxx (
# MAGIC     sys_ip string,
# MAGIC     sys_name string,
# MAGIC     sys_type string,
# MAGIC     sys_ver string,
# MAGIC     ses_run_id string,
# MAGIC     elapsed_time_ms double,
# MAGIC     event_int_id long
# MAGIC     );
# MAGIC 
# MAGIC CREATE TABLE IF NOT EXISTS lleap_poc.event_sdk_lleap_plugin_loaded (
# MAGIC     company string,
# MAGIC     file_version string,
# MAGIC     filename string,
# MAGIC     plugin_name string,
# MAGIC     product_version string,
# MAGIC     event_int_id long
# MAGIC     );
# MAGIC     
# MAGIC CREATE TABLE IF NOT EXISTS lleap_poc.event_sdk_parameter_used (
# MAGIC     name string,
# MAGIC     event_int_id long
# MAGIC     );
# MAGIC 
# MAGIC CREATE TABLE IF NOT EXISTS lleap_poc.event_session_start_fail (
# MAGIC     file_name string,
# MAGIC     manikin_type string,
# MAGIC     ses_start_result string,
# MAGIC     ses_type string,
# MAGIC     elapsed_time_ms double,
# MAGIC     event_int_id long
# MAGIC     );
# MAGIC 
# MAGIC CREATE TABLE IF NOT EXISTS lleap_poc.event_session_xxxxx (
# MAGIC     profile_settings string,
# MAGIC     ses_file_name string,
# MAGIC     ses_run_id string,
# MAGIC     ses_type string,
# MAGIC     sim_srv_type string,
# MAGIC     sim_type string,
# MAGIC     elapsed_time_ms double,
# MAGIC     event_int_id long
# MAGIC     );
# MAGIC     
# MAGIC CREATE TABLE IF NOT EXISTS lleap_poc.event_simulator_connection_xxxxx (
# MAGIC     err_msg string,
# MAGIC     sim_name string,
# MAGIC     sim_srv_type string,
# MAGIC     sim_type string,
# MAGIC     sim_app_ver string,
# MAGIC     sim_prot_ver string,
# MAGIC     elapsed_time_ms double,
# MAGIC     event_int_id long
# MAGIC     );
# MAGIC 
# MAGIC CREATE TABLE IF NOT EXISTS lleap_poc.event_simulator_hardware_warning (
# MAGIC     sim_name string,
# MAGIC     sim_srv_type string,
# MAGIC     sim_type string,
# MAGIC     warn_type string,
# MAGIC     event_int_id long
# MAGIC     );
# MAGIC     
# MAGIC CREATE TABLE IF NOT EXISTS lleap_poc.event_simulator_status_collection (
# MAGIC     ctl_unit_ser_num string,
# MAGIC     battery_status string,
# MAGIC     conn_mode string,
# MAGIC     sim_name string,
# MAGIC     srv_type string,
# MAGIC     sim_type string,
# MAGIC     sim_serial string,
# MAGIC     battery_lvl double,
# MAGIC     wlan_strength double,
# MAGIC     event_int_id long
# MAGIC     );
# MAGIC 
# MAGIC CREATE TABLE IF NOT EXISTS lleap_poc.event_theme_switch (
# MAGIC     file_name string,
# MAGIC     elapsed_time_ms double,
# MAGIC     event_int_id long
# MAGIC     );

# COMMAND ----------


