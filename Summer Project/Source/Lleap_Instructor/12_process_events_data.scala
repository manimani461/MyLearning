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
__applyWorkaround_TableCanNotBeOverwritten("lleap_instructor_stage","raw_delta_event_application_duration")
__applyWorkaround_TableCanNotBeOverwritten("lleap_instructor_stage","raw_delta_event_application_start")
__applyWorkaround_TableCanNotBeOverwritten("lleap_instructor_stage","raw_delta_event_client_status_collection")
__applyWorkaround_TableCanNotBeOverwritten("lleap_instructor_stage","raw_delta_event_debrief_system_xxxxx")
__applyWorkaround_TableCanNotBeOverwritten("lleap_instructor_stage","raw_delta_event_sdk_lleap_plugin_loaded")
__applyWorkaround_TableCanNotBeOverwritten("lleap_instructor_stage","raw_delta_event_sdk_parameter_used")
__applyWorkaround_TableCanNotBeOverwritten("lleap_instructor_stage","raw_delta_event_session_start_fail")
__applyWorkaround_TableCanNotBeOverwritten("lleap_instructor_stage","raw_delta_event_session_xxxxx")
__applyWorkaround_TableCanNotBeOverwritten("lleap_instructor_stage","raw_delta_event_simulator_connection_xxxxx")
__applyWorkaround_TableCanNotBeOverwritten("lleap_instructor_stage","raw_delta_event_simulator_hardware_statistics")
__applyWorkaround_TableCanNotBeOverwritten("lleap_instructor_stage","raw_delta_event_simulator_hardware_warning")
__applyWorkaround_TableCanNotBeOverwritten("lleap_instructor_stage","raw_delta_event_simulator_status_collection")
__applyWorkaround_TableCanNotBeOverwritten("lleap_instructor_stage","raw_delta_event_theme_switch")
__applyWorkaround_TableCanNotBeOverwritten("lleap_instructor_stage","raw_delta_location")
__applyWorkaround_TableCanNotBeOverwritten("lleap_instructor_stage","raw_delta_session_event")

// COMMAND ----------

var jsonDf = InitEtl(
  spark, 
  sc, 
  EventsBlobPrefix,
  FromDate, 
  UntilDate,
  StorageAccConnStr,
  StorageAccContName,
  EventsDataSetSchemaFileName,
  RegenerateDataSetSchema
);

// COMMAND ----------

var cachedJsonDf = jsonDf.cache();

// COMMAND ----------

extractAndMergeLocationData(cachedJsonDf, StageDbName, StageTableNamePrefix, TargetDbName);
extractAndMergeDeviceData(cachedJsonDf, StageDbName, StageTableNamePrefix, TargetDbName);
extractAndMergeSessionEventData(cachedJsonDf, StageDbName, StageTableNamePrefix, TargetDbName);

// COMMAND ----------

var customDataDf = cachedJsonDf
  .withColumn("e", explode_outer($"event"))
  .withColumn("cd", explode_outer($"context.custom.dimensions"))
  .withColumn("cm", explode_outer($"context.custom.metrics"))
  .withColumn("event_name", $"e.name")
  .withColumn("event_id", $"internal.data.id");

// COMMAND ----------

customDataDf
  .filter("event_name IN ('Session Duration', 'Session Start')")
  .groupBy($"event_id")
  .agg(
    max($"cd.`Profile Settings`") as "profile_settings",
    max($"cd.`Session Filename`") as "ses_file_name",
    max($"cd.`Session Run ID`") as "ses_run_id",
    max($"cd.`Session Type`") as "ses_type",
    max($"cd.`Simulator Server Type`") as "sim_srv_type",
    max($"cd.`Simulator Type`") as "sim_type",
    max($"cm.`Elapsed Time (ms)`.value") as "elapsed_time_ms"
  )
  .write
  .mode(SaveMode.Overwrite)
  .saveAsTable(s"$StageDbName.${StageTableNamePrefix}_event_session_xxxxx");

mergeData(spark, StageDbName, s"${StageTableNamePrefix}_event_session_xxxxx", TargetDbName, "event_session_xxxxx", "event_id");

// COMMAND ----------

customDataDf
  .filter(
    """
      event_name IN (
        'Debrief System Connect',
        'Debrief System Connect Fail',
        'Debrief System Connection Lost',
        'Debrief System Connection Restored',
        'Debrief System Open',
        'Debrief System Transfer Failed'
    )
    """
  )
  .groupBy($"event_id")
  .agg(
    max($"cd.`Debrief System IP`") as "sys_ip",
    max($"cd.`Debrief System Name`") as "sys_name",
    max($"cd.`Debrief System Type`") as "sys_type",
    max($"cd.`Debrief System Version`") as "sys_ver",
    max($"cd.`Session Run ID`") as "ses_run_id",
    max($"cm.`Elapsed Time (ms)`.value") as "elapsed_time_ms"
  )
  .write
  .mode(SaveMode.Overwrite)
  .saveAsTable(s"$StageDbName.${StageTableNamePrefix}_event_debrief_system_xxxxx");

mergeData(spark, StageDbName, s"${StageTableNamePrefix}_event_debrief_system_xxxxx", TargetDbName, "event_debrief_system_xxxxx", "event_id");

// COMMAND ----------

customDataDf
  .filter(
    """
      event_name IN (
        'Simulator Connect',
        'Simulator Connect Fail',
        'Simulator Connection Lost',
        'Simulator Connection Restored'
    )
    """
  )
  .groupBy($"event_id")
  .agg(
    max($"cd.`Error Message`") as "err_msg",
    max($"cd.`Simulator Name`") as "sim_name",
    max($"cd.`Simulator Server Type`") as "sim_srv_type",
    max($"cd.`Simulator Type`") as "sim_type",
    max($"cd.`Simulator Application Version`") as "sim_app_ver",
    max($"cd.`Simulator Protocol Version`") as "sim_prot_ver",
    max($"cm.`Elapsed Time (ms)`.value") as "elapsed_time_ms"
  )
  .write
  .mode(SaveMode.Overwrite)
  .saveAsTable(s"$StageDbName.${StageTableNamePrefix}_event_simulator_connection_xxxxx");

mergeData(spark, StageDbName, s"${StageTableNamePrefix}_event_simulator_connection_xxxxx", TargetDbName, "event_simulator_connection_xxxxx", "event_id");

// COMMAND ----------

customDataDf
  .filter("event_name IN ('Client Status Collection')")
  .groupBy($"event_id")
  .agg(
    max($"cd.`Client Battery Status`") as "battery_status",
    max($"cd.`Client Connection Mode`") as "conn_mode",
    max($"cm.`Client WLAN Strength`.value") as "wlan_strength"
  )
  .write
  .mode(SaveMode.Overwrite)
  .saveAsTable(s"$StageDbName.${StageTableNamePrefix}_event_client_status_collection");

mergeData(spark, StageDbName, s"${StageTableNamePrefix}_event_client_status_collection", TargetDbName, "event_client_status_collection", "event_id");

// COMMAND ----------

customDataDf
  .filter("event_name IN ('SDK LLEAP Plugin Loaded')")
  .groupBy($"event_id")
  .agg(
    max($"cd.`Company`") as "company",
    max($"cd.`File Version`") as "file_version",
    max($"cd.`Filename`") as "filename",
    max($"cd.`Plugin Name`") as "plugin_name",
    max($"cd.`Product Version`") as "product_version"
  )
  .write
  .mode(SaveMode.Overwrite)
  .saveAsTable(s"$StageDbName.${StageTableNamePrefix}_event_sdk_lleap_plugin_loaded");

mergeData(spark, StageDbName, s"${StageTableNamePrefix}_event_sdk_lleap_plugin_loaded", TargetDbName, "event_sdk_lleap_plugin_loaded", "event_id");

// COMMAND ----------

customDataDf
  .filter("event_name IN ('Simulator Status Collection')")
  .groupBy($"event_id")
  .agg(
    max($"cd.`ControlUnit Serial`") as "ctl_unit_ser_num",
    max($"cd.`Simulator Battery Status`") as "battery_status",
    max($"cd.`Simulator Connection Mode`") as "conn_mode",
    max($"cd.`Simulator Name`") as "sim_name",
    max($"cd.`Simulator Server Type`") as "srv_type",
    max($"cd.`Simulator Type`") as "sim_type",
    max($"cd.`Simulator Serial`") as "sim_serial",    
    max($"cm.`Simulator Battery Level`.value") as "battery_lvl",
    max($"cm.`Simulator WLAN Strength`.value") as "wlan_strength"
  )
  .write
  .mode(SaveMode.Overwrite)
  .saveAsTable(s"$StageDbName.${StageTableNamePrefix}_event_simulator_status_collection");

mergeData(spark, StageDbName, s"${StageTableNamePrefix}_event_simulator_status_collection", TargetDbName, "event_simulator_status_collection", "event_id");

// COMMAND ----------

customDataDf
  .filter("event_name IN ('SDK Parameter Used')")
  .groupBy($"event_id")
  .agg(
    max($"cd.`Name`") as "name"
  )
  .write
  .mode(SaveMode.Overwrite)
  .saveAsTable(s"$StageDbName.${StageTableNamePrefix}_event_sdk_parameter_used");

mergeData(spark, StageDbName, s"${StageTableNamePrefix}_event_sdk_parameter_used", TargetDbName, "event_sdk_parameter_used", "event_id");

// COMMAND ----------

customDataDf
  .filter("event_name IN ('Simulator Hardware Warning')")
  .groupBy($"event_id")
  .agg(
    max($"cd.`Simulator Name`") as "sim_name",
    max($"cd.`Simulator Server Type`") as "sim_srv_type",
    max($"cd.`Simulator Type`") as "sim_type",
    max($"cd.`Warning Type`") as "warn_type"
  )
  .write
  .mode(SaveMode.Overwrite)
  .saveAsTable(s"$StageDbName.${StageTableNamePrefix}_event_simulator_hardware_warning");

mergeData(spark, StageDbName, s"${StageTableNamePrefix}_event_simulator_hardware_warning", TargetDbName, "event_simulator_hardware_warning", "event_id");

// COMMAND ----------

customDataDf
  .filter("event_name IN ('Theme Switch')")
  .groupBy($"event_id")
  .agg(
    max($"cd.`Filename`") as "file_name",
    max($"cm.`Elapsed Time (ms)`.value") as "elapsed_time_ms"
  )
  .write
  .mode(SaveMode.Overwrite)
  .saveAsTable(s"$StageDbName.${StageTableNamePrefix}_event_theme_switch");

mergeData(spark, StageDbName, s"${StageTableNamePrefix}_event_theme_switch", TargetDbName, "event_theme_switch", "event_id");

// COMMAND ----------

customDataDf
  .filter("event_name IN ('Application Duration')")
  .groupBy($"event_id")
  .agg(
    max($"cd.`App Name`") as "app_name",
    max($"cm.`Elapsed Time (ms)`.value") as "elapsed_time_ms"
  )
  .write
  .mode(SaveMode.Overwrite)
  .saveAsTable(s"$StageDbName.${StageTableNamePrefix}_event_application_duration");

mergeData(spark, StageDbName, s"${StageTableNamePrefix}_event_application_duration", TargetDbName, "event_application_duration", "event_id");

// COMMAND ----------

customDataDf
  .filter("event_name IN ('Application Start')")
  .groupBy($"event_id")
  .agg(
    max($"cd.`App Name`") as "app_name",
    max($"cd.`Baseboard Serial Number`") as "basebrd_ser_num",
    max($"cd.`Enclosure Serial Number`") as "encl_ser_num",
    max($"cd.`Installed Culture`") as "inst_cult",
    max($"cd.`Language`") as "lang",
    max($"cd.`OS Architecture`") as "os_arch",
    max($"cd.`OS Version`") as "os_ver",
    max($"cd.`Screen Resolution`") as "scr_res",
    max($"cd.`Serial Number`") as "ser_num",
    max($"cd.`Version`") as "ver"
  )
  .write
  .mode(SaveMode.Overwrite)
  .saveAsTable(s"$StageDbName.${StageTableNamePrefix}_event_application_start");

mergeData(spark, StageDbName, s"${StageTableNamePrefix}_event_application_start", TargetDbName, "event_application_start", "event_id");

// COMMAND ----------

customDataDf
  .filter("event_name IN ('Session Start Fail')")
  .groupBy($"event_id")
  .agg(
    max($"cd.`Filename`") as "file_name",
    max($"cd.`Manikin Type`") as "manikin_type",
    max($"cd.`Session Start Result`") as "ses_start_result",
    max($"cd.`Session Type`") as "ses_type",
    max($"cm.`Elapsed Time (ms)`.value") as "elapsed_time_ms"
  )
  .write
  .mode(SaveMode.Overwrite)
  .saveAsTable(s"$StageDbName.${StageTableNamePrefix}_event_session_start_fail");

mergeData(spark, StageDbName, s"${StageTableNamePrefix}_event_session_start_fail", TargetDbName, "event_session_start_fail", "event_id");

// COMMAND ----------

customDataDf
  .filter("event_name IN ('Simulator Hardware Statistics')")
  .groupBy($"event_id")
  .agg(
    max($"cd.`ComputerName`") as "comp_name",
    max($"cd.`MAC`") as "mac",
    max($"cd.`Simulator Name`") as "sim_name",
    max($"cm.`Battery1Connected`.value") as "battery_1_connected",
    max($"cm.`Battery1Current`.value") as "battery_1_current",
    max($"cm.`Battery1CurrentAtStart`.value") as "battery_1_current_at_start",
    max($"cm.`Battery1CycleCount`.value") as "battery_1_cycle_count",
    max($"cm.`Battery1DesignCapacity`.value") as "battery_1_design_capacity",
    max($"cm.`Battery1DesignVoltage`.value") as "battery_1_design_voltage",
    max($"cm.`Battery1FullCapacity`.value") as "battery_1_full_capacity",
    max($"cm.`Battery1RelativeCharge`.value") as "battery_1_relative_charge",
    max($"cm.`Battery1RelativeChargeAtStart`.value") as "battery_1_relative_charge_at_start",
    max($"cm.`Battery1RemainingCapacity`.value") as "battery_1_remaining_capacity",
    max($"cm.`Battery1SerialNum`.value") as "battery_1_ser_num",
    max($"cm.`Battery1TemperatureCelsius`.value") as "battery_1_emp_c",
    max($"cm.`Battery1TemperatureCelsiusAtStart`.value") as "battery_1_temp_at_start_c",
    max($"cm.`Battery1TimeToEmpty`.value") as "battery_1_time_to_empty",
    max($"cm.`Battery1Voltage`.value") as "battery_1_voltage",
    max($"cm.`Battery1VoltageAtStart`.value") as "battery_1_voltage_at_start",
    max($"cm.`Battery2Connected`.value") as "battery_2_connected",
    max($"cm.`Battery2Current`.value") as "battery_2_current",
    max($"cm.`Battery2CurrentAtStart`.value") as "battery_2_current_at_start",
    max($"cm.`Battery2CycleCount`.value") as "battery_2_cycle_cnt",
    max($"cm.`Battery2DesignCapacity`.value") as "battery_2_design_capacity",
    max($"cm.`Battery2DesignVoltage`.value") as "battery_2_design_voltage",
    max($"cm.`Battery2FullCapacity`.value") as "battery_2_full_capacity",
    max($"cm.`Battery2RelativeCharge`.value") as "battery_2_relative_charge",
    max($"cm.`Battery2RelativeChargeAtStart`.value") as "battery_2_relative_charge_at_start",
    max($"cm.`Battery2RemainingCapacity`.value") as "battery_2_remaining_capacity",
    max($"cm.`Battery2SerialNum`.value") as "battery_2_ser_num",
    max($"cm.`Battery2TemperatureCelsius`.value") as "battery_2_temp_c",
    max($"cm.`Battery2TemperatureCelsiusAtStart`.value") as "battery_2_temp_at_start_c",
    max($"cm.`Battery2TimeToEmpty`.value") as "battery_2_time_to_empty",
    max($"cm.`Battery2Voltage`.value") as "battery_2_voltage",
    max($"cm.`Battery2VoltageAtStart`.value") as "battery_2_voltage_at_start",
    max($"cm.`BatteryRemainingMinutes`.value") as "battery_remaining_minutes",
    max($"cm.`BatteryRemainingMinutesAtStart`.value") as "battery_remaining_minutes_at_start",
    max($"cm.`BatteryRemainingPercent`.value") as "battery_remaining_pct",
    max($"cm.`BatteryTemperatureCelsius`.value") as "battery_temp_c",
    max($"cm.`BloodAmnt`.value") as "blood_amnt",
    max($"cm.`CompNum`.value") as "comp_num",
    max($"cm.`CompressorMaxTemperatureBoardSinceReplaced`.value") as "compressor_max_temp_board_since_replaced",
    max($"cm.`CompressorMaxTemperatureExt1SinceReplaced`.value") as "compressor_max_temp_ext_1_since_replaced",
    max($"cm.`CompressorMaxTemperatureExt2SinceReplaced`.value") as "compressor_max_temp_ext_2_since_replaced",
    max($"cm.`CompressorOnTime`.value") as "compressor_on_time",
    max($"cm.`CompressorOnTime.limit`.value") as "compressor_on_time_limit",
    max($"cm.`CompressorOnTimeSecAtExt1Below50CSinceReplaced`.value") as "compressor_on_time_sec_at_ext_1_below_50_c_since_replaced",
    max($"cm.`CompressorOnTimeSecAtExt1Below55CSinceReplaced`.value") as "compressor_on_time_sec_at_ext_1_below_55_c_since_replaced",
    max($"cm.`CompressorOnTimeSecAtExt1Below60CSinceReplaced`.value") as "compressor_on_time_sec_at_ext_1_below_60_c_since_replaced",
    max($"cm.`CompressorOnTimeSecAtExt1Below65CSinceReplaced`.value") as "compressor_on_time_sec_at_ext_1_below_65_c_since_replaced",
    max($"cm.`CompressorOnTimeSecAtExt1Below70CSinceReplaced`.value") as "compressor_on_time_sec_at_ext_1_below_70_c_since_replaced",
    max($"cm.`CompressorOnTimeSecAtExt1Below75CSinceReplaced`.value") as "compressor_on_time_sec_at_ext_1_below_75_c_since_replaced",
    max($"cm.`CompressorOnTimeSecAtExt1Below80CSinceReplaced`.value") as "compressor_on_time_sec_at_ext_1_below_80_c_since_replaced",
    max($"cm.`CompressorOnTimeSecAtExt1Below85CSinceReplaced`.value") as "compressor_on_time_sec_at_ext_1_below_85_c_since_replaced",
    max($"cm.`CompressorOnTimeSecAtExt1Below90CSinceReplaced`.value") as "compressor_on_time_sec_at_ext_1_below_90_c_since_replaced",
    max($"cm.`CompressorOnTimeSecAtExt1GreaterEqual90CSinceReplaced`.value") as "compressor_on_time_sec_at_ext_1_gt_eq_90_c_since_replaced",
    max($"cm.`CompressorOnTimeSecAtExt2Below50CSinceReplaced`.value") as "compressor_on_time_sec_at_ext_2_below_50_c_since_replaced",
    max($"cm.`CompressorOnTimeSecAtExt2Below55CSinceReplaced`.value") as "compressor_on_time_sec_at_ext_2_below_55_c_since_replaced",
    max($"cm.`CompressorOnTimeSecAtExt2Below60CSinceReplaced`.value") as "compressor_on_time_sec_at_ext_2_below_60_c_since_replaced",
    max($"cm.`CompressorOnTimeSecAtExt2Below65CSinceReplaced`.value") as "compressor_on_time_sec_at_ext_2_below_65_c_since_replaced",
    max($"cm.`CompressorOnTimeSecAtExt2Below70CSinceReplaced`.value") as "compressor_on_time_sec_at_ext_2_below_70_c_since_replaced",
    max($"cm.`CompressorOnTimeSecAtExt2Below75CSinceReplaced`.value") as "compressor_on_time_sec_at_ext_2_below_75_c_since_replaced",
    max($"cm.`CompressorOnTimeSecAtExt2Below80CSinceReplaced`.value") as "compressor_on_time_sec_at_ext_2_below_80_c_since_replaced",
    max($"cm.`CompressorOnTimeSecAtExt2Below85CSinceReplaced`.value") as "compressor_on_time_sec_at_ext_2_below_85_c_since_replaced",
    max($"cm.`CompressorOnTimeSecAtExt2Below90CSinceReplaced`.value") as "compressor_on_time_sec_at_ext_2_below_90_c_since_replaced",
    max($"cm.`CompressorOnTimeSecAtExt2GreaterEqual90CSinceReplaced`.value") as "compressor_on_time_sec_at_ext_2_gt_eq_90_c_since_replaced",
    max($"cm.`CompressorOnTimeSecSinceReplaced`.value") as "compressor_on_time_sec_since_replaced",
    max($"cm.`CompressorOnTimeSecSinceReplaced.limit`.value") as "compressor_on_time_sec_since_replaced_limit",
    max($"cm.`DrugsAmnt`.value") as "drugs_amnt",
    max($"cm.`OnTimeBatt`.value") as "on_time_batt",
    max($"cm.`OnTimePwr`.value") as "on_time_pwr",
    max($"cm.`PwrOnNum`.value") as "pwr_on_num",
    max($"cm.`SOMCheckDiskErrorCount`.value") as "som_check_disk_err_cnt",
    max($"cm.`Shock1Num`.value") as "shock_1_num",
    max($"cm.`Shock2Num`.value") as "shock_2_num",
    max($"cm.`Shock3Num`.value") as "shock_3_num",
    max($"cm.`Shock4Num`.value") as "shock_4_num",
    max($"cm.`TimeSinceStartMs`.value") as "time_since_start_ms",
    max($"cm.`VentNum`.value") as "vent_num"
  )
  .write
  .mode(SaveMode.Overwrite)
  .saveAsTable(s"$StageDbName.${StageTableNamePrefix}_event_simulator_hardware_statistics");

mergeData(spark, StageDbName, s"${StageTableNamePrefix}_event_simulator_hardware_statistics", TargetDbName, "event_simulator_hardware_statistics", "event_id");

// COMMAND ----------

cachedJsonDf.unpersist();

// COMMAND ----------


