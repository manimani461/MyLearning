# Databricks notebook source
# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql.functions import col, udf, mean
from datetime import datetime


# COMMAND ----------

EVENT_TABLES = [
                'event_application_duration',
                'event_application_start',
                'event_client_status_collection',
                'event_debrief_system_xxxxx',
                'event_sdk_lleap_plugin_loaded',
                'event_sdk_parameter_used',
                'event_session_start_fail',
                'event_session_xxxxx',
                'event_simulator_connection_xxxxx',
                'event_simulator_hardware_statistics',
                'event_simulator_hardware_warning',
                'event_simulator_status_collection',
                'event_theme_switch'
]

NON_EVENT_TABLES = [
                'session_event',
                'device',
                'location',
                'fact_table',
                'fact_table1',
                'simulator'
]

# Load all non-event dataframes into a referrable dict
non_event_dfs = {}
for table in NON_EVENT_TABLES:
    non_event_dfs[table] = spark.table('lleap_poc.{}'.format(table))

# Need to add an integer id to table session_event
# All session_event rows are assumed unique on column event_id
non_event_dfs['session_event'] = non_event_dfs['session_event'].withColumn('event_int_id', F.monotonically_increasing_id())

# Load all event dataframes into a referrable dict
event_dfs = {}
for table in EVENT_TABLES:
    event_dfs[table] = spark.table('lleap_poc.{}'.format(table))

# COMMAND ----------

session_id = non_event_dfs['fact_table1'].select('session_id', 'time_stamp')

# COMMAND ----------

df = spark.sql('SELECT sim_serial, drugs_amnt, time_stamp from lleap_poc.simulator as sim JOIN lleap_poc.fact_table1 as ft on ft.simlator_int_id=sim.simulator_int_id JOIN lleap_poc.event_simulator_hardware_statistics as hw on hw.event_int_id=ft.event_int_id WHERE sim_serial=21228175631 ORDER BY time_stamp')

# COMMAND ----------

df.show(100, truncate=False)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT sim_serial, elapsed_time_ms, time_stamp 
# MAGIC FROM lleap_poc.simulator as sim JOIN lleap_poc.fact_table1 as ft on ft.simlator_int_id=sim.simulator_int_id JOIN lleap_poc.event_application_duration as ead on ead.event_int_id=ft.event_int_id 
# MAGIC WHERE sim_serial=21228175631 
# MAGIC ORDER BY time_stamp

# COMMAND ----------

# store last hardware stats of the serial numbers to dataframe
df = spark.sql('SELECT user_id, time_stamp, s1.* FROM (lleap_poc.simulator as sim JOIN lleap_poc.fact_table1 as ft on ft.simlator_int_id=sim.simulator_int_id JOIN lleap_poc.event_simulator_hardware_statistics as hw on hw.event_int_id=ft.event_int_id) as s1 WHERE time_stamp = (SELECT MAX(time_stamp) FROM (lleap_poc.simulator as sim JOIN lleap_poc.fact_table1 as ft on ft.simlator_int_id=sim.simulator_int_id JOIN lleap_poc.event_simulator_hardware_statistics as hw on hw.event_int_id=ft.event_int_id) as s2 WHERE s1.user_id = s2.user_id) ORDER BY user_id, time_stamp')

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT  s1.* 
# MAGIC FROM (lleap_poc.simulator as sim 
# MAGIC         JOIN lleap_poc.fact_table1 as ft on ft.simlator_int_id=sim.simulator_int_id 
# MAGIC         JOIN lleap_poc.event_simulator_hardware_statistics as hw on hw.event_int_id=ft.event_int_id) as s1
# MAGIC WHERE time_stamp = (SELECT MAX(time_stamp) FROM (lleap_poc.simulator as sim 
# MAGIC         JOIN lleap_poc.fact_table1 as ft on ft.simlator_int_id=sim.simulator_int_id 
# MAGIC         JOIN lleap_poc.event_simulator_hardware_statistics as hw on hw.event_int_id=ft.event_int_id) as s2 WHERE s1.user_id = s2.user_id)
# MAGIC ORDER BY user_id, time_stamp;

# COMMAND ----------

# Verify that the above function returns last hardware statistic event by comparing the result to the complete list:
%sql

SELECT   user_id,  on_time_pwr, time_stamp
FROM    (lleap_poc.simulator as sim 
        JOIN lleap_poc.fact_table1 as ft on ft.simlator_int_id=sim.simulator_int_id 
        JOIN lleap_poc.event_simulator_hardware_statistics as hw on hw.event_int_id=ft.event_int_id) as tb
ORDER BY user_id ASC , time_stamp DESC;




# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT sim_serial, on_time_pwr, time_stamp 
# MAGIC FROM lleap_poc.simulator as sim JOIN lleap_poc.fact_table1 as ft on ft.simlator_int_id=sim.simulator_int_id JOIN lleap_poc.event_simulator_hardware_statistics as hw on hw.event_int_id=ft.event_int_id 
# MAGIC WHERE sim_serial=21228175631 
# MAGIC ORDER BY time_stamp

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT user_id, time_stamp, compressor_on_time
# MAGIC FROM lleap_poc.fact_table1 as ft inner join lleap_poc.event_simulator_hardware_statistics as hw on hw.event_int_id = ft.event_int_id
# MAGIC WHERE user_id = '9AC089F2-1864-E411-8B14-E47FB276ACC7'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*)
# MAGIC FROM lleap_instructor_prod.session_event;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(DISTINCT event_id)
# MAGIC FROM lleap_instructor_prod.session_event;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM lleap_instructor_prod.session_event
# MAGIC WHERE user_id LIKE '024DEEE4-402E-E6FB-858E-4CEDFB77C45B' AND event_name LIKE 'Simulator Hardware Statistics'
# MAGIC ORDER BY time_stamp DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT time_stamp
# MAGIC FROM lleap_instructor_prod.session_event
# MAGIC ORDER BY time_stamp DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(session_id)
# MAGIC FROM ( SELECT   session_id, COUNT(*) as cnt
# MAGIC        FROM     lleap_poc.fact_table1
# MAGIC        WHERE    event_name LIKE 'SDK LLEAP%'
# MAGIC        GROUP BY session_id
# MAGIC        HAVING   cnt > 0 );

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(DISTINCT session_id)
# MAGIC FROM   lleap_poc.fact_table1;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT   session_id, COUNT(*)
# MAGIC FROM     lleap_poc.fact_table1
# MAGIC WHERE    event_name LIKE 'SDK LLEAP%'
# MAGIC GROUP BY session_id;

# COMMAND ----------


