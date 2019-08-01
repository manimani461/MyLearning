-- Databricks notebook source
-- MAGIC %sql
-- MAGIC select
-- MAGIC   l.country,
-- MAGIC   count(distinct session_id) as sessions
-- MAGIC from
-- MAGIC   lleap_instructor_prod.session_event as se
-- MAGIC     left join
-- MAGIC   lleap_instructor_prod.location as l on se.location_id = l.location_id
-- MAGIC group by
-- MAGIC   l.country
-- MAGIC order by
-- MAGIC   sessions desc

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC select *
-- MAGIC from
-- MAGIC   lleap_instructor_prod.session_event as e
-- MAGIC     left join
-- MAGIC   lleap_instructor_prod.event_application_start as eas on e.event_id = eas.event_id
-- MAGIC     left join 
-- MAGIC   lleap_instructor_prod.location as l on e.location_id = l.location_id
-- MAGIC     left join 
-- MAGIC   lleap_instructor_prod.device as d on e.device_id = d.device_id
-- MAGIC where
-- MAGIC   e.event_id = "1d5f7320-9526-11e7-9cec-1d83b5020c18"

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC select *
-- MAGIC from
-- MAGIC   lleap_instructor_prod.session_event as se
-- MAGIC     left join
-- MAGIC   lleap_instructor_prod.event_application_start as eas on se.event_id = eas.event_id
-- MAGIC     left join
-- MAGIC   lleap_instructor_prod.event_simulator_connection_xxxxx as esc on se.event_id = esc.event_id
-- MAGIC     left join
-- MAGIC   lleap_instructor_prod.event_debrief_system_xxxxx as eds on se.event_id = eds.event_id
-- MAGIC     left join
-- MAGIC   lleap_instructor_prod.event_session_xxxxx as esx on se.event_id = esx.event_id
-- MAGIC     left join
-- MAGIC   lleap_instructor_prod.event_client_status_collection as ecsc on se.event_id = ecsc.event_id
-- MAGIC     left join
-- MAGIC   lleap_instructor_prod.event_simulator_status_collection as essc on se.event_id = essc.event_id
-- MAGIC     left join
-- MAGIC   lleap_instructor_prod.event_application_duration as ead on se.event_id = ead.event_id
-- MAGIC where
-- MAGIC   se.session_id = '9ee03638-a190-4a33-bffe-ed3287b64602'
-- MAGIC order by
-- MAGIC   se.time_stamp

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC select count(1)
-- MAGIC from
-- MAGIC   lleap_instructor_prod.session_event

-- COMMAND ----------


