-- Databricks notebook source
-- event_qcpr_manikin
drop view if exists __simpad_session;
create temporary view __simpad_session as
with t0 as
(
  select
    se.*,
    eqm.vent_count as manikin_vent_count,
    eqm.comp_count as manikin_comp_count,
    ecm.vent_count as prod_vent_count,
    ecm.comp_count as prod_comp_count,
    max(eqm.ser_num) over(partition by session_id) as ser_num
  from
    simpad_prod.session_event as se
      left join
    simpad_prod.event_qcpr_manikin as eqm on se.event_id = eqm.event_id
      left join
    simpad_prod.event_cpu_module as ecm on se.event_id = ecm.event_id
)
select
  session_id,
  ser_num as serial_number,
  min(time_stamp) as time_stamp,
  max(manikin_vent_count) as ventilations,
  max(manikin_comp_count) as compressions,
  max(manikin_vent_count) - lag(max(manikin_vent_count)) over(partition by ser_num order by min(time_stamp)) as ventilations_delta,
  max(manikin_comp_count) - lag(max(manikin_comp_count)) over(partition by ser_num order by min(time_stamp)) as compressions_delta
from
  t0
group by
  session_id,
  ser_num
having
  ser_num is not null and ser_num <> '0000000000';

-- COMMAND ----------

select *
from
  __simpad_session
order by
  serial_number,
  time_stamp;

-- COMMAND ----------

-- event_qcpr_manikin
drop view if exists __simpad_asset;
create temporary view __simpad_asset as
select
  serial_number,
  max(time_stamp) as last_known_session_time_stamp,
  datediff(max(time_stamp), min(time_stamp)) as age,
  max(ventilations) as ventilations,
  max(compressions) as compressions
from
  __simpad_session
group by
  serial_number;

-- COMMAND ----------

select *
from
  __simpad_asset;

-- COMMAND ----------

-- percentage of assets having address information in QAD. 18% is not that much but it is better than nothing.
select
  count(distinct eqm.ser_num) as serials_count,
  count(distinct qs.serial_number) as has_address_in_qad_count,
  count(distinct qs.serial_number) / count(distinct eqm.ser_num) * 100.0 as has_address_in_qad_pct
from
  simpad_prod.event_qcpr_manikin as eqm
    left join
  qad_shipment as qs on eqm.ser_num = qs.serial_number
where
  ser_num is not null
  and ser_num <> '';

-- COMMAND ----------

-- let's look at the addresses in QAD. Looks good!
with
-- unique serials of lleap-instructor
simpad_serials as
(
  select distinct ser_num from simpad_prod.event_qcpr_manikin
),
-- last known address of serial number (asset) will have rn = 1
qad_shipment_ranked as
(
  select *, row_number() over(partition by serial_number order by shipment_date) as rn
  from qad_shipment
)
select distinct
  lis.ser_num, qs.*
from
  simpad_serials as lis
    inner join
  qad_shipment_ranked as qs on lis.ser_num = qs.serial_number
where
  qs.rn = 1
  and ser_num is not null
  and ser_num <> '';  

-- COMMAND ----------

-- percentage of assets having address information in Sales Force. There are only 0.07%.
select
  count(distinct eas.ser_num) as serials_count,
  count(distinct sfa.serial_number) as has_address_in_sales_force_count,
  count(distinct sfa.serial_number) / count(distinct eas.ser_num) * 100.0 as has_address_in_sales_force_pct
from
  simpad_prod.event_qcpr_manikin as eas
    left join
  sf_assets as sfa on eas.ser_num = sfa.serial_number
where
  ser_num is not null
  and ser_num <> '';

-- COMMAND ----------

-- let's look at the addresses in Sales Force.
with
-- unique serials of lleap-instructor
simpad_serials as
(
  select distinct ser_num from simpad_prod.event_qcpr_manikin
)
select
  lis.ser_num, sfa.serial_number, sfacc.*
from
  simpad_serials as lis
    inner join
  sf_assets as sfa on lis.ser_num = sfa.serial_number
    inner join
  sf_accounts as sfacc on sfa.account_id = sfacc.id
where
  ser_num is not null
  and ser_num <> '';

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC -- percentage of assets having maintenance information in QAD. There are only 4 assets hace maintenance tracked in QAD.
-- MAGIC select
-- MAGIC   count(distinct eas.ser_num) as serials_count,
-- MAGIC   count(distinct sfa.serial_number) as has_address_in_sales_force_count,
-- MAGIC   count(distinct sfa.serial_number) / count(distinct eas.ser_num) * 100.0 as has_address_in_sales_force_pct
-- MAGIC from
-- MAGIC   simpad_prod.event_qcpr_manikin as eas
-- MAGIC     left join
-- MAGIC   qad_service_requests as sfa on eas.ser_num = sfa.serial_number
-- MAGIC where
-- MAGIC   ser_num is not null
-- MAGIC   and ser_num <> '';

-- COMMAND ----------

-- let's look at the maintenance information in QAD
with
-- unique serials of lleap-instructor
simpad_serials as
(
  select distinct ser_num from simpad_prod.event_qcpr_manikin
)
select
  lis.ser_num, sfa.*
from
  simpad_serials as lis
    inner join
  qad_service_requests as sfa on lis.ser_num = sfa.serial_number
where
  ser_num is not null
  and ser_num <> '';

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC select *
-- MAGIC from
-- MAGIC   __simpad_asset;

-- COMMAND ----------

select
  ser_num,
  max(time_stamp) as time_stamp,
  max(vent_count) as vent_count,
  max(comp_count) as comp_count
from
  simpad_prod.session_event as se
    inner join
  simpad_prod.event_qcpr_manikin as em on se.event_id = em.event_id
group by
  ser_num
order by
  time_stamp desc

-- COMMAND ----------


