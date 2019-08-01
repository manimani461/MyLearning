# Databricks notebook source
# DBTITLE 1,Imports
from pyspark.sql.types import *
from pyspark.sql import functions as F
from pyspark.sql.functions import col, udf, stddev, lit
from datetime import datetime
import warnings

# COMMAND ----------

# DBTITLE 1,Load dataframes from file
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
                'location'
]

# Load all non-event dataframes into a referrable dict
non_event_dfs = {}
for table in NON_EVENT_TABLES:
    non_event_dfs[table] = spark.read.format('delta') \
                                .load('/user/hive/warehouse/lleap_instructor_prod.db/{}/'.format(table))

# Need to add an integer id to table session_event
# All session_event rows are assumed unique on column event_id
non_event_dfs['session_event'] = non_event_dfs['session_event'].withColumn('event_int_id', F.monotonically_increasing_id())

# Load all event dataframes into a referrable dict
event_dfs = {}
for table in EVENT_TABLES:
    event_dfs[table] = spark.read.format('delta') \
                            .load('/user/hive/warehouse/lleap_instructor_prod.db/{}/'.format(table))

# COMMAND ----------

# DBTITLE 1,Create simulator dataframe/table
# We filter out all simulators that are not SimMan3G, as these are the only simulators
# that produce the data we are looking for.
# Since this table/dataframe is only used to do lookups on specific serial numbers, we only
# add an entry for simulators with valid serial numbers - this number is quite low: 55
def simulator_df():
    ''' Create simulator dataframe where there is a SimMan3G simulator that has a valid serial number '''
    simulator = event_dfs['event_simulator_status_collection'].select('sim_serial') \
                                                              .where("sim_type LIKE 'SimMan3G%' "
                                                                   + "AND sim_serial IS NOT NULL "
                                                                   + "AND sim_serial NOT LIKE '0'") \
                                                               .distinct() \
                                                               .withColumn('simulator_int_id', F.monotonically_increasing_id())

    return simulator

# COMMAND ----------

# DBTITLE 0,Create simulator dataframe
non_event_dfs['simulator'] = simulator_df()

# COMMAND ----------

# Used in order to get the simualtor_int_id for each session_id in session_event
# Known and reported bug creating some issues when joining dataframes that are derived from eachother,
# see this bug-report: https://issues.apache.org/jira/browse/SPARK-14948
# causes this to fail if it is done after the remove_non_simman3g() function
ser_nums = non_event_dfs['session_event'].join(event_dfs['event_simulator_status_collection'], 
                                               'event_id') \
                                         .join(non_event_dfs['simulator'], 
                                               'sim_serial') \
                                         .select('session_id', 'simulator_int_id')

# COMMAND ----------

# DBTITLE 1,Filter out non-SimMan3G data
def remove_non_simman3g(): 
    ''' Remove all non-SimMan3G simtype events '''
    # Retrieves all valid session_events, based on sessions where
    # sim_type/manikin_type is present in one of five events
    session_ids = non_event_dfs['session_event'].alias('A') \
                        .join(event_dfs['event_simulator_status_collection'],   'event_id', 'left') \
                        .join(event_dfs['event_session_start_fail'],            'event_id', 'left') \
                        .join(event_dfs['event_session_xxxxx'],                 'event_id', 'left') \
                        .join(event_dfs['event_simulator_connection_xxxxx'],    'event_id', 'left') \
                        .join(event_dfs['event_simulator_hardware_warning'],    'event_id', 'left') \
                        .filter(event_dfs['event_simulator_status_collection'].sim_type.like('SimMan3G%') 
                                | event_dfs['event_session_start_fail'].manikin_type.like('SimMan3G%') 
                                | event_dfs['event_session_xxxxx'].sim_type.like('SimMan3G%') 
                                | event_dfs['event_simulator_connection_xxxxx'].sim_type.like('SimMan3G%') 
                                | event_dfs['event_simulator_hardware_warning'].sim_type.like('SimMan3G%')) \
                        .select('A.session_id').distinct()

    # Updates session_event, containing only valid sessions
    non_event_dfs['session_event'] = session_ids.alias('B').join(non_event_dfs['session_event'].alias('A'),
                                                                 'session_id') \
                                                           .alias('A')

    # All other event tables needs to be updated with this new session_event table
    # A simple join on their event_int_id does the trick - necessary due to no
    # on delete cascade constraint in Hive DBs
    for event_df_name, event_df in event_dfs.items():
        result_df = event_df.alias(event_df_name) \
                            .join(non_event_dfs['session_event'],
                                                'event_id') \
                            .select('{}.*'.format(event_df_name), 'A.event_int_id') \
                            .drop('event_id')
        event_dfs[event_df_name] = result_df

# COMMAND ----------

remove_non_simman3g()

# COMMAND ----------

# DBTITLE 1,Define fact table
def fact_table_df():
    ''' Create fact table dataframe, including FKs from all dimension types '''
    # Builds a map from session_id to simulator_int_id, in order to fill the fact table
    # Beware of toPandas() loading the selected dataframe into memory
    ser_nums_panda = ser_nums.dropDuplicates().toPandas()
    ser_num_dict = {}
    for _, row in ser_nums_panda.iterrows():
        # session_id *can* point to None. Remember that a single session_id will have max
        # one serial number, but can have several None pointers
        if row['session_id'] not in ser_num_dict or ser_num_dict[row['session_id']] is None:
            ser_num_dict[row['session_id']] = row['simulator_int_id']

    # Define fact table df
    # There is one entry foreach entry in session_event, with some modified columns
    # -> using session_event as a baseline for this table... 
    fact_table = non_event_dfs['session_event'].withColumn('row_id', F.monotonically_increasing_id())
    fact_table = fact_table.drop('event_id')

    def simulator_id_from_session_id(session_id):
        ''' Return the corresponding simulator_int_id for a given session_id '''
        return ser_num_dict[session_id] if session_id in ser_num_dict else None

    # Fills in the correct simulator_int_id for each corresponding session_id
    f_udf = udf(simulator_id_from_session_id, LongType())
    fact_table = fact_table.withColumn('simulator_int_id', f_udf(fact_table.session_id))

    # Drop unecessary columns in session_event now moved to fact table
    non_event_dfs['session_event'] = non_event_dfs['session_event'].drop('device_id', 'location_id')

    return fact_table

# COMMAND ----------

non_event_dfs['fact_table'] = fact_table_df()

# COMMAND ----------

# DBTITLE 1,Persist data to hive database
def write_to_tables():
    ''' Write all dataframes to tables in lleap_poc '''

    def write(df, df_name):
        ''' Write dataframe df to lleap_poc.df_name table '''
        df.write.insertInto('lleap_poc.{}'.format(df_name), 'true')

    for event_df_name, event_df in event_dfs.items():
        write(event_df, event_df_name)

    for non_event_df_name, non_event_df in non_event_dfs.items():
        write(non_event_df, non_event_df_name)

# COMMAND ----------

write_to_tables()
