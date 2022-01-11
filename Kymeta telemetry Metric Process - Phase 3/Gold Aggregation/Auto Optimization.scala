// Databricks notebook source
// MAGIC %run ./Configuration

// COMMAND ----------

// all the meta tables
val sspcpath = basePath + "DeltaTable/sspc"

spark.sql(s"""
CREATE TABLE IF NOT EXISTS sspc 
  USING DELTA 
  OPTIONS (path = "$sspcpath")
  """)

val serviceplanpath = basePath + "DeltaTable/servicePlan"

spark.sql(s"""
CREATE TABLE IF NOT EXISTS servicePlan 
  USING DELTA 
  OPTIONS (path = "$serviceplanpath")
  """)

val terminalpath = basePath + "DeltaTable/terminal"

spark.sql(s"""
CREATE TABLE IF NOT EXISTS terminal 
  USING DELTA 
  OPTIONS (path = "$terminalpath")
  """)

val satelliterouterpath = basePath + "DeltaTable/satelliterouter"

spark.sql(s"""
CREATE TABLE IF NOT EXISTS satelliterouter 
  USING DELTA 
  OPTIONS (path = "$satelliterouterpath")
  """)

// Load the metric mapping table
val Device_History = basePath + "DeltaTable/remotedevicehistory"

spark.sql(s"""
CREATE TABLE IF NOT EXISTS RemoteDeviceHistory 
  USING DELTA 
  OPTIONS (path = "$Device_History")
  """)

// Load the metric mapping table
val Device_remotes = basePath + "DeltaTable/remotes"

spark.sql(s"""
CREATE TABLE IF NOT EXISTS RemoteDevice 
  USING DELTA 
  OPTIONS (path = "$Device_remotes")
  """)

// Load the metric mapping table

val metricmapping = basePath + "DeltaTable/metricmappings"

spark.sql(s"""
CREATE TABLE IF NOT EXISTS MetricMapping 
  USING DELTA 
  OPTIONS (path = "$metricmapping")
  """)

val metricprovider = basePath + "DeltaTable/metricproviders"

spark.sql(s"""
CREATE TABLE IF NOT EXISTS MetricProviders 
  USING DELTA 
  OPTIONS (path = "$metricprovider")
  """)


val leptonUniqueId = basePath + "DeltaTable/uniqueidmapping"

spark.sql(s"""
CREATE TABLE IF NOT EXISTS LeptonUniqueId 
  USING DELTA 
  OPTIONS (path = "$leptonUniqueId")
  """)

val custommetric = basePath + "DeltaTable/custommetrics"

spark.sql(s"""
CREATE TABLE IF NOT EXISTS CustomMetric 
  USING DELTA 
  OPTIONS (path = "$custommetric")
  """)

val metricsaggtype = basePath + "DeltaTable/metrics-aggtype"

spark.sql(s"""
CREATE TABLE IF NOT EXISTS MetricsAggtype 
  USING DELTA 
  OPTIONS (path = "$metricsaggtype")
  """)

// COMMAND ----------

val Intelsatusage_bronze = basePath + "DeltaTable/IntelsatUsage-bronze"

spark.sql(s"""
CREATE TABLE IF NOT EXISTS IntelsatusageBronze 
  USING DELTA 
  OPTIONS (path = "$Intelsatusage_bronze")
  """)

// Load the bronze table for Hub Usage
val Hubusage_bronze = basePath + "DeltaTable/Hubusage-bronze"

spark.sql(s"""
CREATE TABLE IF NOT EXISTS HubusageBronze 
  USING DELTA 
  OPTIONS (path = "$Hubusage_bronze")
  """)

// Load the bronze table for Hub Stats
val Hubstats_bronze = basePath + "DeltaTable/Hubstats-bronze"

spark.sql(s"""
CREATE TABLE IF NOT EXISTS HubstatsBronze 
  USING DELTA 
  OPTIONS (path = "$Hubstats_bronze")
  """)

// Load the bronze table for Hub Status
val Hubstatus_bronze = basePath + "DeltaTable/Hubstatus-bronze"

spark.sql(s"""
CREATE TABLE IF NOT EXISTS HubstatusBronze 
  USING DELTA 
  OPTIONS (path = "$Hubstatus_bronze")
  """)

// Load the bronze table for EVO
val Evo_bronze = basePath + "DeltaTable/EVO-bronze"

spark.sql(s"""
CREATE TABLE IF NOT EXISTS EvoBronze 
  USING DELTA 
  OPTIONS (path = "$Evo_bronze")
  """)

// Load the bronze table for ASM
val Asm_bronze = basePath + "DeltaTable/ASM-bronze"

spark.sql(s"""
CREATE TABLE IF NOT EXISTS AsmBronze 
  USING DELTA 
  OPTIONS (path = "$Asm_bronze")
  """)



// COMMAND ----------

// Load the bronze table for peplink
val Peplink_bronze = basePath + "DeltaTable/Peplink-bronze"

spark.sql(s"""
CREATE TABLE IF NOT EXISTS PeplinkBronze 
  USING DELTA 
  OPTIONS (path = "$Peplink_bronze")
  """)

// COMMAND ----------

// Load the bronze table for peplink
val Cubic_bronze = basePath + "DeltaTable/Cubic-bronze"

spark.sql(s"""
CREATE TABLE IF NOT EXISTS CubicBronze 
  USING DELTA 
  OPTIONS (path = "$Cubic_bronze")
  """)

// COMMAND ----------

// Load the silver 1 table for Intelsat Usage
val Intelsatusage_silver1 = basePath + "DeltaTable/IntelsatUsage-silver-step1"

spark.sql(s"""
CREATE TABLE IF NOT EXISTS IntelsatusageSilver1 
  USING DELTA 
  OPTIONS (path = "$Intelsatusage_silver1")
  """)

// Load the silver 1 table for Hub Usage
val Hubusage_silver1 = basePath + "DeltaTable/Hubusage-silver-step1"

spark.sql(s"""
CREATE TABLE IF NOT EXISTS HubusageSilver1 
  USING DELTA 
  OPTIONS (path = "$Hubusage_silver1")
  """)

// Load the silver 1 table for Hub Stats
val Hubstats_silver1 = basePath + "DeltaTable/Hubstats-silver-step1"

spark.sql(s"""
CREATE TABLE IF NOT EXISTS HubstatsSilver1 
  USING DELTA 
  OPTIONS (path = "$Hubstats_silver1")
  """)

// Load the silver 1 table for Hub Status
val Hubstatus_silver1 = basePath + "DeltaTable/Hubstatus-silver-step1"

spark.sql(s"""
CREATE TABLE IF NOT EXISTS HubstatusSilver1 
  USING DELTA 
  OPTIONS (path = "$Hubstatus_silver1")
  """)

// Load the silver 1 table for ASM
val ASM_silver1 = basePath + "DeltaTable/ASM-silver-step1"

spark.sql(s"""
CREATE TABLE IF NOT EXISTS ASMSilver1 
  USING DELTA 
  OPTIONS (path = "$ASM_silver1")
  """)

// Load the silver 1 table for EVO
val EVO_silver1 = basePath + "DeltaTable/EVO-silver-step1"

spark.sql(s"""
CREATE TABLE IF NOT EXISTS EVOSilver1 
  USING DELTA 
  OPTIONS (path = "$EVO_silver1")
  """)

// COMMAND ----------

// Load the silver 2 table
val Metric_silver2 = basePath + "DeltaTable/Metric-silver-step2"

spark.sql(s"""
CREATE TABLE IF NOT EXISTS MetricSilver2 
  USING DELTA 
  OPTIONS (path = "$Metric_silver2")
  """)

// Load the gold table
val Metric_gold = basePath + "DeltaTable/Metric-gold-raw"

spark.sql(s"""
CREATE TABLE IF NOT EXISTS MetricGold 
  USING DELTA 
  OPTIONS (path = "$Metric_gold")
  """)

// Load the gold table
val Metric_gold_custom_join = basePath + "DeltaTable/Metric-gold-custom-join"

spark.sql(s"""
CREATE TABLE IF NOT EXISTS MetricCustomJoinGold 
  USING DELTA 
  OPTIONS (path = "$Metric_gold_custom_join")
  """)



// COMMAND ----------

// Load the gold table
val Metric_gold_custom_sum = basePath + "DeltaTable/Metric-gold-custom-sum"

spark.sql(s"""
CREATE TABLE IF NOT EXISTS MetricCustomJoinSum 
  USING DELTA 
  OPTIONS (path = "$Metric_gold_custom_sum")
  """)

// COMMAND ----------

// Load the history gold table
val History_gold = basePath + "DeltaTable/History-gold-raw"

spark.sql(s"""
CREATE TABLE IF NOT EXISTS MetricHistoryGold 
  USING DELTA 
  OPTIONS (path = "$History_gold")
  """)


// COMMAND ----------

val metric_gold_Agg_Day = basePath + "DeltaTable/Metric-gold-Agg-Day"
spark.sql(s"""
CREATE TABLE IF NOT EXISTS MetricAggregatedByDay 
  USING DELTA 
  OPTIONS (path = "$metric_gold_Agg_Day")
  """)

val metric_gold_Agg_Month = basePath + "DeltaTable/Metric-gold-Agg-Month"
spark.sql(s"""
CREATE TABLE IF NOT EXISTS MetricAggregatedByMonth 
  USING DELTA 
  OPTIONS (path = "$metric_gold_Agg_Month")
  """)

val metric_gold_Agg_Hour = basePath + "DeltaTable/Metric-gold-Agg-Hour"
spark.sql(s"""
CREATE TABLE IF NOT EXISTS MetricAggregatedByHour 
  USING DELTA 
  OPTIONS (path = "$metric_gold_Agg_Hour")
  """)

// COMMAND ----------

// MAGIC %sql
// MAGIC ALTER TABLE sspc SET TBLPROPERTIES (delta.autoOptimize.optimizeWrite = true,delta.autoOptimize.autoCompact = true);
// MAGIC ALTER TABLE servicePlan SET TBLPROPERTIES (delta.autoOptimize.optimizeWrite = true,delta.autoOptimize.autoCompact = true);
// MAGIC ALTER TABLE terminal SET TBLPROPERTIES (delta.autoOptimize.optimizeWrite = true,delta.autoOptimize.autoCompact = true);
// MAGIC ALTER TABLE satelliterouter SET TBLPROPERTIES (delta.autoOptimize.optimizeWrite = true,delta.autoOptimize.autoCompact = true);
// MAGIC ALTER TABLE RemoteDeviceHistory SET TBLPROPERTIES (delta.autoOptimize.optimizeWrite = true,delta.autoOptimize.autoCompact = true);
// MAGIC ALTER TABLE RemoteDevice SET TBLPROPERTIES (delta.autoOptimize.optimizeWrite = true,delta.autoOptimize.autoCompact = true);
// MAGIC ALTER TABLE MetricProviders SET TBLPROPERTIES (delta.autoOptimize.optimizeWrite = true,delta.autoOptimize.autoCompact = true);
// MAGIC ALTER TABLE LeptonUniqueId SET TBLPROPERTIES (delta.autoOptimize.optimizeWrite = true,delta.autoOptimize.autoCompact = true);
// MAGIC ALTER TABLE CustomMetric SET TBLPROPERTIES (delta.autoOptimize.optimizeWrite = true,delta.autoOptimize.autoCompact = true);
// MAGIC ALTER TABLE MetricsAggtype SET TBLPROPERTIES (delta.autoOptimize.optimizeWrite = true,delta.autoOptimize.autoCompact = true);
// MAGIC ALTER TABLE MetricMapping SET TBLPROPERTIES (delta.autoOptimize.optimizeWrite = true,delta.autoOptimize.autoCompact = true);

// COMMAND ----------

// MAGIC %sql
// MAGIC ALTER TABLE IntelsatusageBronze SET TBLPROPERTIES (delta.autoOptimize.optimizeWrite = true);
// MAGIC ALTER TABLE HubusageBronze SET TBLPROPERTIES (delta.autoOptimize.optimizeWrite = true);
// MAGIC ALTER TABLE HubstatsBronze SET TBLPROPERTIES (delta.autoOptimize.optimizeWrite = true);
// MAGIC ALTER TABLE HubstatusBronze SET TBLPROPERTIES (delta.autoOptimize.optimizeWrite = true);
// MAGIC ALTER TABLE EvoBronze SET TBLPROPERTIES (delta.autoOptimize.optimizeWrite = true);
// MAGIC ALTER TABLE AsmBronze SET TBLPROPERTIES (delta.autoOptimize.optimizeWrite = true);

// COMMAND ----------

// MAGIC %sql
// MAGIC ALTER TABLE IntelsatusageSilver1 SET TBLPROPERTIES (delta.autoOptimize.optimizeWrite = true);
// MAGIC ALTER TABLE HubusageSilver1 SET TBLPROPERTIES (delta.autoOptimize.optimizeWrite = true);
// MAGIC ALTER TABLE HubstatsSilver1 SET TBLPROPERTIES (delta.autoOptimize.optimizeWrite = true);
// MAGIC ALTER TABLE HubstatusSilver1 SET TBLPROPERTIES (delta.autoOptimize.optimizeWrite = true);
// MAGIC ALTER TABLE ASMSilver1 SET TBLPROPERTIES (delta.autoOptimize.optimizeWrite = true);
// MAGIC ALTER TABLE EVOSilver1 SET TBLPROPERTIES (delta.autoOptimize.optimizeWrite = true);

// COMMAND ----------

// MAGIC %sql
// MAGIC ALTER TABLE MetricSilver2 SET TBLPROPERTIES (delta.autoOptimize.optimizeWrite = true);
// MAGIC ALTER TABLE MetricGold SET TBLPROPERTIES (delta.autoOptimize.optimizeWrite = true);
// MAGIC ALTER TABLE MetricCustomJoinGold SET TBLPROPERTIES (delta.autoOptimize.optimizeWrite = true);

// COMMAND ----------

// MAGIC %sql
// MAGIC ALTER TABLE MetricAggregatedByDay SET TBLPROPERTIES (delta.autoOptimize.optimizeWrite = true, delta.autoOptimize.autoCompact = true);
// MAGIC ALTER TABLE MetricAggregatedByMonth SET TBLPROPERTIES (delta.autoOptimize.optimizeWrite = true, delta.autoOptimize.autoCompact = true);
// MAGIC ALTER TABLE MetricAggregatedByHour SET TBLPROPERTIES (delta.autoOptimize.optimizeWrite = true, delta.autoOptimize.autoCompact = true);

// COMMAND ----------

// MAGIC %sql
// MAGIC --optimize MetricAggregatedByDay;
// MAGIC optimize MetricAggregatedByMonth;
// MAGIC optimize MetricAggregatedByHour;

// COMMAND ----------

// MAGIC %sql
// MAGIC ALTER TABLE MetricAggregatedByDay ADD COLUMNS (currentTimestamp bigint AFTER categoryId)
// MAGIC ALTER TABLE MetricAggregatedByMonth ADD COLUMNS (currentTimestamp bigint AFTER categoryId)
// MAGIC ALTER TABLE MetricAggregatedByHour ADD COLUMNS (currentTimestamp bigint AFTER categoryId)

// COMMAND ----------

// MAGIC %sql
// MAGIC select model, count(*) from satelliterouter
// MAGIC group by model

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from satelliterouter where model = 'idirect950mp'

// COMMAND ----------

// MAGIC %sql
// MAGIC select Model, count(*) from RemoteDeviceHistory
// MAGIC group by Model

// COMMAND ----------

// MAGIC %sql
// MAGIC select ModelType, count(*) from LeptonUniqueId
// MAGIC group by ModelType

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from LeptonUniqueId where ModelType='idirect950mp'

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from MetricsAggtype where id in (9006,9007)

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from MetricMapping

// COMMAND ----------

// MAGIC %sql
// MAGIC ALTER TABLE MetricMapping ADD COLUMNS (regex string AFTER rawSymbol)