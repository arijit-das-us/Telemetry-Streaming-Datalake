// Databricks notebook source
// MAGIC %run ./Configuration

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
// MAGIC ALTER TABLE MetricAggregatedByDay SET TBLPROPERTIES (delta.autoOptimize.optimizeWrite = true);
// MAGIC ALTER TABLE MetricAggregatedByMonth SET TBLPROPERTIES (delta.autoOptimize.optimizeWrite = true);
// MAGIC ALTER TABLE MetricAggregatedByHour SET TBLPROPERTIES (delta.autoOptimize.optimizeWrite = true);