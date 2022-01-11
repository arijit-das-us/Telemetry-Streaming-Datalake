// Databricks notebook source
// MAGIC %run ./Configuration

// COMMAND ----------

// MAGIC %sql
// MAGIC delete from IntelsatusageBronze where Datestamp < '2021-06-15';
// MAGIC delete from HubusageBronze where Datestamp < '2021-06-15';
// MAGIC delete from HubstatsBronze where Datestamp < '2021-06-15';
// MAGIC delete from HubstatusBronze where Datestamp < '2021-06-15';

// COMMAND ----------

// MAGIC %sql
// MAGIC delete from EvoBronze where Datestamp < '2021-06-15';
// MAGIC delete from AsmBronze where Datestamp < '2021-06-15';
// MAGIC delete from PeplinkBronze where Datestamp < '2021-06-15';
// MAGIC delete from CubicBronze where Datestamp < '2021-06-15';
// MAGIC delete from AsmBronze where Datestamp < '2021-06-15';

// COMMAND ----------

// MAGIC %sql
// MAGIC delete from MetricSilver2 where Datestamp < '2021-06-15';
// MAGIC --delete from MetricCustomJoinGold where Datestamp < '2021-06-15';
// MAGIC delete from MetricCustomJoinSum where Datestamp < '2021-06-15';