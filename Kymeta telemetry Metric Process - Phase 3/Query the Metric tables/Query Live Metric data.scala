// Databricks notebook source
// MAGIC %run ./Configuration

// COMMAND ----------

spark.conf.set(
  "fs.azure.account.key.kymetabigdata.blob.core.windows.net",
  "HcmXJ6Ks9sV44ef5JhFtIaAEIgL+EhIF8n1wG/an8wyyVuKe/CbPNrSsQuCN7dKy/f6he4cEBi6JGYuGT6//IA==")

// COMMAND ----------

val rawRecords_livemetric = spark.read.parquet(basePath + "BigDataTable/Metric-gold-live-raw")
rawRecords_livemetric.printSchema

// COMMAND ----------

display(rawRecords_livemetric)

// COMMAND ----------

val rawRecords_AggDay = spark.read.parquet(basePath + "BigDataTable/Metric-gold-Agg-Day")
rawRecords_AggDay.printSchema

// COMMAND ----------

display(rawRecords_AggDay)

// COMMAND ----------

val rawRecords_AggMonth = spark.read.parquet(basePath + "BigDataTable/Metric-gold-Agg-Month")
rawRecords_AggMonth.printSchema

// COMMAND ----------

display(rawRecords_AggMonth)

// COMMAND ----------

val rawRecords_AggHour = spark.read.parquet(basePath + "BigDataTable/Metric-gold-Agg-Hour")
rawRecords_AggHour.printSchema

// COMMAND ----------

display(rawRecords_AggHour)