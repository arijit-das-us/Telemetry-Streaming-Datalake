// Databricks notebook source
// MAGIC %run ./Configuration

// COMMAND ----------

spark.conf.set(
  "fs.azure.account.key.kymetabigdata.blob.core.windows.net",
  "HcmXJ6Ks9sV44ef5JhFtIaAEIgL+EhIF8n1wG/an8wyyVuKe/CbPNrSsQuCN7dKy/f6he4cEBi6JGYuGT6//IA==")

// COMMAND ----------

val rawRecords_livemetric = spark.read
  .format("delta")
  .load(basePath + "DeltaTable/Metric-gold-raw")
rawRecords_livemetric.printSchema

// COMMAND ----------

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

val rawRecords_livemetric_new = rawRecords_livemetric.select("unix_timestamp", "metric", "value", "Kymeta_metricId", "metricProviderId","RemoteId").withColumn("value", $"value".cast(DoubleType))

// COMMAND ----------

rawRecords_livemetric_new.printSchema

// COMMAND ----------

rawRecords_livemetric_new.write.mode("overwrite").parquet(basePath + "BigDataTable/Metric-gold-live-raw")

// COMMAND ----------

val MetricAggDay = spark.read
  .format("delta")
  .load(basePath + "DeltaTable/Metric-gold-Agg-Day")


// COMMAND ----------

val DfMetricAggDay = MetricAggDay.select("Datestamp","RemoteId", "Kymeta_metricId","sum_value","avg_value","min_value","max_value").withColumn("sum_value", $"sum_value".cast(DoubleType)).withColumn("avg_value", $"avg_value".cast(DoubleType)).withColumn("min_value", $"min_value".cast(DoubleType)).withColumn("max_value", $"max_value".cast(DoubleType))
DfMetricAggDay.printSchema

// COMMAND ----------

display(DfMetricAggDay)

// COMMAND ----------

DfMetricAggDay.write.mode("overwrite").parquet(basePath + "BigDataTable/Metric-gold-Agg-Day")

// COMMAND ----------

val rawRecords_AggMonth = spark.read
  .format("delta")
  .load(basePath + "DeltaTable/Metric-gold-Agg-Month")

// COMMAND ----------

val DfMetricAggMonth = rawRecords_AggMonth.select("YearMonth","RemoteId", "Kymeta_metricId","sum_value","avg_value","min_value","max_value").withColumn("sum_value", $"sum_value".cast(DoubleType)).withColumn("avg_value", $"avg_value".cast(DoubleType)).withColumn("min_value", $"min_value".cast(DoubleType)).withColumn("max_value", $"max_value".cast(DoubleType))
DfMetricAggMonth.printSchema

// COMMAND ----------

DfMetricAggMonth.write.mode("overwrite").parquet(basePath + "BigDataTable/Metric-gold-Agg-Month")

// COMMAND ----------

val rawRecords_AggHour = spark.read
  .format("delta")
  .load(basePath + "DeltaTable/Metric-gold-Agg-Hour")

// COMMAND ----------

val DfMetricAggHour = rawRecords_AggHour.select("YearMonthDayHour","RemoteId", "Kymeta_metricId","sum_value","avg_value","min_value","max_value").withColumn("sum_value", $"sum_value".cast(DoubleType)).withColumn("avg_value", $"avg_value".cast(DoubleType)).withColumn("min_value", $"min_value".cast(DoubleType)).withColumn("max_value", $"max_value".cast(DoubleType))
DfMetricAggHour.printSchema

// COMMAND ----------

DfMetricAggHour.write.mode("overwrite").parquet(basePath + "BigDataTable/Metric-gold-Agg-Hour")