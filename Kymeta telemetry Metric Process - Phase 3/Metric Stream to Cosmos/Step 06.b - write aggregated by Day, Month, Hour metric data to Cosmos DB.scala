// Databricks notebook source
// MAGIC %run ./Configuration

// COMMAND ----------

import org.joda.time._
import org.joda.time.format._

import com.microsoft.azure.cosmosdb.spark.schema._
import com.microsoft.azure.cosmosdb.spark.CosmosDBSpark
import com.microsoft.azure.cosmosdb.spark.config.Config
import com.microsoft.azure.cosmosdb.spark.streaming.CosmosDBSinkProvider


import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._


val dfAggDayMetric = spark.read
  .format("delta")
  .load(basePath + "DeltaTable/Metric-gold-Agg-Day")
//  .filter($"unixTimestamp" >= 1601510400)
  .filter($"currentTimestamp" >= unix_timestamp() - lit(1800))

// COMMAND ----------

val configMapAggByDay = Map(
  "Endpoint" -> cdbEndpoint,
  "Masterkey" -> cdbMasterkey,
  "Database" -> cdbDatabaseAgg,
  "Collection" -> cdbCollectionAggByDay,
  "preferredRegions" -> cdbRegions,
  "Upsert" -> "true",
  "WritingBatchSize" -> cdbBatchSize)
val configAggByDay = Config(configMapAggByDay)

// COMMAND ----------

val dfAggDayMetricCosmos = dfAggDayMetric
.withColumn("id",concat(col("remoteId"),lit('|'), col("unixTimestamp"),lit('|'), col("kymetaMetricId"),lit('|'),col("metricProviderId"),lit('|'),col("categoryId")))
.select("id","unixTimestamp","remoteId","kymetaMetricId","metricProviderId","categoryId","sumValue","avgValue","minValue","maxValue")
.withColumn("sumValue", $"sumValue".cast(DoubleType))
.withColumn("avgValue", $"avgValue".cast(DoubleType))
.withColumn("minValue", $"minValue".cast(DoubleType))
.withColumn("maxValue", $"maxValue".cast(DoubleType))
.coalesce(32)

// COMMAND ----------

//println(dfAggDayMetricCosmos.count())

// COMMAND ----------

import org.apache.spark.sql.SaveMode
dfAggDayMetricCosmos.write.mode(SaveMode.Overwrite).cosmosDB(configAggByDay)

// COMMAND ----------

val dfAggMonthMetric = spark.read
  .format("delta")
  .load(basePath + "DeltaTable/Metric-gold-Agg-Month")
 // .filter($"unixTimestamp" >= 1601510400)
  .filter($"currentTimestamp" >= unix_timestamp() - lit(3600))

// COMMAND ----------

val configMapAggByMonth = Map(
  "Endpoint" -> cdbEndpoint,
  "Masterkey" -> cdbMasterkey,
  "Database" -> cdbDatabaseAgg,
  "Collection" -> cdbCollectionAggByMonth,
  "preferredRegions" -> cdbRegions,
  "Upsert" -> "true",
  "WritingBatchSize" -> cdbBatchSize)
val configAggByMonth = Config(configMapAggByMonth)

// COMMAND ----------

val dfAggMonthMetricCosmos = dfAggMonthMetric
.withColumn("id",concat(col("remoteId"),lit('|'),col("unixTimestamp"),lit('|'), col("kymetaMetricId"),lit('|'),col("metricProviderId"),lit('|'),col("categoryId")))
.select("id","unixTimestamp","remoteId","kymetaMetricId","metricProviderId","categoryId","sumValue","avgValue","minValue","maxValue")
.withColumn("sumValue", $"sumValue".cast(DoubleType))
.withColumn("avgValue", $"avgValue".cast(DoubleType))
.withColumn("minValue", $"minValue".cast(DoubleType))
.withColumn("maxValue", $"maxValue".cast(DoubleType))
.coalesce(32)

// COMMAND ----------

//println(dfAggMonthMetricCosmos.count())

// COMMAND ----------

dfAggMonthMetricCosmos.write.mode(SaveMode.Overwrite).cosmosDB(configAggByMonth)

// COMMAND ----------

val dfAggHourMetric = spark.read
  .format("delta")
  .load(basePath + "DeltaTable/Metric-gold-Agg-Hour")
  //.filter($"unixTimestamp" >= 1601510400)
  .filter($"currentTimestamp" >= unix_timestamp() - lit(1800))

// COMMAND ----------

val configMapAggByHour = Map(
  "Endpoint" -> cdbEndpoint,
  "Masterkey" -> cdbMasterkey,
  "Database" -> cdbDatabaseAgg,
  "Collection" -> cdbCollectionAggByHour,
  "preferredRegions" -> cdbRegions,
  "Upsert" -> "true",
  "WritingBatchSize" -> cdbBatchSize)
val configAggByHour = Config(configMapAggByHour)

// COMMAND ----------

val dfAggHourMetricCosmos = dfAggHourMetric
.withColumn("id",concat(col("remoteId"),lit('|'), col("unixTimestamp"),lit('|'), col("kymetaMetricId"),lit('|'),col("metricProviderId"),lit('|'),col("categoryId")))
.select("id","unixTimestamp","remoteId","kymetaMetricId","metricProviderId","categoryId","sumValue","avgValue","minValue","maxValue")
.withColumn("sumValue", $"sumValue".cast(DoubleType))
.withColumn("avgValue", $"avgValue".cast(DoubleType))
.withColumn("minValue", $"minValue".cast(DoubleType))
.withColumn("maxValue", $"maxValue".cast(DoubleType))
.coalesce(32)

// COMMAND ----------

//println(dfAggHourMetricCosmos.count())

// COMMAND ----------

dfAggHourMetricCosmos.write.mode(SaveMode.Overwrite).cosmosDB(configAggByHour)