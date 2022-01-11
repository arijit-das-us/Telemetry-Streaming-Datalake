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


val dfAggDayMetricStream = spark.readStream
  .format("delta")
  .option("ignoreChanges", "true")
  .load(basePath + "DeltaTable/Metric-gold-Agg-Day")
  .filter($"unixTimestamp" >= 1601510400)
 // .withColumn("TimestampYYMMddHHmmss", from_unixtime($"unixTimestamp","yyyy-MM-dd'T'HH:mm:ss") cast TimestampType)
 // .filter($"TimestampYYMMddHHmmss" >= (date_sub(current_timestamp(), 30)))
//display(dfLiveMetricStream)

// COMMAND ----------

val configMapAggDay = Map(
  "Endpoint" -> cdbEndpoint,
  "Masterkey" -> cdbMasterkey,
  "Database" -> cdbDatabaseAgg,
  "Collection" -> cdbCollectionAggByDay,
  "preferredRegions" -> cdbRegions,
  "Upsert" -> "true",
  "WritingBatchSize" -> "10",
  "ConnectionMaxPoolSize" -> "10",
  "CheckpointLocation" -> cdbCheckpointAggDay)
val configAggDay = Config(configMapAggDay)

// COMMAND ----------

val dfAggDayMetricStreamCosmos = dfAggDayMetricStream
.withColumn("id",concat(col("remoteId"),lit('|'), col("unixTimestamp"),lit('|'), col("kymetaMetricId"),lit('|'),col("metricProviderId"),lit('|'),col("categoryId")))
.select("id","unixTimestamp","remoteId","kymetaMetricId","metricProviderId","categoryId","sumValue","avgValue","minValue","maxValue")
.withColumn("sumValue", $"sumValue".cast(DoubleType))
.withColumn("avgValue", $"avgValue".cast(DoubleType))
.withColumn("minValue", $"minValue".cast(DoubleType))
.withColumn("maxValue", $"maxValue".cast(DoubleType))

// COMMAND ----------

// Write to Cosmos DB as stream
spark.sparkContext.setLocalProperty("spark.scheduler.pool", "AggDayStreamPool1")

dfAggDayMetricStreamCosmos
.writeStream
.queryName("AggDayStreamQuery1")
.format(classOf[CosmosDBSinkProvider].getName)
.outputMode("append")
.options(configMapAggDay)
.option("checkpointLocation", cdbCheckpointAggDay)
.start()



// COMMAND ----------

val dfAggMonthMetricStream = spark.readStream
  .format("delta")
  .option("ignoreChanges", "true")
  .load(basePath + "DeltaTable/Metric-gold-Agg-Month")
  .filter($"unixTimestamp" >= 1601510400)

// COMMAND ----------

val configMapAggMonth = Map(
  "Endpoint" -> cdbEndpoint,
  "Masterkey" -> cdbMasterkey,
  "Database" -> cdbDatabaseAgg,
  "Collection" -> cdbCollectionAggByMonth,
  "preferredRegions" -> cdbRegions,
  "Upsert" -> "true",
   "WritingBatchSize" -> "10",
  "ConnectionMaxPoolSize" -> "10",
  "CheckpointLocation" -> cdbCheckpointAggMonth)
val configAggMonth = Config(configMapAggMonth)

// COMMAND ----------

val dfAggMonthMetricStreamCosmos = dfAggMonthMetricStream
.withColumn("id",concat(col("remoteId"),lit('|'),col("unixTimestamp"),lit('|'), col("kymetaMetricId"),lit('|'),col("metricProviderId"),lit('|'),col("categoryId")))
.select("id","unixTimestamp","remoteId","kymetaMetricId","metricProviderId","categoryId","sumValue","avgValue","minValue","maxValue")
.withColumn("sumValue", $"sumValue".cast(DoubleType))
.withColumn("avgValue", $"avgValue".cast(DoubleType))
.withColumn("minValue", $"minValue".cast(DoubleType))
.withColumn("maxValue", $"maxValue".cast(DoubleType))

// COMMAND ----------

// Write to Cosmos DB as stream
spark.sparkContext.setLocalProperty("spark.scheduler.pool", "AggMonthStreamPool2")

dfAggMonthMetricStreamCosmos
.writeStream
.queryName("AggMonthStreamQuery2")
.format(classOf[CosmosDBSinkProvider].getName)
.outputMode("append")
.options(configMapAggMonth)
.option("checkpointLocation", cdbCheckpointAggMonth)
.start()



// COMMAND ----------

val dfAggHourMetricStream = spark.readStream
  .format("delta")
  .option("ignoreChanges", "true")
  .load(basePath + "DeltaTable/Metric-gold-Agg-Hour")
  .filter($"unixTimestamp" >= 1601510400)

// COMMAND ----------

val configMapAggHour = Map(
  "Endpoint" -> cdbEndpoint,
  "Masterkey" -> cdbMasterkey,
  "Database" -> cdbDatabaseAgg,
  "Collection" -> cdbCollectionAggByHour,
  "preferredRegions" -> cdbRegions,
  "Upsert" -> "true",
  "WritingBatchSize" -> "10",
  "ConnectionMaxPoolSize" -> "10",
  "CheckpointLocation" -> cdbCheckpointAggHour)
val configAggHour = Config(configMapAggHour)

// COMMAND ----------

val dfAggHourMetricStreamCosmos = dfAggHourMetricStream
.withColumn("id",concat(col("remoteId"),lit('|'), col("unixTimestamp"),lit('|'), col("kymetaMetricId"),lit('|'),col("metricProviderId"),lit('|'),col("categoryId")))
.select("id","unixTimestamp","remoteId","kymetaMetricId","metricProviderId","categoryId","sumValue","avgValue","minValue","maxValue")
.withColumn("sumValue", $"sumValue".cast(DoubleType))
.withColumn("avgValue", $"avgValue".cast(DoubleType))
.withColumn("minValue", $"minValue".cast(DoubleType))
.withColumn("maxValue", $"maxValue".cast(DoubleType))

// COMMAND ----------

// Write to Cosmos DB as stream
spark.sparkContext.setLocalProperty("spark.scheduler.pool", "AggHourStreamPool3")

dfAggHourMetricStreamCosmos
.writeStream
.queryName("AggHourStreamQuery3")
.format(classOf[CosmosDBSinkProvider].getName)
.outputMode("append")
.options(configMapAggHour)
.option("checkpointLocation", cdbCheckpointAggHour)
.start()

