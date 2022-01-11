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


val dfAggHourMetricStream = spark.readStream
  .format("delta")
  .option("ignoreChanges", "true")
  .load(basePath + "DeltaTable/Metric-gold-Agg-Hour")
  .filter($"unixTimestamp" > (unix_timestamp() - lit(1209600)))
 // .withColumn("TimestampYYMMddHHmmss", from_unixtime($"unixTimestamp","yyyy-MM-dd'T'HH:mm:ss") cast TimestampType)
 // .filter($"TimestampYYMMddHHmmss" >= (date_sub(current_timestamp(), 30)))
//display(dfLiveMetricStream)

// COMMAND ----------

//display(dfLiveMetricStream)
//val dfAggHourMetric = spark.read
//  .format("delta")
//  .load(basePath + "DeltaTable/Metric-gold-Agg-Hour")
//println(dfAggHourMetric.count())

// COMMAND ----------

val configMapAggHour = Map(
  "Endpoint" -> cdbEndpoint,
  "Masterkey" -> cdbMasterkey,
  "Database" -> cdbDatabaseAgg,
  "Collection" -> cdbCollectionAggByHour,
  "preferredRegions" -> cdbRegions,
  "Upsert" -> "true",
 // "WritingBatchSize" -> cdbBatchSize,
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
dfAggHourMetricStreamCosmos.writeStream.format(classOf[CosmosDBSinkProvider].getName).outputMode("update").options(configMapAggHour).option("checkpointLocation", cdbCheckpointAggHour).start()

