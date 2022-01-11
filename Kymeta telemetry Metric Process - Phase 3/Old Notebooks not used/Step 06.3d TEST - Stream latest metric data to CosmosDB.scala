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


val dfLatestMetricStream = spark.readStream
  .format("delta")
  .option("ignoreChanges", "true")
  .load(basePath + "DeltaTable/Metric-gold-Agg-latest")
  //.filter($"unixTimestamp" > (unix_timestamp() - lit(1209600)))
  .withColumn("TimestampYYMMddHHmmss", from_unixtime($"unixTimestamp","yyyy-MM-dd'T'HH:mm:ss") cast TimestampType)
 // .filter($"TimestampYYMMddHHmmss" >= (date_sub(current_timestamp(), 30)))
//display(dfLiveMetricStream)

// COMMAND ----------

val GoldDFStream = 
spark.readStream
  .format("delta")
  .load(basePath + "DeltaTable/Metric-gold-raw")
  .withColumn("TimestampYYMMddHHmmss", from_unixtime($"unixTimestamp","yyyy-MM-dd'T'HH:mm:ss") cast TimestampType)
  //.filter($"unixTimestamp" > (unix_timestamp() - lit(172800)))

// COMMAND ----------

val GoldDfStreamWithWatermark = GoldDFStream.withWatermark("TimestampYYMMddHHmmss", "36 hours")
val LatestMetricStreamWatermark = dfLatestMetricStream.withWatermark("TimestampYYMMddHHmmss", "36 hours")

// COMMAND ----------

display(LatestMetricStreamWatermark)

// COMMAND ----------

display(GoldDfStreamWithWatermark)

// COMMAND ----------

val streamingDfWithValue =  LatestMetricStreamWatermark
.join(GoldDfStreamWithWatermark).where(GoldDfStreamWithWatermark("remoteId") === LatestMetricStreamWatermark("remoteId") && GoldDfStreamWithWatermark("kymetaMetricId") === LatestMetricStreamWatermark("kymetaMetricId") && GoldDfStreamWithWatermark("metricProviderId") === LatestMetricStreamWatermark("metricProviderId") && GoldDfStreamWithWatermark("unixTimestamp") === LatestMetricStreamWatermark("unixTimestamp") && GoldDfStreamWithWatermark("categoryId") === LatestMetricStreamWatermark("categoryId"))
.select(LatestMetricStreamWatermark("remoteId"),LatestMetricStreamWatermark("kymetaMetricId"),LatestMetricStreamWatermark("metricProviderId"),LatestMetricStreamWatermark("categoryId"),LatestMetricStreamWatermark("unixTimestamp"),GoldDfStreamWithWatermark("valueInDouble"),GoldDfStreamWithWatermark("valueInString"))
.withColumn("id",concat(col("remoteId"),lit('|'), col("kymetaMetricId"),lit('|'),col("metricProviderId"),lit('|'),col("categoryId")))

// COMMAND ----------

//display(streamingDfWithValue)

// COMMAND ----------

//val GoldDFStream = 
//spark.readStream
//  .format("delta")
//  .load(basePath + "DeltaTable/Metric-gold-raw")
//  .filter($"unixTimestamp" > (unix_timestamp() - lit(1209600)))
//  .withColumn("TimestampYYMMddHHmmss", from_unixtime($"unixTimestamp","yyyy-MM-dd'T'HH:mm:ss") cast TimestampType)
 // .dropDuplicates("remoteId","kymetaMetricId","metricProviderId","categoryId","unixTimestamp")

// COMMAND ----------

// Apply watermarks 
//val LatestDfStreamWithWatermark = dfLatestMetricStream.withWatermark("TimestampYYMMddHHmmss", "36 hours")
//val GoldDfStreamWithWatermark = GoldDFStream.withWatermark("TimestampYYMMddHHmmss", "36 hours")

// COMMAND ----------

//val streamingDfWithValue =  LatestDfStreamWithWatermark
//.join(GoldDfStreamWithWatermark).where(GoldDfStreamWithWatermark("remoteId") === LatestDfStreamWithWatermark("remoteId") && GoldDfStreamWithWatermark("kymetaMetricId") === //LatestDfStreamWithWatermark("kymetaMetricId") && GoldDfStreamWithWatermark("metricProviderId") === LatestDfStreamWithWatermark("metricProviderId") && GoldDfStreamWithWatermark("unixTimestamp") === LatestDfStreamWithWatermark("unixTimestamp") && GoldDfStreamWithWatermark("categoryId") === LatestDfStreamWithWatermark("categoryId"))
//.select(LatestDfStreamWithWatermark("remoteId"),LatestDfStreamWithWatermark("kymetaMetricId"),LatestDfStreamWithWatermark("metricProviderId"),LatestDfStreamWithWatermark("categoryId"),LatestDfStreamWithWatermark("unixTimestamp"),GoldDfStreamWithWatermark("valueInDouble"),GoldDfStreamWithWatermark("valueInString"))
//.withColumn("id",concat(col("remoteId"),lit('|'), col("kymetaMetricId"),lit('|'),col("metricProviderId"),lit('|'),col("categoryId")))

// COMMAND ----------

val configMapLatestTimeStamp = Map(
  "Endpoint" -> cdbEndpoint,
  "Masterkey" -> cdbMasterkey,
  "Database" -> cdbDatabaseRaw,
  "Collection" -> cdbCollectionAggLatestTime,
  "preferredRegions" -> cdbRegions,
  "Upsert" -> "true",
  "WritingBatchSize" -> cdbBatchSize,
  "CheckpointLocation" -> cdbCheckpointLatestMetricTest)
val configLatestTimeStamp = Config(configMapLatestTimeStamp)

// COMMAND ----------

val dfLatestMetricStreamCosmos = streamingDfWithValue
.select("id","remoteId","kymetaMetricId","metricProviderId","categoryId","unixTimestamp","valueInDouble","valueInString")
.withColumn("valueInDouble", $"valueInDouble".cast(DoubleType))

// COMMAND ----------

display(dfLatestMetricStreamCosmos)

// COMMAND ----------

//import org.apache.spark.sql.streaming.ProcessingTime
//import org.apache.spark.sql.streaming.Trigger
// Specifying a watermark limits state because dropping duplicates requires keeping state
// Drop anyindentical rows that arrive in a one hour time frame. It needs to include the watermark field.
//val dfExplodedQuery = 
//dfLatestMetricStreamCosmos
// dfGolddeDup
//.withWatermark("TimestampYYMMSSHHMMSS", "30 seconds")
//.dropDuplicates("uniqueHashKey","TimestampYYMMSSHHMMSS")
//.writeStream
//.format("delta")
//.partitionBy("remoteId")
//.option("mergeSchema", "true")
//.option("checkpointLocation", basePath + "DeltaTable/_checkpoint/metric-gold-latest-temp1")
//.option("path",basePath + "DeltaTable/Metric-latest-store")                          // Specify the output path
//.outputMode("append")                                                  // Append new records to the output path
//.trigger(Trigger.ProcessingTime("60 seconds")).start()

// COMMAND ----------

// Write to Cosmos DB as stream
dfLatestMetricStreamCosmos.writeStream.format(classOf[CosmosDBSinkProvider].getName).outputMode("append").options(configMapLatestTimeStamp).option("checkpointLocation", cdbCheckpointLatestMetricTest).start()

