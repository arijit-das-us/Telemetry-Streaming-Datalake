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
  .withColumn("id",concat(col("remoteId"),lit('|'), col("kymetaMetricId"),lit('|'),col("metricProviderId"),lit('|'),col("categoryId")))
  //.filter($"unixTimestamp" > (unix_timestamp() - lit(1209600)))
  //.withColumn("TimestampYYMMddHHmmss", from_unixtime($"unixTimestamp","yyyy-MM-dd'T'HH:mm:ss") cast TimestampType)
 // .filter($"TimestampYYMMddHHmmss" >= (date_sub(current_timestamp(), 30)))
//display(dfLiveMetricStream)

// COMMAND ----------

//val GoldDFStream = 
//spark.readStream
//  .format("delta")
//  .load(basePath + "DeltaTable/Metric-gold-raw")
//  .withColumn("TimestampYYMMddHHmmss", from_unixtime($"unixTimestamp","yyyy-MM-dd'T'HH:mm:ss") cast TimestampType)
  //.filter($"unixTimestamp" > (unix_timestamp() - lit(172800)))

// COMMAND ----------

//val GoldDfStreamWithWatermark = GoldDFStream.withWatermark("TimestampYYMMddHHmmss", "36 hours")

// COMMAND ----------

//display(GoldDfStreamWithWatermark)

// COMMAND ----------

//val streamingDfWithValue =  dfLatestMetricStream
//.join(GoldDfStreamWithWatermark).where(GoldDfStreamWithWatermark("remoteId") === dfLatestMetricStream("remoteId") && GoldDfStreamWithWatermark("kymetaMetricId") === dfLatestMetricStream("kymetaMetricId") && GoldDfStreamWithWatermark("metricProviderId") === dfLatestMetricStream("metricProviderId") && GoldDfStreamWithWatermark("unixTimestamp") === dfLatestMetricStream("unixTimestamp") && GoldDfStreamWithWatermark("categoryId") === dfLatestMetricStream("categoryId"))
//.select(dfLatestMetricStream("remoteId"),dfLatestMetricStream("kymetaMetricId"),dfLatestMetricStream("metricProviderId"),dfLatestMetricStream("categoryId"),dfLatestMetricStream("unixTimestamp"),GoldDfStreamWithWatermark("valueInDouble"),GoldDfStreamWithWatermark("valueInString"))
//.withColumn("id",concat(col("remoteId"),lit('|'), col("kymetaMetricId"),lit('|'),col("metricProviderId"),lit('|'),col("categoryId")))

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
 // "WritingBatchSize" -> cdbBatchSize,
  "CheckpointLocation" -> cdbCheckpointLatestMetric)
val configLatestTimeStamp = Config(configMapLatestTimeStamp)

// COMMAND ----------

val dfLatestMetricStreamCosmos = dfLatestMetricStream
.select("id","remoteId","kymetaMetricId","metricProviderId","categoryId","unixTimestamp","valueInDouble","valueInString")
.withColumn("valueInDouble", $"valueInDouble".cast(DoubleType))

// COMMAND ----------

// Write to Cosmos DB as stream
dfLatestMetricStreamCosmos.writeStream.format(classOf[CosmosDBSinkProvider].getName).outputMode("update").options(configMapLatestTimeStamp).option("checkpointLocation", cdbCheckpointLatestMetric).start()

